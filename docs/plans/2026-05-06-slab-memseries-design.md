# Slab-Based memSeries Storage for GC Reduction

## Problem

At 10M active series, Prometheus's TSDB head holds 10M individual `*memSeries` heap allocations. Each allocation is ~192 bytes with ~8 internal pointer fields. The Go GC must trace every pointer in the heap during its mark phase:

- **10M heap objects** from `newMemSeries` allocations.
- **20M+ map pointer slots** — each series appears in both `series[]` (by ref) and `hashes[]` (by label hash), and Go's GC traces pointer-valued map entries.
- **80M+ internal pointer slots** within the `memSeries` structs themselves.

This creates sustained GC overhead proportional to series count, regardless of allocation rate. The GC scans long-lived pointers every cycle even when nothing changes.

## Goal

Reduce GC-visible pointer count by ~100M by:
1. Eliminating per-series heap allocations (10M objects → ~10K slab blocks).
2. Converting maps from pointer-valued to integer-valued (GC ignores `uint32` map values).

## Design

### Core Data Structures

```go
const slabBlockSize = 1024

// seriesSlab manages contiguous memSeries storage for one stripe.
type seriesSlab struct {
    blocks   []*[slabBlockSize]memSeries // Fixed-size blocks; never relocated.
    freeList []uint32                    // Recycled slot indices.
    count    uint32                      // High-water mark for next allocation.
}

type stripeSeries struct {
    size                    int
    series                  []map[chunks.HeadSeriesRef]uint32 // ref → slab slot index
    hashes                  []seriesHashmap                   // stores uint32 indices
    locks                   []stripeLock
    slabs                   []seriesSlab                      // one per stripe
    seriesLifecycleCallback SeriesLifecycleCallback
}
```

### seriesHashmap Changes

```go
type seriesHashmap struct {
    unique    map[uint64]chunks.HeadSeriesRef   // hash → series ref (common case)
    conflicts map[uint64][]chunks.HeadSeriesRef // hash → series refs (collisions)
}

// get resolves a series by hash and labels. The resolve function translates
// a HeadSeriesRef to *memSeries — callers provide different resolvers depending
// on what locks they already hold.
func (m *seriesHashmap) get(hash uint64, lset labels.Labels, resolve func(chunks.HeadSeriesRef) *memSeries) *memSeries {
    if ref, found := m.unique[hash]; found {
        series := resolve(ref)
        if series != nil && labels.Equal(series.labels(), lset) {
            return series
        }
    }
    for _, ref := range m.conflicts[hash] {
        series := resolve(ref)
        if series != nil && labels.Equal(series.labels(), lset) {
            return series
        }
    }
    return nil
}
```

The hashmap stores `HeadSeriesRef` values (a `uint64`) instead of pointers. The `get` method takes a resolver function to avoid deadlocks — callers that hold a write lock pass `resolveRef` (which skips locking when shards match), while callers with only a read lock pass `getByID`.

**GC note:** `HeadSeriesRef` is a `uint64` — the GC does not trace it. The `unique` map is pointer-free from the GC's perspective. The `conflicts` map has slice headers (pointers to backing arrays), but hash collisions are rare — typically a few hundred entries across all shards at 10M series.

### Slot Resolution

```go
func (sl *seriesSlab) get(idx uint32) *memSeries {
    return &sl.blocks[idx/slabBlockSize][idx%slabBlockSize]
}
```

Returns a stable pointer. Blocks are heap-allocated arrays that never move, so the returned `*memSeries` remains valid for the slot's lifetime.

### Allocation

```go
func (sl *seriesSlab) alloc() (uint32, *memSeries) {
    if n := len(sl.freeList); n > 0 {
        idx := sl.freeList[n-1]
        sl.freeList = sl.freeList[:n-1]
        return idx, sl.get(idx)
    }
    idx := sl.count
    blockIdx := idx / slabBlockSize
    if int(blockIdx) >= len(sl.blocks) {
        sl.blocks = append(sl.blocks, new([slabBlockSize]memSeries))
    }
    sl.count++
    return idx, sl.get(idx)
}
```

Called under the stripe lock during `setUnlessAlreadySet`. No new synchronization required.

### Deallocation

```go
func (sl *seriesSlab) free(idx uint32) {
    *sl.get(idx) = memSeries{} // Zero struct to release internal pointers.
    sl.freeList = append(sl.freeList, idx)
}
```

Called during `gc()` and `gcStaleSeries()` after removing the series from both maps. Zeroing ensures the GC can collect objects referenced by the deleted series's internal fields (slices, `*metadata.Metadata`, `chunkenc.Appender` interface, etc.).

### Modified stripeSeries Methods

#### getByID

```go
func (s *stripeSeries) getByID(id chunks.HeadSeriesRef) *memSeries {
    i := uint64(id) & uint64(s.size-1)
    s.locks[i].RLock()
    idx, ok := s.series[i][id]
    s.locks[i].RUnlock()
    if !ok {
        return nil
    }
    return s.slabs[i].get(idx)
}
```

#### getByHash

```go
func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
    i := hash & uint64(s.size-1)
    s.locks[i].RLock()
    series := s.hashes[i].get(hash, lset, s)
    s.locks[i].RUnlock()
    return series
}
```

Note: `seriesHashmap.get` calls `s.getByID(ref)` internally, which acquires the ref shard's lock. This means `getByHash` briefly holds two locks (hash shard read lock + ref shard read lock). This is safe because lock ordering is always hash-shard-first — matching the existing `setUnlessAlreadySet` pattern.

#### setUnlessAlreadySet

The slab allocation belongs to the **ref shard** because `getByID` is the hot path and must resolve the index from its own shard's slab.

```go
func (s *stripeSeries) setUnlessAlreadySet(hash uint64, lset labels.Labels, series *memSeries) (*memSeries, bool) {
    hashShard := hash & uint64(s.size-1)

    s.locks[hashShard].Lock()
    if prev := s.hashes[hashShard].get(hash, lset, s); prev != nil {
        s.locks[hashShard].Unlock()
        return prev, false
    }
    s.hashes[hashShard].set(hash, series.ref)
    s.locks[hashShard].Unlock()

    // Allocate from the ref shard's slab (lock released above, same as existing code).
    refShard := uint64(series.ref) & uint64(s.size-1)
    s.locks[refShard].Lock()
    idx, slot := s.slabs[refShard].alloc()
    *slot = *series
    slot.slabIdx = idx
    s.series[refShard][series.ref] = idx
    s.locks[refShard].Unlock()

    return slot, true
}
```

**Lock ordering matches existing code:** Hash shard lock acquired and released first, then ref shard lock acquired and released. No nested locking. This is identical to the current pattern.

**seriesHashmap.get and nested locking:** The `get` method calls `s.getByID(ref)` which acquires the ref shard's **read** lock. When called from `getByHash` (which holds the hash shard's read lock), this creates nested read locks. Read locks cannot deadlock with each other regardless of ordering. When called from `setUnlessAlreadySet` (which holds the hash shard's write lock), the inner `getByID` takes a read lock on a *different* shard — this is safe as long as no other path takes these locks in reverse order with write locks. The existing GC path (`iterForDeletion`) only holds one shard's write lock at a time, so no conflict exists.

Note: The caller still creates a `memSeries` optimistically. On success, we copy it into the slab slot. On conflict, the optimistic allocation is discarded — same as today.

#### gc / deletion path

In the `check` callback within `iterForDeletion`:

```go
// After removing from hashes and series maps:
s.hashes[hashShard].del(hash, series.ref)
delete(s.series[refShard], series.ref)
s.slabs[refShard].free(slotIdx) // Slab lives in the ref shard.
```

The slot index is retrieved from `s.series[refShard][series.ref]` before deletion, or stored in the `memSeries` itself.

**Recommendation:** Add a `slabIdx uint32` field to `memSeries`. It costs 4 bytes per series (with padding, likely zero cost due to existing alignment gaps) and avoids a map lookup in the hot GC path.

### newStripeSeries Changes

```go
func newStripeSeries(stripeSize int, seriesCallback SeriesLifecycleCallback) *stripeSeries {
    s := &stripeSeries{
        size:                    stripeSize,
        series:                  make([]map[chunks.HeadSeriesRef]uint32, stripeSize),
        hashes:                  make([]seriesHashmap, stripeSize),
        locks:                   make([]stripeLock, stripeSize),
        slabs:                   make([]seriesSlab, stripeSize),
        seriesLifecycleCallback: seriesCallback,
    }
    for i := range s.series {
        s.series[i] = map[chunks.HeadSeriesRef]uint32{}
    }
    for i := range s.hashes {
        s.hashes[i] = seriesHashmap{
            unique: map[uint64]chunks.HeadSeriesRef{},
        }
    }
    return s
}
```

## Scope of Change

### Files Modified

| File | Change |
|------|--------|
| `tsdb/head.go` | `stripeSeries`, `seriesHashmap`, `seriesSlab` (new), `newStripeSeries`, `getByID`, `getByHash`, `setUnlessAlreadySet`, `gc`, `gcStaleSeries`, `iterForDeletion`, `mmapHeadChunks` iteration |
| `tsdb/head_append.go` | No change — receives `*memSeries` from `getByID`/`getByHash` as before |
| `tsdb/head_read.go` | No change — receives `*memSeries` from `getByID` as before |
| `tsdb/head_wal.go` | `getOrCreateWithOptionalID` — adjust to pass series into slab |
| `tsdb/head_test.go` | Update `stripeSeriesWithCollidingSeries` helper, add slab-specific tests |

### Files NOT Modified

All code that operates on `*memSeries` after retrieval (149 usages across 16 files) remains unchanged. The slab returns a stable `*memSeries` pointer identical to what `new(memSeries)` would return.

## GC Impact Analysis

| Metric | Before (10M series) | After |
|--------|---------------------|-------|
| Heap objects from series | 10,000,000 | ~10,000 (slab blocks) |
| GC-traced map pointer slots | 20,000,000+ | 0 (`uint32` values) |
| Internal pointer fields scanned | ~80,000,000 | ~80,000,000 (unchanged) |
| **Net pointer slot reduction** | — | **~100,000,000** |

The GC mark phase time scales with pointer count. Eliminating 100M pointer slots should reduce GC pause contribution from `stripeSeries` by roughly 50-60% (the internal pointers within `memSeries` structs remain, but the map overhead and per-object header scanning disappear).

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Slab slot index stored in wrong stripe | `slabIdx` is only meaningful within its ref shard. The stripe lock protects all access. |
| Lock ordering in getByHash | `getByHash` holds hash-shard read lock while calling `getByID` (which takes ref-shard read lock). This matches the existing lock order in `setUnlessAlreadySet` (hash lock first, ref lock second). Read locks don't deadlock with each other. |
| Use-after-free (pointer to freed slot) | Same risk as today — code that holds `*memSeries` past a GC cycle already races. The stripe lock prevents concurrent free+access. |
| Memory not returned to OS after mass deletion | Free-list holds slots but blocks remain allocated. If needed, add periodic compaction that releases trailing empty blocks. Not required for initial implementation — Prometheus series counts are relatively stable. |
| Optimistic series copy overhead | One `memSeries` copy (~192 bytes) per new series creation. Negligible compared to the label allocation and postings update cost already on this path. |

## Testing Strategy

1. **Unit tests:** Verify `seriesSlab` alloc/free/get, free-list reuse, multi-block growth.
2. **Existing tests pass:** All `stripeSeries` tests (`TestStripeSeries_getOrSet`, `TestStripeSeries_gc`, benchmarks) must pass unchanged since the external API is identical.
3. **Benchmark:** Run `BenchmarkHeadStripeSeriesCreate` and `BenchmarkHeadStripeSeriesCreateParallel` before/after. Measure allocations via `-benchmem`.
4. **GC benchmark:** Write a benchmark that creates 1M series, triggers `runtime.GC()`, and measures pause time. Compare before/after.

## Success Criteria

- All existing TSDB tests pass.
- `BenchmarkHeadStripeSeriesCreate` shows reduced allocations per operation.
- GC pause time benchmark shows measurable reduction at 1M+ series.
- No new data races under `-race`.
