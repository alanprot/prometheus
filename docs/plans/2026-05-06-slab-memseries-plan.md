# Slab-Based memSeries Implementation Plan

> **For Kiro:** REQUIRED SUB-SKILL: Use subagent-driven-development (recommended) or executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace per-series heap allocations with slab-based storage to reduce GC overhead by ~100M pointer slots at 10M series.

**Architecture:** Introduce `seriesSlab` (fixed-size block allocator with free-list) per ref-shard in `stripeSeries`. Maps store `uint32` slot indices (ref→slot) and `HeadSeriesRef` values (hash→ref). External API unchanged — all code receiving `*memSeries` continues to work.

**Tech Stack:** Go, Prometheus TSDB internals (`tsdb/head.go`)

**Design doc:** `docs/plans/2026-05-06-slab-memseries-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `tsdb/head_slab.go` (new) | `seriesSlab` type: alloc, free, get methods |
| `tsdb/head_slab_test.go` (new) | Unit tests for `seriesSlab` |
| `tsdb/head.go` | Modified `stripeSeries`, `seriesHashmap`, `newStripeSeries`, `getByID`, `getByHash`, `setUnlessAlreadySet`, `iterForDeletion`, `gc`, `gcStaleSeries` |
| `tsdb/head_test.go` | Updated helpers and new integration-level tests |
| `tsdb/head_bench_test.go` | GC benchmark for before/after comparison |

---

### Task 1: Add `seriesSlab` type with unit tests

**Files:**
- Create: `tsdb/head_slab.go`
- Create: `tsdb/head_slab_test.go`

- [ ] **Step 1: Write failing tests for seriesSlab**

Create `tsdb/head_slab_test.go`:

```go
package tsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeriesSlab_AllocAndGet(t *testing.T) {
	var sl seriesSlab
	idx, s := sl.alloc()
	require.NotNil(t, s)
	require.Equal(t, uint32(0), idx)
	require.Equal(t, s, sl.get(idx))

	// Allocate more than one block.
	for i := uint32(1); i < slabBlockSize+1; i++ {
		idx2, s2 := sl.alloc()
		require.Equal(t, i, idx2)
		require.NotNil(t, s2)
		require.Equal(t, s2, sl.get(idx2))
	}
	// Should have 2 blocks now.
	require.Equal(t, 2, len(sl.blocks))
}

func TestSeriesSlab_FreeAndReuse(t *testing.T) {
	var sl seriesSlab
	idx0, _ := sl.alloc()
	idx1, _ := sl.alloc()
	idx2, _ := sl.alloc()

	// Free middle slot.
	sl.free(idx1)
	require.Equal(t, 1, len(sl.freeList))

	// Next alloc reuses freed slot.
	reused, s := sl.alloc()
	require.Equal(t, idx1, reused)
	require.NotNil(t, s)
	require.Equal(t, 0, len(sl.freeList))

	// Free all, then reallocate in LIFO order.
	sl.free(idx0)
	sl.free(idx2)
	r1, _ := sl.alloc()
	r2, _ := sl.alloc()
	require.Equal(t, idx2, r1) // LIFO
	require.Equal(t, idx0, r2)
}

func TestSeriesSlab_GetStablePointer(t *testing.T) {
	var sl seriesSlab
	_, s1 := sl.alloc()
	s1.ref = 42

	// Allocate enough to trigger a second block.
	for i := 1; i < slabBlockSize+10; i++ {
		sl.alloc()
	}

	// Original pointer must still be valid.
	require.Equal(t, chunks.HeadSeriesRef(42), sl.get(0).ref)
	require.Equal(t, s1, sl.get(0))
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./tsdb/ -run TestSeriesSlab -v -count=1`
Expected: Compilation error — `seriesSlab` type not defined.

- [ ] **Step 3: Implement seriesSlab**

Create `tsdb/head_slab.go`:

```go
package tsdb

const slabBlockSize = 1024

// seriesSlab manages contiguous memSeries storage for one stripe.
// All methods must be called under the stripe lock.
type seriesSlab struct {
	blocks   []*[slabBlockSize]memSeries
	freeList []uint32
	count    uint32
}

func (sl *seriesSlab) get(idx uint32) *memSeries {
	return &sl.blocks[idx/slabBlockSize][idx%slabBlockSize]
}

func (sl *seriesSlab) alloc() (uint32, *memSeries) {
	if n := len(sl.freeList); n > 0 {
		idx := sl.freeList[n-1]
		sl.freeList = sl.freeList[:n-1]
		return idx, sl.get(idx)
	}
	idx := sl.count
	if int(idx/slabBlockSize) >= len(sl.blocks) {
		sl.blocks = append(sl.blocks, new([slabBlockSize]memSeries))
	}
	sl.count++
	return idx, sl.get(idx)
}

func (sl *seriesSlab) free(idx uint32) {
	*sl.get(idx) = memSeries{}
	sl.freeList = append(sl.freeList, idx)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./tsdb/ -run TestSeriesSlab -v -count=1`
Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tsdb/head_slab.go tsdb/head_slab_test.go
git commit -s -m "tsdb: add seriesSlab allocator for contiguous memSeries storage

Introduce seriesSlab type that manages memSeries in fixed-size blocks
with a free-list for slot recycling. This is the foundation for
reducing GC overhead by eliminating per-series heap allocations."
```

---

### Task 2: Add `slabIdx` field to `memSeries`

**Files:**
- Modify: `tsdb/head.go:2448-2508` (memSeries struct)

- [ ] **Step 1: Add the field**

In `tsdb/head.go`, add `slabIdx uint32` to `memSeries` after the `shardHash` field (before the Mutex). This field is set at allocation time and never changes, so it doesn't need lock protection:

```go
type memSeries struct {
	ref  chunks.HeadSeriesRef
	meta *metadata.Metadata

	shardHash uint64

	// slabIdx is the index of this series within its ref-shard's seriesSlab.
	// Set once during allocation; used during GC to free the slot.
	slabIdx uint32

	sync.Mutex
	// ... rest unchanged
}
```

- [ ] **Step 2: Verify build passes**

Run: `go build ./tsdb/...`
Expected: Success (field is unused but that's fine — it's exported within the package).

- [ ] **Step 3: Commit**

```bash
git add tsdb/head.go
git commit -s -m "tsdb: add slabIdx field to memSeries

Store the slab slot index in memSeries for O(1) lookup during GC
deallocation. Placed before the Mutex to fit in existing alignment
padding."
```

---

### Task 3: Convert `stripeSeries` and `seriesHashmap` to slab-based storage

**Files:**
- Modify: `tsdb/head.go` — `stripeSeries` struct, `seriesHashmap` struct, `newStripeSeries`

This is the core structural change. After this task, the code will compile but tests will fail until Task 4 updates the methods.

- [ ] **Step 1: Modify `seriesHashmap` to store `HeadSeriesRef` instead of `*memSeries`**

Replace the existing `seriesHashmap` struct and all its methods in `tsdb/head.go`:

```go
type seriesHashmap struct {
	unique    map[uint64]chunks.HeadSeriesRef
	conflicts map[uint64][]chunks.HeadSeriesRef
}

// get resolves a series by hash and labels. The resolver function translates
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

func (m *seriesHashmap) set(hash uint64, ref chunks.HeadSeriesRef) {
	if existingRef, found := m.unique[hash]; !found {
		m.unique[hash] = ref
		return
	} else if existingRef == ref {
		return
	}
	if m.conflicts == nil {
		m.conflicts = make(map[uint64][]chunks.HeadSeriesRef)
	}
	l := m.conflicts[hash]
	for i, prev := range l {
		if prev == ref {
			l[i] = ref
			return
		}
	}
	m.conflicts[hash] = append(l, ref)
}

func (m *seriesHashmap) del(hash uint64, ref chunks.HeadSeriesRef) {
	var rem []chunks.HeadSeriesRef
	unique, found := m.unique[hash]
	switch {
	case !found:
		return
	case unique == ref:
		conflicts := m.conflicts[hash]
		if len(conflicts) == 0 {
			delete(m.unique, hash)
			return
		}
		m.unique[hash] = conflicts[0]
		rem = conflicts[1:]
	default:
		for _, r := range m.conflicts[hash] {
			if r != ref {
				rem = append(rem, r)
			}
		}
	}
	if len(rem) == 0 {
		delete(m.conflicts, hash)
	} else {
		m.conflicts[hash] = rem
	}
}
```

The `get` method takes a `resolve` function instead of `*stripeSeries`. This avoids the deadlock where `getByID` would try to read-lock a shard that the caller already write-locks. Callers provide the appropriate resolver:

- `getByHash`: passes `s.getByID` (takes ref shard read lock — safe, different shard).
- `setUnlessAlreadySet`: passes `s.resolveRef(ref, i)` which skips locking when `refShard == hashShard`.
- `iterForDeletion`: passes `s.resolveRef(ref, i)` similarly.

- [ ] **Step 2: Modify `stripeSeries` struct to add slabs and change map value types**

```go
type stripeSeries struct {
	size                    int
	series                  []map[chunks.HeadSeriesRef]uint32 // ref → slab slot index
	hashes                  []seriesHashmap
	locks                   []stripeLock
	slabs                   []seriesSlab // one per ref-shard
	seriesLifecycleCallback SeriesLifecycleCallback
}
```

- [ ] **Step 3: Update `newStripeSeries`**

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

- [ ] **Step 4: Verify compilation**

Run: `go build ./tsdb/...`
Expected: Compilation errors in methods that still use old types (`getByID`, `getByHash`, `setUnlessAlreadySet`, `gc`, `iterForDeletion`, etc.). This is expected — Task 4 fixes them.

- [ ] **Step 5: Commit (WIP — will not compile until Task 4)**

Do NOT commit yet. Proceed directly to Task 4.

---

### Task 4: Update `stripeSeries` methods to use slab

**Files:**
- Modify: `tsdb/head.go` — `getByID`, `getByHash`, `setUnlessAlreadySet`, `iterForDeletion`, `gc`, `gcStaleSeries`, `mmapHeadChunks` iteration

This task makes the code compile and pass tests again.

- [ ] **Step 1: Update `getByID`**

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

- [ ] **Step 2: Update `getByHash`**

```go
func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	i := hash & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset, s.getByID)
	s.locks[i].RUnlock()

	return series
}
```

Note: `getByID` acquires the ref shard's read lock. Since `getByHash` holds the hash shard's *read* lock, and read locks don't block each other, this is safe even when `hashShard == refShard`.

- [ ] **Step 3: Update `setUnlessAlreadySet`**

```go
func (s *stripeSeries) setUnlessAlreadySet(hash uint64, lset labels.Labels, series *memSeries) (*memSeries, bool) {
	i := hash & uint64(s.size-1)
	s.locks[i].Lock()
	// Use resolveRef with heldShard=i to avoid deadlock when refShard==hashShard.
	resolver := func(ref chunks.HeadSeriesRef) *memSeries {
		return s.resolveRef(ref, int(i))
	}
	if prev := s.hashes[i].get(hash, lset, resolver); prev != nil {
		s.locks[i].Unlock()
		return prev, false
	}
	s.hashes[i].set(hash, series.ref)
	s.locks[i].Unlock()

	refShard := uint64(series.ref) & uint64(s.size-1)

	s.locks[refShard].Lock()
	idx, slot := s.slabs[refShard].alloc()
	*slot = *series
	slot.slabIdx = idx
	s.series[refShard][series.ref] = idx
	s.locks[refShard].Unlock()

	return slot, true
}

// resolveRef looks up a memSeries by ref without locking if refShard == heldShard.
// Otherwise acquires a read lock on the ref shard.
func (s *stripeSeries) resolveRef(ref chunks.HeadSeriesRef, heldShard int) *memSeries {
	refShard := int(uint64(ref) & uint64(s.size-1))
	if refShard == heldShard {
		idx, ok := s.series[refShard][ref]
		if !ok {
			return nil
		}
		return s.slabs[refShard].get(idx)
	}
	s.locks[refShard].RLock()
	idx, ok := s.series[refShard][ref]
	s.locks[refShard].RUnlock()
	if !ok {
		return nil
	}
	return s.slabs[refShard].get(idx)
}
```

- [ ] **Step 4: Update `iterForDeletion`**

The hashmap now stores `HeadSeriesRef` values. Use `resolveRef` to look up each series:

```go
func (s *stripeSeries) iterForDeletion(checkDeletedFunc func(int, uint64, *memSeries, map[chunks.HeadSeriesRef]labels.Labels)) int {
	seriesSetFromPrevStripe := 0
	totalDeletedSeries := 0
	for i := 0; i < s.size; i++ {
		seriesSet := make(map[chunks.HeadSeriesRef]labels.Labels, seriesSetFromPrevStripe)
		s.locks[i].Lock()
		for hash, all := range s.hashes[i].conflicts {
			for _, ref := range all {
				series := s.resolveRef(ref, i)
				if series != nil {
					checkDeletedFunc(i, hash, series, seriesSet)
				}
			}
		}
		for hash, ref := range s.hashes[i].unique {
			series := s.resolveRef(ref, i)
			if series != nil {
				checkDeletedFunc(i, hash, series, seriesSet)
			}
		}
		s.locks[i].Unlock()
		s.seriesLifecycleCallback.PostDeletion(seriesSet)
		totalDeletedSeries += len(seriesSet)
		seriesSetFromPrevStripe = len(seriesSet)
	}
	return totalDeletedSeries
}
```

`resolveRef(ref, i)` handles the cross-shard case: if `refShard == i`, it reads directly (we hold the lock); otherwise it takes a read lock on the ref shard. This is safe because we hold shard `i`'s write lock and only take read locks on other shards — no deadlock possible.

- [ ] **Step 5: Update the `gc` `check` callback to free slab slots**

In the `gc` method's `check` callback, after `delete(s.series[refShard], series.ref)`, add:

```go
s.slabs[refShard].free(series.slabIdx)
```

The existing code already acquires the ref shard lock when `hashShard != refShard` before deleting from `s.series[refShard]`. The `free` call goes right after the `delete`.

- [ ] **Step 6: Update `mmapHeadChunks` iteration**

The `mmapHeadChunks` method iterates `h.series.series[i]` which now stores `uint32` indices. Update:

```go
func (h *Head) mmapHeadChunks() {
	var count int
	for i := range h.series.size {
		h.series.locks[i].RLock()
		for _, idx := range h.series.series[i] {
			series := h.series.slabs[i].get(idx)
			if series.headChunkCount.Load() < 2 {
				continue
			}
			series.Lock()
			// ... rest unchanged
```

- [ ] **Step 7: Verify build compiles**

Run: `go build ./tsdb/...`
Expected: Success.

- [ ] **Step 8: Run existing tests**

Run: `go test ./tsdb/ -run "TestStripeSeries|TestHead" -count=1 -timeout 300s`
Expected: All pass.

- [ ] **Step 9: Commit**

```bash
git add tsdb/head.go
git commit -s -m "tsdb: convert stripeSeries to slab-based memSeries storage

Replace per-series heap allocations with seriesSlab blocks. Maps now
store uint32 slot indices (series map) and HeadSeriesRef values
(hashmap). External API unchanged - all code receiving *memSeries
continues to work via stable pointers into slab blocks.

This eliminates ~10M heap objects and ~20M GC-traced map pointer slots
at 10M active series."
```

---

### Task 5: Update test helpers and verify full test suite

**Files:**
- Modify: `tsdb/head_test.go` — `stripeSeriesWithCollidingSeries` helper

- [ ] **Step 1: Update `stripeSeriesWithCollidingSeries` helper**

This helper creates a `stripeSeries` with colliding series for testing. It currently calls `setUnlessAlreadySet` with `*memSeries` — the signature hasn't changed, so it may already work. Read the helper and verify.

If it uses internal fields of `seriesHashmap` directly (e.g., accessing `.unique` or `.conflicts`), update those accesses to use the new types (`HeadSeriesRef` instead of `*memSeries`).

Run: `go test ./tsdb/ -run TestStripeSeries -v -count=1`
Expected: All `TestStripeSeries_*` tests pass.

- [ ] **Step 2: Run the full TSDB test suite**

Run: `go test ./tsdb/... -count=1 -timeout 600s 2>&1 | tail -50`
Expected: All tests pass.

- [ ] **Step 3: Run with race detector**

Run: `go test ./tsdb/ -run "TestStripeSeries|TestHead" -race -count=1 -timeout 300s`
Expected: No data races detected.

- [ ] **Step 4: Commit any test fixes**

```bash
git add tsdb/head_test.go
git commit -s -m "tsdb: update test helpers for slab-based stripeSeries"
```

---

### Task 6: Add GC benchmark

**Files:**
- Modify: `tsdb/head_bench_test.go`

- [ ] **Step 1: Write GC overhead benchmark**

Add to `tsdb/head_bench_test.go`:

```go
func BenchmarkHeadStripeSeriesGC(b *testing.B) {
	for _, numSeries := range []int{100_000, 1_000_000} {
		b.Run(fmt.Sprintf("series=%d", numSeries), func(b *testing.B) {
			h, _ := NewHead(nil, nil, nil, nil, DefaultHeadOptions(), nil)
			defer h.Close()

			for i := 0; i < numSeries; i++ {
				lset := labels.FromStrings("__name__", "test", "instance", fmt.Sprintf("inst_%d", i))
				h.getOrCreate(lset.Hash(), lset)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				runtime.GC()
			}
		})
	}
}
```

Add `"runtime"` and `"fmt"` to imports if not already present.

- [ ] **Step 2: Run benchmark to get baseline**

Run: `go test ./tsdb/ -run=^$ -bench BenchmarkHeadStripeSeriesGC -benchmem -count=6 -timeout 600s | tee /tmp/bench_gc_after.txt`

Compare with baseline (run on the commit before Task 1):
```bash
# To get baseline, stash changes, run bench, then pop:
# git stash && go test ./tsdb/ -bench BenchmarkHeadStripeSeriesGC -benchmem -count=6 > /tmp/bench_gc_before.txt && git stash pop
# benchstat /tmp/bench_gc_before.txt /tmp/bench_gc_after.txt
```

- [ ] **Step 3: Also run existing allocation benchmarks**

Run: `go test ./tsdb/ -run=^$ -bench BenchmarkHeadStripeSeriesCreate -benchmem -count=6 -timeout 300s`
Expected: Reduced allocs/op (no more `newMemSeries` per series — allocation is a slab slot copy).

- [ ] **Step 4: Commit**

```bash
git add tsdb/head_bench_test.go
git commit -s -m "tsdb: add GC overhead benchmark for stripeSeries

Measures runtime.GC() cost at 100K and 1M series to quantify the
impact of slab-based storage on GC pause times."
```

---

## Final Verification

- [ ] **Run full test suite one final time:**

```bash
go test ./tsdb/... -count=1 -timeout 600s
```

- [ ] **Run race detector on key tests:**

```bash
go test ./tsdb/ -race -run "TestStripeSeries|TestHead|TestMemSeries" -count=1 -timeout 300s
```

- [ ] **Run lint:**

```bash
make lint 2>&1 | grep -A2 "tsdb/head"
```

**Note on integration tests:** Prometheus does not have a separate integration test suite that requires AWS resources or external services. The `go test ./tsdb/...` suite includes both unit and integration-level tests (e.g., full Head lifecycle tests with WAL replay). Running the full suite with the race detector constitutes sufficient integration verification for this change.