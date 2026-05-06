package tsdb

// slabBlockSize is the number of memSeries slots per slab block.
const slabBlockSize = 1024

// seriesSlab manages contiguous memSeries storage for one stripe.
// All methods must be called under the stripe lock.
type seriesSlab struct {
	blocks   []*[slabBlockSize]memSeries
	freeList []uint32
	count    uint32
}

// Get returns a pointer to the memSeries at the given slot index.
func (sl *seriesSlab) get(idx uint32) *memSeries {
	return &sl.blocks[idx/slabBlockSize][idx%slabBlockSize]
}

// Alloc returns a free slot index and a pointer to the zeroed memSeries in that slot.
func (sl *seriesSlab) alloc() (uint32, *memSeries) {
	if n := len(sl.freeList); n > 0 {
		idx := sl.freeList[n-1]
		sl.freeList = sl.freeList[:n-1]
		s := sl.get(idx)
		*s = memSeries{}
		return idx, s
	}
	idx := sl.count
	if int(idx/slabBlockSize) >= len(sl.blocks) {
		sl.blocks = append(sl.blocks, new([slabBlockSize]memSeries))
	}
	sl.count++
	return idx, sl.get(idx)
}

// free returns the slot at the given index to the free list for reuse.
// The slot contents are NOT zeroed here — zeroing happens in alloc() when
// the slot is recycled. This avoids races with concurrent readers that may
// still hold a pointer to the slot.
func (sl *seriesSlab) free(idx uint32) {
	sl.freeList = append(sl.freeList, idx)
}
