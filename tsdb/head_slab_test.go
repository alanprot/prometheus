package tsdb

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunks"
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
	idx1, s1 := sl.alloc()
	idx2, _ := sl.alloc()

	// Set a field to verify slot is reusable (not zeroed — callers init on alloc).
	s1.ref = 99

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
	require.Same(t, s1, sl.get(0))
}
