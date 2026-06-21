package metrics

import "testing"

// TestObserve_InOrderBeyondWindow is the regression test for the windowing bug:
// a monotonic stream far longer than the window must report every message as
// delivered and zero duplicates. Before the fix, delivered capped at windowSize
// and every later seq was miscounted as a duplicate.
func TestObserve_InOrderBeyondWindow(t *testing.T) {
	const window = 64
	const total = window * 10 // well beyond one window
	st := NewSequenceTracker(window)

	for seq := uint64(1); seq <= total; seq++ {
		st.Observe(seq)
	}

	s := st.Snapshot()
	if s.Delivered != total {
		t.Errorf("Delivered = %d, want %d", s.Delivered, total)
	}
	if s.Duplicated != 0 {
		t.Errorf("Duplicated = %d, want 0 (windowing bug regressed)", s.Duplicated)
	}
	if s.Reordered != 0 {
		t.Errorf("Reordered = %d, want 0", s.Reordered)
	}
}

// TestObserve_RealDuplicate verifies a genuine duplicate within the window is
// still detected after the fix.
func TestObserve_RealDuplicate(t *testing.T) {
	st := NewSequenceTracker(64)
	st.Observe(1)
	st.Observe(2)
	st.Observe(3)
	st.Observe(2) // genuine duplicate

	s := st.Snapshot()
	if s.Duplicated != 1 {
		t.Errorf("Duplicated = %d, want 1", s.Duplicated)
	}
	if s.Delivered != 3 {
		t.Errorf("Delivered = %d, want 3", s.Delivered)
	}
}

// TestObserve_Reorder verifies a gap that is later filled counts as a reorder,
// not a loss or duplicate, and the slot was correctly cleared on advance.
func TestObserve_Reorder(t *testing.T) {
	st := NewSequenceTracker(64)
	st.Observe(1)
	st.Observe(2)
	st.Observe(4) // 3 is skipped (gap)
	st.Observe(3) // late arrival fills the gap

	s := st.Snapshot()
	if s.Delivered != 4 {
		t.Errorf("Delivered = %d, want 4", s.Delivered)
	}
	if s.Reordered != 1 {
		t.Errorf("Reordered = %d, want 1", s.Reordered)
	}
	if s.Duplicated != 0 {
		t.Errorf("Duplicated = %d, want 0", s.Duplicated)
	}
}

// TestObserve_TailLossFlush verifies Flush counts sequence numbers never
// received beyond the frontier as lost.
func TestObserve_TailLossFlush(t *testing.T) {
	st := NewSequenceTracker(64)
	st.Observe(1)
	st.Observe(2)
	st.Observe(3)
	st.Flush(6) // 4, 5, 6 were sent but never received

	s := st.Snapshot()
	if s.Lost != 3 {
		t.Errorf("Lost = %d, want 3", s.Lost)
	}
}

// TestObserve_GiantJump verifies a jump wider than the window resets cleanly
// without fabricating duplicates.
func TestObserve_GiantJump(t *testing.T) {
	const window = 64
	st := NewSequenceTracker(window)
	st.Observe(1)
	st.Observe(1 + window*3) // jump far beyond the window
	st.Observe(2 + window*3) // continue in order

	s := st.Snapshot()
	if s.Duplicated != 0 {
		t.Errorf("Duplicated = %d, want 0", s.Duplicated)
	}
	if s.Delivered != 3 {
		t.Errorf("Delivered = %d, want 3", s.Delivered)
	}
}
