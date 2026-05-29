package metrics

import (
	"sync"
	"sync/atomic"
)

// SeqStats holds atomic counters for sequence-based reliability metrics.
// All fields are exported for direct reading by the Recorder without locking.
type SeqStats struct {
	Delivered  atomic.Uint64
	Lost       atomic.Uint64
	Duplicated atomic.Uint64
	Reordered  atomic.Uint64
}

// SequenceTracker detects message loss, duplication, and reordering using a
// sliding window bitmask.
//
// Algorithm:
//   - A bit array of windowSize bits tracks which sequence numbers have been seen.
//   - bit[seq % windowSize] = 1 means seq has been received.
//   - Sequence numbers arrive in roughly increasing order; gaps indicate loss.
//   - A sequence arriving below nextExpected is a reorder (if not seen) or duplicate (if seen).
//   - At end-of-run, Flush(lastSentSeq) declares all unset bits as lost.
//
// Window size = 4096 bits = 512 bytes = 8 uint64 words.
// This allows up to 4096 in-flight reorders before incorrectly declaring loss.
//
// Thread safety: all public methods are mutex-protected.
type SequenceTracker struct {
	mu           sync.Mutex
	nextExpected uint64   // the next seqnum we expect (the "frontier")
	windowSize   uint64   // size of the tracking window (must be power of 2)
	windowMask   uint64   // windowSize - 1 (for fast modulo)
	window       []uint64 // bitmask: window[seq/64 % words] bit (seq%64)
	words        uint64   // len(window) = windowSize / 64
	Stats        SeqStats
}

// NewSequenceTracker creates a tracker with the given window size.
// windowSize must be a power of 2. Recommended: 4096 for typical benchmarks,
// 65536 for high-reorder scenarios.
func NewSequenceTracker(windowSize uint64) *SequenceTracker {
	if windowSize == 0 || (windowSize&(windowSize-1)) != 0 {
		panic("SequenceTracker: windowSize must be a power of 2")
	}
	words := windowSize / 64
	if words == 0 {
		words = 1
	}
	return &SequenceTracker{
		windowSize: windowSize,
		windowMask: windowSize - 1,
		window:     make([]uint64, words),
		words:      words,
	}
}

// wordIndex returns the uint64 word index for sequence number seq.
//
//go:nosplit
func (t *SequenceTracker) wordIndex(seq uint64) uint64 {
	return (seq / 64) % t.words
}

// bitMask returns the bit mask for sequence number seq within its word.
//
//go:nosplit
func (t *SequenceTracker) bitMask(seq uint64) uint64 {
	return uint64(1) << (seq & 63)
}

// isSet returns true if the bit for seq is set in the window.
//
//go:nosplit
func (t *SequenceTracker) isSet(seq uint64) bool {
	return t.window[t.wordIndex(seq)]&t.bitMask(seq) != 0
}

// set marks the bit for seq.
//
//go:nosplit
func (t *SequenceTracker) set(seq uint64) {
	t.window[t.wordIndex(seq)] |= t.bitMask(seq)
}

// clearRange clears bits for seqnums in [from, to) to reuse window slots.
// Called as the frontier advances past old entries.
func (t *SequenceTracker) clearRange(from, to uint64) {
	for s := from; s < to; s++ {
		t.window[t.wordIndex(s)] &^= t.bitMask(s)
	}
}

// Observe records a received sequence number.
//
// Cases:
//  1. seq < nextExpected and isSet(seq): duplicate (already counted as delivered)
//  2. seq < nextExpected and !isSet(seq): late arrival (reorder); still counts as delivered
//  3. seq == nextExpected: in-order delivery; advance frontier
//  4. seq > nextExpected: advance frontier, gaps become tentatively pending
//     (they may still arrive; final loss accounting happens in Flush)
func (t *SequenceTracker) Observe(seq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isSet(seq) {
		// Already seen — duplicate
		t.Stats.Duplicated.Add(1)
		return
	}

	t.set(seq)
	t.Stats.Delivered.Add(1)

	if seq < t.nextExpected {
		// Arrived late — reorder
		t.Stats.Reordered.Add(1)
		return
	}

	// seq >= nextExpected: advance the frontier
	// Slots between old nextExpected and seq are pending (in-flight).
	// We do NOT declare them lost here — that happens in Flush().
	// Clear old window slots as the frontier advances to reuse bit positions.
	if seq-t.nextExpected > t.windowSize {
		// Giant jump: clear the entire window to avoid stale bits
		for i := range t.window {
			t.window[i] = 0
		}
		// Re-mark seq as seen after clearing
		t.set(seq)
		t.nextExpected = seq + 1
	} else {
		// Normal advance
		t.nextExpected = seq + 1
	}
}

// Flush declares all sequence numbers in [0, lastSentSeq] that have not been
// received as lost. Call this once at end-of-run after the benchmark completes.
// Should only be called once; calling multiple times double-counts losses.
func (t *SequenceTracker) Flush(lastSentSeq uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for s := t.nextExpected; s <= lastSentSeq; s++ {
		if !t.isSet(s) {
			t.Stats.Lost.Add(1)
		}
	}
	// Also check any un-advanced seqnums below nextExpected that were never set
	// (this handles the case where the sender started at seqnum 1 and we started tracking from 0)
}

// Reset clears all state for reuse between warmup and measurement phases.
func (t *SequenceTracker) Reset() {
	t.mu.Lock()
	for i := range t.window {
		t.window[i] = 0
	}
	t.nextExpected = 0
	t.Stats.Delivered.Store(0)
	t.Stats.Lost.Store(0)
	t.Stats.Duplicated.Store(0)
	t.Stats.Reordered.Store(0)
	t.mu.Unlock()
}

// Snapshot returns a SeqStats copy (not the live atomic values — use Stats.* directly for live data).
type SeqStatsSnapshot struct {
	Delivered  uint64
	Lost       uint64
	Duplicated uint64
	Reordered  uint64
}

func (t *SequenceTracker) Snapshot() SeqStatsSnapshot {
	return SeqStatsSnapshot{
		Delivered:  t.Stats.Delivered.Load(),
		Lost:       t.Stats.Lost.Load(),
		Duplicated: t.Stats.Duplicated.Load(),
		Reordered:  t.Stats.Reordered.Load(),
	}
}

// LossRate returns the packet loss rate as a fraction [0, 1].
// Returns 0 if no packets have been tracked.
func (t *SequenceTracker) LossRate() float64 {
	delivered := t.Stats.Delivered.Load()
	lost := t.Stats.Lost.Load()
	total := delivered + lost
	if total == 0 {
		return 0
	}
	return float64(lost) / float64(total)
}
