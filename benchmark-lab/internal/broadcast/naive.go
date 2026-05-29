// Package broadcast provides multiple fanout strategies for benchmarking message delivery.
// Each broadcaster implements a common Broadcaster interface so scenario runners can
// swap strategies without changing the test harness.
package broadcast

import (
	"fmt"
	"sync"
)

// Writer is the minimal interface any connection must satisfy to receive broadcasts.
type Writer interface {
	// Write delivers data to the connection. Implementations must not retain data
	// after Write returns (copy if needed).
	Write(data []byte) error
	// ID returns the unique connection identifier (e.g., UUID or "ip:port").
	ID() string
}

// Broadcaster is the common interface implemented by every broadcast strategy.
type Broadcaster interface {
	// Broadcast sends data to all currently registered writers.
	Broadcast(data []byte) error
	// Add registers a writer. Safe to call concurrently.
	Add(w Writer)
	// Remove deregisters the writer identified by id. Safe to call concurrently.
	Remove(id string)
	// Len returns the number of currently registered writers.
	Len() int
}

// NaiveBroadcaster sends to all connections serially under a single mutex.
// This is the baseline strategy: it serialises all writes, which means throughput
// is bounded by the slowest writer in the set. Every Broadcast call acquires the
// mutex once, iterates the writer slice, and returns only after the last write.
//
// Use this to establish a lower-bound baseline before comparing async strategies.
type NaiveBroadcaster struct {
	mu      sync.Mutex
	writers []Writer
}

// NewNaiveBroadcaster returns a ready-to-use NaiveBroadcaster.
func NewNaiveBroadcaster() *NaiveBroadcaster {
	return &NaiveBroadcaster{}
}

// Broadcast iterates all writers serially while holding the mutex and calls Write on
// each one. Errors from individual writers are collected; the first error is returned
// after all writers have been attempted so that a single slow/failing writer does not
// skip subsequent writers.
func (b *NaiveBroadcaster) Broadcast(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for _, w := range b.writers {
		if err := w.Write(data); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("naive broadcast: writer %s: %w", w.ID(), err)
		}
	}
	return firstErr
}

// Add appends w to the writer list. If a writer with the same ID is already
// registered, it is replaced.
func (b *NaiveBroadcaster) Add(w Writer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, existing := range b.writers {
		if existing.ID() == w.ID() {
			b.writers[i] = w
			return
		}
	}
	b.writers = append(b.writers, w)
}

// Remove deregisters the writer whose ID matches id. It is a no-op if no such
// writer is found.
func (b *NaiveBroadcaster) Remove(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, w := range b.writers {
		if w.ID() == id {
			// Swap-delete to avoid shifting the whole slice.
			last := len(b.writers) - 1
			b.writers[i] = b.writers[last]
			b.writers[last] = nil // prevent memory leak
			b.writers = b.writers[:last]
			return
		}
	}
}

// Len returns the number of registered writers.
func (b *NaiveBroadcaster) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.writers)
}
