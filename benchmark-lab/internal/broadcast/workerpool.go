package broadcast

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/panjf2000/ants/v2"
	"github.com/valyala/bytebufferpool"
)

const numShards = 16

// WorkerPoolBroadcaster fans out messages to all writers using a goroutine pool.
// Writers are spread across numShards (16) shards so that pool tasks are coarser-grained
// than one-task-per-writer, reducing scheduling overhead at scale.
//
// Broadcast submits one task per shard to the pool (up to 16 concurrent goroutines).
// A sync.WaitGroup ensures Broadcast does not return until every writer in every shard
// has been written to, giving callers a synchronous delivery guarantee equivalent to
// NaiveBroadcaster but with parallel execution.
//
// Buffer recycling via bytebufferpool ensures the broadcast payload is not allocated
// per-shard; each shard receives a reference-counted copy derived from the pool.
type WorkerPoolBroadcaster struct {
	pool   *ants.Pool
	shards [numShards]poolShard
}

type poolShard struct {
	mu      sync.RWMutex
	writers []Writer
}

// NewWorkerPoolBroadcaster creates a WorkerPoolBroadcaster with runtime.NumCPU() workers.
// Callers must call Release() when done to free the goroutine pool.
func NewWorkerPoolBroadcaster() (*WorkerPoolBroadcaster, error) {
	workers := runtime.NumCPU()
	if workers < numShards {
		workers = numShards // at least one potential goroutine per shard
	}
	pool, err := ants.NewPool(workers, ants.WithPreAlloc(true))
	if err != nil {
		return nil, fmt.Errorf("workerpool broadcaster: create pool: %w", err)
	}
	return &WorkerPoolBroadcaster{pool: pool}, nil
}

// Release frees the underlying goroutine pool. Must be called when the broadcaster
// is no longer needed.
func (b *WorkerPoolBroadcaster) Release() {
	b.pool.Release()
}

// Broadcast submits one task per shard to the goroutine pool. All 16 tasks run
// concurrently (up to pool capacity). Broadcast blocks until all tasks complete.
//
// Data is copied into a bytebufferpool.Buffer per task so each shard receives
// its own slice that won't be invalidated if the caller reuses data afterward.
func (b *WorkerPoolBroadcaster) Broadcast(data []byte) error {
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	wg.Add(numShards)
	for i := 0; i < numShards; i++ {
		shardIdx := i
		shard := &b.shards[shardIdx]

		// Snapshot shard writers under read lock to avoid holding the lock
		// inside the pool goroutine.
		shard.mu.RLock()
		if len(shard.writers) == 0 {
			shard.mu.RUnlock()
			wg.Done()
			continue
		}
		writers := make([]Writer, len(shard.writers))
		copy(writers, shard.writers)
		shard.mu.RUnlock()

		// Copy payload into a pooled buffer so the shard goroutine owns the data.
		buf := bytebufferpool.Get()
		buf.Set(data)

		err := b.pool.Submit(func() {
			defer wg.Done()
			defer bytebufferpool.Put(buf)

			payload := buf.Bytes()
			for _, w := range writers {
				if werr := w.Write(payload); werr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("workerpool broadcast: shard %d: writer %s: %w",
							shardIdx, w.ID(), werr)
					}
					errMu.Unlock()
				}
			}
		})
		if err != nil {
			// Pool is full or shut down; fall back to inline execution.
			wg.Done()
			bytebufferpool.Put(buf)
			for _, w := range writers {
				if werr := w.Write(data); werr != nil && firstErr == nil {
					firstErr = fmt.Errorf("workerpool broadcast fallback: shard %d: writer %s: %w",
						shardIdx, w.ID(), werr)
				}
			}
		}
	}

	wg.Wait()
	return firstErr
}

// Add registers w in the shard determined by the writer's ID hash.
func (b *WorkerPoolBroadcaster) Add(w Writer) {
	idx := shardIndex(w.ID())
	shard := &b.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	for i, existing := range shard.writers {
		if existing.ID() == w.ID() {
			shard.writers[i] = w
			return
		}
	}
	shard.writers = append(shard.writers, w)
}

// Remove deregisters the writer with the given id from its shard.
func (b *WorkerPoolBroadcaster) Remove(id string) {
	idx := shardIndex(id)
	shard := &b.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	for i, w := range shard.writers {
		if w.ID() == id {
			last := len(shard.writers) - 1
			shard.writers[i] = shard.writers[last]
			shard.writers[last] = nil
			shard.writers = shard.writers[:last]
			return
		}
	}
}

// Len returns the total number of registered writers across all shards.
func (b *WorkerPoolBroadcaster) Len() int {
	total := 0
	for i := 0; i < numShards; i++ {
		b.shards[i].mu.RLock()
		total += len(b.shards[i].writers)
		b.shards[i].mu.RUnlock()
	}
	return total
}

// shardIndex maps an ID string to a shard index in [0, numShards).
// Uses FNV-like inline hash for minimal overhead.
func shardIndex(id string) int {
	h := uint32(2166136261)
	for i := 0; i < len(id); i++ {
		h ^= uint32(id[i])
		h *= 16777619
	}
	return int(h % numShards)
}
