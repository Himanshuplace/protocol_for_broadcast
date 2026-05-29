package broadcast

import (
	"fmt"
	"sync"
)

// shardChanCap is the capacity of each shard's message channel.
// If a shard's goroutine falls behind, sends beyond this cap are dropped
// to prevent Broadcast from blocking the caller.
const shardChanCap = 256

// broadcastShard is one partition of the ShardedBroadcaster.
// Each shard owns a dedicated goroutine that reads messages from ch and
// delivers them to all writers in the shard. This decouples the caller
// from the delivery loop: Broadcast returns as soon as all shard channels
// have been notified (or drops the notification if the channel is full).
type broadcastShard struct {
	mu      sync.RWMutex
	writers []Writer
	ch      chan []byte
}

// ShardedBroadcaster partitions writers across numShards (16) shards, each
// backed by a dedicated goroutine and buffered channel (cap 256).
//
// Broadcast fans the payload out to all shard channels concurrently.  Sends
// are non-blocking: if a shard's channel is full the message is dropped for
// that shard to prevent one slow shard from stalling all others.
//
// Call Start() before using the broadcaster and Stop() to shut down the
// background goroutines cleanly.
type ShardedBroadcaster struct {
	shards [numShards]broadcastShard
	stop   chan struct{}
	wg     sync.WaitGroup
}

// NewShardedBroadcaster creates a ShardedBroadcaster. Call Start() to launch
// the shard goroutines before the first Broadcast.
func NewShardedBroadcaster() *ShardedBroadcaster {
	b := &ShardedBroadcaster{
		stop: make(chan struct{}),
	}
	for i := 0; i < numShards; i++ {
		b.shards[i].ch = make(chan []byte, shardChanCap)
	}
	return b
}

// Start launches one goroutine per shard.  It must be called exactly once
// before any Broadcast calls.
func (b *ShardedBroadcaster) Start() {
	for i := 0; i < numShards; i++ {
		b.wg.Add(1)
		go b.runShard(i)
	}
}

// Stop signals all shard goroutines to exit and waits for them to finish.
// After Stop returns no further Broadcast calls should be made.
func (b *ShardedBroadcaster) Stop() {
	close(b.stop)
	b.wg.Wait()
}

// runShard is the event loop for a single shard.  It reads payloads from the
// shard channel and delivers them to every writer currently registered in the
// shard.
func (b *ShardedBroadcaster) runShard(idx int) {
	defer b.wg.Done()
	shard := &b.shards[idx]

	for {
		select {
		case data, ok := <-shard.ch:
			if !ok {
				return
			}
			shard.mu.RLock()
			writers := shard.writers
			shard.mu.RUnlock()

			for _, w := range writers {
				_ = w.Write(data) // best-effort; shard goroutine does not aggregate errors
			}

		case <-b.stop:
			// Drain any remaining messages before exiting.
			for {
				select {
				case data := <-shard.ch:
					shard.mu.RLock()
					writers := shard.writers
					shard.mu.RUnlock()
					for _, w := range writers {
						_ = w.Write(data)
					}
				default:
					return
				}
			}
		}
	}
}

// Broadcast fans data out to all numShards shard channels concurrently.
// The send to each channel is non-blocking: if the channel buffer is full
// the payload is dropped for that shard (counted as a lost delivery).
// The returned error is non-nil only if every shard's channel is full.
func (b *ShardedBroadcaster) Broadcast(data []byte) error {
	dropped := 0
	for i := 0; i < numShards; i++ {
		select {
		case b.shards[i].ch <- data:
		default:
			dropped++
		}
	}
	if dropped == numShards {
		return fmt.Errorf("sharded broadcast: all %d shard channels full, message dropped", numShards)
	}
	return nil
}

// Add registers w in the shard determined by the writer's ID hash.
func (b *ShardedBroadcaster) Add(w Writer) {
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
func (b *ShardedBroadcaster) Remove(id string) {
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

// Len returns the total number of writers across all shards.
func (b *ShardedBroadcaster) Len() int {
	total := 0
	for i := 0; i < numShards; i++ {
		b.shards[i].mu.RLock()
		total += len(b.shards[i].writers)
		b.shards[i].mu.RUnlock()
	}
	return total
}
