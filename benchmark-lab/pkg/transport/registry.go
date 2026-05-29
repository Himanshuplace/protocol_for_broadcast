package transport

import (
	"hash/fnv"
	"sync"
)

// numShards is the number of independent lock shards in the registry.
// 16 shards reduce mutex contention by ~16× during concurrent broadcast fan-out.
// Chosen as a power of 2 to allow modulo via bitwise AND.
const numShards = 16

// Registry is a thread-safe, sharded connection map.
// It is generic over the connection type C — used as:
//
//	Registry[*net.UDPConn]   for UDP peers
//	Registry[net.Conn]        for TCP connections
//	Registry[*wsConn]         for WebSocket connections
//	Registry[quic.Connection] for QUIC sessions
//
// The 16 shards each have their own sync.RWMutex and are padded to 64 bytes
// (one CPU cache line) to eliminate false sharing between adjacent shards on
// multi-core CPUs. This is critical for AMD Ryzen (CCX cache topology) and
// Intel (ring bus) alike.
type Registry[C any] struct {
	shards [numShards]registryShard[C]
}

// registryShard is one partition of the registry.
// Size is carefully managed: sync.RWMutex = 24 bytes, map = 8 bytes, padding = 32 bytes → 64 bytes.
// Verify with: unsafe.Sizeof(registryShard[*int]{})
type registryShard[C any] struct {
	mu      sync.RWMutex        // 24 bytes
	clients map[ConnID]C        // 8 bytes (pointer)
	_       [32]byte            // pad to 64 bytes (one cache line)
}

// shardFor returns the shard that owns the given ConnID.
// FNV-1a 32-bit hash: non-cryptographic, fast, good distribution for string keys.
// On AMD64 the FNV multiply is a single IMULQ instruction.
func (r *Registry[C]) shardFor(id ConnID) *registryShard[C] {
	h := fnv.New32a()
	// WriteString never returns an error for FNV hash
	_, _ = h.Write([]byte(id))
	return &r.shards[h.Sum32()&(numShards-1)]
}

// Add inserts or replaces the connection for the given ID.
// If the shard's map is uninitialized (zero-value Registry), it is created here.
func (r *Registry[C]) Add(id ConnID, conn C) {
	s := r.shardFor(id)
	s.mu.Lock()
	if s.clients == nil {
		s.clients = make(map[ConnID]C, 64) // pre-size for typical connections per shard
	}
	s.clients[id] = conn
	s.mu.Unlock()
}

// Remove deletes the connection for the given ID. No-op if not present.
func (r *Registry[C]) Remove(id ConnID) {
	s := r.shardFor(id)
	s.mu.Lock()
	delete(s.clients, id)
	s.mu.Unlock()
}

// Get returns the connection for the given ID and whether it was found.
func (r *Registry[C]) Get(id ConnID) (C, bool) {
	s := r.shardFor(id)
	s.mu.RLock()
	c, ok := s.clients[id]
	s.mu.RUnlock()
	return c, ok
}

// Len returns the total number of registered connections across all shards.
// Acquires each shard's read lock in sequence — O(numShards).
func (r *Registry[C]) Len() int {
	n := 0
	for i := range r.shards {
		r.shards[i].mu.RLock()
		n += len(r.shards[i].clients)
		r.shards[i].mu.RUnlock()
	}
	return n
}

// Range iterates over all connections, calling fn(id, conn) for each.
// If fn returns false, iteration stops (but not necessarily immediately — current
// shard iteration finishes before stopping).
// Range acquires one shard's read lock at a time, NOT a global lock.
// This means Range is NOT a consistent snapshot — concurrent Add/Remove may affect
// shards not yet visited. This is acceptable for broadcast fan-out.
func (r *Registry[C]) Range(fn func(ConnID, C) bool) {
	for i := range r.shards {
		cont := true
		r.shards[i].mu.RLock()
		for id, conn := range r.shards[i].clients {
			if !fn(id, conn) {
				cont = false
				break
			}
		}
		r.shards[i].mu.RUnlock()
		if !cont {
			return
		}
	}
}

// Snapshot returns a slice containing all connection values at the time of the call.
// The returned slice is a fresh allocation and is safe to use after further registry
// modifications. Pre-allocates to the current Len() to avoid reallocations.
//
// Used by broadcast implementations to acquire the client list before calling
// per-connection write functions, allowing the registry lock to be released
// during the (potentially slow) network writes.
func (r *Registry[C]) Snapshot() []C {
	// Two-phase: count first (read locks), then collect (read locks again).
	// This avoids holding all locks simultaneously.
	total := r.Len()
	result := make([]C, 0, total)
	for i := range r.shards {
		r.shards[i].mu.RLock()
		for _, conn := range r.shards[i].clients {
			result = append(result, conn)
		}
		r.shards[i].mu.RUnlock()
	}
	return result
}

// SnapshotWithIDs returns parallel slices of ConnIDs and connections.
// Used when the broadcast needs to identify which connections failed.
func (r *Registry[C]) SnapshotWithIDs() ([]ConnID, []C) {
	total := r.Len()
	ids := make([]ConnID, 0, total)
	conns := make([]C, 0, total)
	for i := range r.shards {
		r.shards[i].mu.RLock()
		for id, conn := range r.shards[i].clients {
			ids = append(ids, id)
			conns = append(conns, conn)
		}
		r.shards[i].mu.RUnlock()
	}
	return ids, conns
}
