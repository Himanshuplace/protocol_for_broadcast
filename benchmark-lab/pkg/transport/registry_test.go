package transport

import (
	"fmt"
	"sync"
	"testing"
)

func TestRegistry_AddGetRemove(t *testing.T) {
	var r Registry[int]
	r.Add("conn-1", 42)
	v, ok := r.Get("conn-1")
	if !ok || v != 42 {
		t.Fatalf("Get: want (42, true), got (%d, %v)", v, ok)
	}
	r.Remove("conn-1")
	_, ok = r.Get("conn-1")
	if ok {
		t.Fatal("after Remove: expected not found")
	}
}

func TestRegistry_Len(t *testing.T) {
	var r Registry[string]
	for i := 0; i < 100; i++ {
		r.Add(ConnID(fmt.Sprintf("conn-%d", i)), fmt.Sprintf("val-%d", i))
	}
	if n := r.Len(); n != 100 {
		t.Errorf("Len: want 100, got %d", n)
	}
	for i := 0; i < 50; i++ {
		r.Remove(ConnID(fmt.Sprintf("conn-%d", i)))
	}
	if n := r.Len(); n != 50 {
		t.Errorf("Len after removes: want 50, got %d", n)
	}
}

func TestRegistry_Snapshot(t *testing.T) {
	var r Registry[int]
	for i := 0; i < 10; i++ {
		r.Add(ConnID(fmt.Sprintf("c%d", i)), i)
	}
	snap := r.Snapshot()
	if len(snap) != 10 {
		t.Errorf("Snapshot len: want 10, got %d", len(snap))
	}
	// Modify registry after snapshot — snapshot should be unaffected
	r.Remove("c0")
	if len(snap) != 10 {
		t.Errorf("Snapshot mutated after Remove")
	}
}

// TestRegistry_ConcurrentAddRemove verifies no data races under high concurrency.
// Run with: go test -race ./pkg/transport/
func TestRegistry_ConcurrentAddRemove(t *testing.T) {
	var r Registry[int]
	const goroutines = 50
	const ops = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				id := ConnID(fmt.Sprintf("conn-%d-%d", g, i))
				r.Add(id, g*ops+i)
				r.Get(id)
				r.Range(func(_ ConnID, _ int) bool { return true })
				r.Remove(id)
			}
		}()
	}
	wg.Wait()
}

// TestRegistry_ShardDistribution verifies keys spread across all 16 shards.
func TestRegistry_ShardDistribution(t *testing.T) {
	var r Registry[int]
	const n = 10000
	for i := 0; i < n; i++ {
		r.Add(ConnID(fmt.Sprintf("conn-%d", i)), i)
	}
	// Each shard should have roughly n/numShards entries (±50%)
	low := n/numShards/2
	high := n/numShards*3/2
	for i := range r.shards {
		r.shards[i].mu.RLock()
		count := len(r.shards[i].clients)
		r.shards[i].mu.RUnlock()
		if count < low || count > high {
			t.Errorf("shard %d has %d entries, want [%d, %d] (poor distribution)", i, count, low, high)
		}
	}
}

// BenchmarkRegistry_Add measures Add throughput under concurrency.
func BenchmarkRegistry_Add(b *testing.B) {
	var r Registry[int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			r.Add(ConnID(fmt.Sprintf("c%d", i)), i)
			i++
		}
	})
}

// BenchmarkRegistry_Snapshot_10K measures the cost of taking a full snapshot.
func BenchmarkRegistry_Snapshot_10K(b *testing.B) {
	var r Registry[int]
	for i := 0; i < 10000; i++ {
		r.Add(ConnID(fmt.Sprintf("c%d", i)), i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Snapshot()
	}
}
