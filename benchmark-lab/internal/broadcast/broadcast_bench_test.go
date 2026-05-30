package broadcast_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/internal/broadcast"
)

// mockWriter implements broadcast.Writer using an atomic counter to confirm delivery.
type mockWriter struct {
	id       string
	received atomic.Uint64
}

func (w *mockWriter) Write(_ []byte) error {
	w.received.Add(1)
	return nil
}
func (w *mockWriter) ID() string { return w.id }

// makeWriters creates n mock writers and registers them on b.
func makeWriters(b broadcast.Broadcaster, n int) []*mockWriter {
	writers := make([]*mockWriter, n)
	for i := range writers {
		writers[i] = &mockWriter{id: fmt.Sprintf("w%d", i)}
		b.Add(writers[i])
	}
	return writers
}

// payload is a 1 KB benchmark payload (matches benchmark-runner default).
var payload = make([]byte, 1024)

func benchBroadcast(b *testing.B, bc broadcast.Broadcaster, n int) {
	b.Helper()
	makeWriters(bc, n)
	data := payload
	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(n), "receivers")
	for i := 0; i < b.N; i++ {
		if err := bc.Broadcast(data); err != nil {
			b.Fatal(err)
		}
	}
}

// ── Naive ────────────────────────────────────────────────────────────────────

func BenchmarkBroadcast_Naive_10(b *testing.B) {
	benchBroadcast(b, broadcast.NewNaiveBroadcaster(), 10)
}

func BenchmarkBroadcast_Naive_100(b *testing.B) {
	benchBroadcast(b, broadcast.NewNaiveBroadcaster(), 100)
}

func BenchmarkBroadcast_Naive_1000(b *testing.B) {
	benchBroadcast(b, broadcast.NewNaiveBroadcaster(), 1000)
}

func BenchmarkBroadcast_Naive_10000(b *testing.B) {
	benchBroadcast(b, broadcast.NewNaiveBroadcaster(), 10000)
}

// ── Worker Pool ───────────────────────────────────────────────────────────────

func BenchmarkBroadcast_WorkerPool_10(b *testing.B) {
	bc, err := broadcast.NewWorkerPoolBroadcaster()
	if err != nil {
		b.Fatal(err)
	}
	defer bc.Release()
	benchBroadcast(b, bc, 10)
}

func BenchmarkBroadcast_WorkerPool_100(b *testing.B) {
	bc, err := broadcast.NewWorkerPoolBroadcaster()
	if err != nil {
		b.Fatal(err)
	}
	defer bc.Release()
	benchBroadcast(b, bc, 100)
}

func BenchmarkBroadcast_WorkerPool_1000(b *testing.B) {
	bc, err := broadcast.NewWorkerPoolBroadcaster()
	if err != nil {
		b.Fatal(err)
	}
	defer bc.Release()
	benchBroadcast(b, bc, 1000)
}

func BenchmarkBroadcast_WorkerPool_10000(b *testing.B) {
	bc, err := broadcast.NewWorkerPoolBroadcaster()
	if err != nil {
		b.Fatal(err)
	}
	defer bc.Release()
	benchBroadcast(b, bc, 10000)
}

// ── Sharded ───────────────────────────────────────────────────────────────────

func BenchmarkBroadcast_Sharded_10(b *testing.B) {
	bc := broadcast.NewShardedBroadcaster()
	defer bc.Stop()
	benchBroadcast(b, bc, 10)
}

func BenchmarkBroadcast_Sharded_100(b *testing.B) {
	bc := broadcast.NewShardedBroadcaster()
	defer bc.Stop()
	benchBroadcast(b, bc, 100)
}

func BenchmarkBroadcast_Sharded_1000(b *testing.B) {
	bc := broadcast.NewShardedBroadcaster()
	defer bc.Stop()
	benchBroadcast(b, bc, 1000)
}

func BenchmarkBroadcast_Sharded_10000(b *testing.B) {
	bc := broadcast.NewShardedBroadcaster()
	defer bc.Stop()
	benchBroadcast(b, bc, 10000)
}

// ── Correctness: all writers receive every message ───────────────────────────

func TestBroadcast_AllWritersReceive(t *testing.T) {
	for _, tc := range []struct {
		name    string
		newFunc func() broadcast.Broadcaster
		cleanup func(broadcast.Broadcaster)
	}{
		{
			name:    "naive",
			newFunc: func() broadcast.Broadcaster { return broadcast.NewNaiveBroadcaster() },
			cleanup: func(_ broadcast.Broadcaster) {},
		},
		{
			name: "workerpool",
			newFunc: func() broadcast.Broadcaster {
				bc, err := broadcast.NewWorkerPoolBroadcaster()
				if err != nil {
					t.Fatal(err)
				}
				return bc
			},
			cleanup: func(bc broadcast.Broadcaster) { bc.(*broadcast.WorkerPoolBroadcaster).Release() },
		},
		{
			name: "sharded",
			newFunc: func() broadcast.Broadcaster {
				return broadcast.NewShardedBroadcaster()
			},
			cleanup: func(bc broadcast.Broadcaster) { bc.(*broadcast.ShardedBroadcaster).Stop() },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bc := tc.newFunc()
			defer tc.cleanup(bc)

			const n = 50
			const msgs = 100
			writers := makeWriters(bc, n)

			for i := 0; i < msgs; i++ {
				if err := bc.Broadcast(payload); err != nil {
					t.Fatalf("broadcast %d: %v", i, err)
				}
			}

			// ShardedBroadcaster delivers asynchronously via goroutines.
			// Poll until all writers have received all messages or a timeout fires.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				allDone := true
				for _, w := range writers {
					if w.received.Load() < uint64(msgs) {
						allDone = false
						break
					}
				}
				if allDone {
					break
				}
				time.Sleep(time.Millisecond)
			}

			for i, w := range writers {
				if got := w.received.Load(); got != msgs {
					t.Errorf("writer[%d] received %d/%d messages", i, got, msgs)
				}
			}
		})
	}
}
