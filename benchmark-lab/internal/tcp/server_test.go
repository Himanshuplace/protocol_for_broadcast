package tcp_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/internal/tcp"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// TestTCPServer_StartStop verifies that Start and Stop work correctly.
func TestTCPServer_StartStop(t *testing.T) {
	srv := tcp.NewTCPServer("127.0.0.1:0")

	if err := srv.Start(); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if !srv.IsStarted() {
		t.Fatal("expected server to be started")
	}

	if addr := srv.Addr(); addr == nil {
		t.Fatal("Addr() returned nil after Start")
	}

	if err := srv.Stop(); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	if srv.IsStarted() {
		t.Fatal("expected server to be stopped")
	}

	// Double-stop must return ErrNotStarted.
	if err := srv.Stop(); err != transport.ErrNotStarted {
		t.Fatalf("expected ErrNotStarted on double-stop, got: %v", err)
	}
}

// TestTCPServer_Broadcast starts a server, connects 10 clients, broadcasts 100
// messages, and verifies every client receives all 100 messages.
func TestTCPServer_Broadcast(t *testing.T) {
	const numClients = 10
	const numMessages = 100
	const payload = "tcp-broadcast-test"

	srv := tcp.NewTCPServer("127.0.0.1:0")
	if err := srv.Start(); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	serverAddr := srv.Addr().String()

	// recvCounts[i] tracks how many messages client i received.
	recvCounts := make([]atomic.Int32, numClients)
	var allDone sync.WaitGroup
	allDone.Add(numClients * numMessages)

	clients := make([]*tcp.TCPClient, numClients)
	for i := 0; i < numClients; i++ {
		idx := i
		cli := tcp.NewTCPClient(
			tcp.WithTCPClientRecvHandler(func(_ transport.ConnID, data []byte, _ time.Time) {
				if len(data) >= wire.HeaderLen {
					f, _, err := wire.Decode(data)
					if err == nil && string(f.Payload) == payload {
						if recvCounts[idx].Add(1) <= numMessages {
							allDone.Done()
						}
					}
				}
			}),
		)
		if err := cli.Dial(serverAddr); err != nil {
			t.Fatalf("client %d Dial: %v", i, err)
		}
		clients[i] = cli
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	// Wait for all clients to be registered on the server.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Connections() >= numClients {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if srv.Connections() < numClients {
		t.Fatalf("server has %d connections, want %d", srv.Connections(), numClients)
	}

	// Broadcast numMessages times.
	for i := 0; i < numMessages; i++ {
		if err := srv.Broadcast([]byte(payload)); err != nil {
			t.Fatalf("Broadcast %d: %v", i, err)
		}
	}

	// Wait for all messages to be received with a timeout.
	done := make(chan struct{})
	go func() {
		allDone.Wait()
		close(done)
	}()
	select {
	case <-done:
		// all received
	case <-time.After(10 * time.Second):
		for i, cnt := range recvCounts {
			t.Logf("client %d received %d/%d", i, cnt.Load(), numMessages)
		}
		t.Fatalf("timeout waiting for all messages")
	}
}

// BenchmarkTCP_Broadcast_100 benchmarks broadcasting to 100 concurrent TCP clients.
func BenchmarkTCP_Broadcast_100(b *testing.B) {
	const numClients = 100
	payload := make([]byte, 128) // 128-byte payload

	srv := tcp.NewTCPServer("127.0.0.1:0")
	if err := srv.Start(); err != nil {
		b.Fatalf("server Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	serverAddr := srv.Addr().String()

	clients := make([]*tcp.TCPClient, numClients)
	for i := 0; i < numClients; i++ {
		cli := tcp.NewTCPClient()
		if err := cli.Dial(serverAddr); err != nil {
			b.Fatalf("client %d Dial: %v", i, err)
		}
		clients[i] = cli
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	// Wait for all connections to be registered.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Connections() >= numClients {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := srv.Broadcast(payload); err != nil {
			b.Fatal(err)
		}
	}
}
