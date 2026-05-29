package udp_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/internal/udp"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// TestUDPServer_StartStop verifies that Start and Stop work correctly.
func TestUDPServer_StartStop(t *testing.T) {
	srv := udp.NewUDPServer("127.0.0.1:0")

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

// TestUDPServer_SendReceive starts a server, connects 3 clients, broadcasts a
// message, and verifies all clients receive it.
func TestUDPServer_SendReceive(t *testing.T) {
	const numClients = 3
	const payload = "hello-broadcast"

	srv := udp.NewUDPServer("127.0.0.1:0")
	if err := srv.Start(); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	serverAddr := srv.Addr().String()

	// recv tracks how many clients received the broadcast.
	var recv atomic.Int32
	var wg sync.WaitGroup

	clients := make([]*udp.UDPClient, numClients)
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		cli := udp.NewUDPClient(
			udp.WithClientRecvHandler(func(_ transport.ConnID, data []byte, _ time.Time) {
				if len(data) >= wire.HeaderLen {
					f, _, err := wire.Decode(data)
					if err == nil && string(f.Payload) == payload {
						// Prevent WaitGroup from going below zero on duplicates.
						if recv.Add(1) <= numClients {
							wg.Done()
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

	// Wait for all clients to register with the server (HELLO packet).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Connections() >= numClients {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if srv.Connections() < numClients {
		t.Fatalf("server has %d connections, want %d", srv.Connections(), numClients)
	}

	// Broadcast.
	if err := srv.Broadcast([]byte(payload)); err != nil {
		t.Fatalf("Broadcast: %v", err)
	}

	// Wait for all receives with a timeout.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		// all received
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout: only %d/%d clients received broadcast", recv.Load(), numClients)
	}
}

// BenchmarkUDP_Broadcast_100 benchmarks broadcasting to 100 peers.
func BenchmarkUDP_Broadcast_100(b *testing.B) {
	const numClients = 100
	payload := make([]byte, 128) // 128-byte payload

	srv := udp.NewUDPServer("127.0.0.1:0")
	if err := srv.Start(); err != nil {
		b.Fatalf("server Start: %v", err)
	}
	defer func() { _ = srv.Stop() }()

	serverAddr := srv.Addr().String()

	clients := make([]*udp.UDPClient, numClients)
	for i := 0; i < numClients; i++ {
		cli := udp.NewUDPClient()
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

	// Wait for peer registration.
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
