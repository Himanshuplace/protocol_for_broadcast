package http3

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func waitForConns(srv interface{ Connections() int }, want int) error {
	deadline := time.Now().Add(15 * time.Second) // QUIC handshakes may be slower
	for time.Now().Before(deadline) {
		if srv.Connections() >= want {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("bench transport: timeout waiting for %d connections, got %d", want, srv.Connections())
}

func init() {
	scenarios.Register("http3", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := NewHTTP3Server(addr, WithLogger(logger))
		return &http3BenchTransport{server: srv, addr: addr, cfg: cfg, logger: logger}, nil
	})
}

type http3BenchTransport struct {
	server  *HTTP3Server
	addr    string
	clients []*HTTP3Client
	mu      sync.Mutex
	cfg     scenarios.ScenarioConfig
	logger  *zap.Logger
}

func (t *http3BenchTransport) Start() error {
	if err := t.server.Start(); err != nil {
		return err
	}
	time.Sleep(150 * time.Millisecond)

	count := t.cfg.ReceiverCount
	if count <= 0 {
		count = 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < count; i++ {
		var opts []ClientOption
		opts = append(opts, WithClientLogger(t.logger))
		if t.cfg.RecvHandler != nil {
			opts = append(opts, WithClientRecvHandler(t.cfg.RecvHandler))
		}
		c := NewHTTP3Client(t.addr, opts...)
		if err := c.Start(); err != nil {
			return fmt.Errorf("http3 bench: start client %d: %w", i, err)
		}
		t.clients = append(t.clients, c)
	}
	return waitForConns(t.server, count)
}

func (t *http3BenchTransport) Stop() error {
	t.mu.Lock()
	clients := t.clients
	t.clients = nil
	t.mu.Unlock()

	for _, c := range clients {
		_ = c.Stop()
	}
	return t.server.Stop()
}

func (t *http3BenchTransport) Broadcast(data []byte) error { return t.server.Broadcast(data) }
func (t *http3BenchTransport) Send(id transport.ConnID, data []byte) error {
	return t.server.Send(id, data)
}
func (t *http3BenchTransport) Connections() int       { return t.server.Connections() }
func (t *http3BenchTransport) Stats() transport.Stats { return t.server.Stats() }
