package http1

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func waitForConns(srv interface{ Connections() int }, want int) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Connections() >= want {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("bench transport: timeout waiting for %d connections, got %d", want, srv.Connections())
}

func init() {
	scenarios.Register("http1", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := NewHTTP1Server(transport.TransportConfig{ListenAddr: addr}, logger)
		return &http1BenchTransport{server: srv, addr: addr, cfg: cfg, logger: logger}, nil
	})
}

type http1BenchTransport struct {
	server  *HTTP1Server
	addr    string
	clients []*HTTP1Client
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     scenarios.ScenarioConfig
	logger  *zap.Logger
}

func (t *http1BenchTransport) Start() error {
	t.ctx, t.cancel = context.WithCancel(context.Background())
	if err := t.server.Start(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)

	count := t.cfg.ReceiverCount
	if count <= 0 {
		count = 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < count; i++ {
		c := NewHTTP1Client(t.addr, t.cfg.RecvHandler, t.logger)
		if err := c.Connect(t.ctx); err != nil {
			return fmt.Errorf("http1 bench: connect client %d: %w", i, err)
		}
		t.clients = append(t.clients, c)
	}
	return waitForConns(t.server, count)
}

func (t *http1BenchTransport) Stop() error {
	t.mu.Lock()
	clients := t.clients
	t.clients = nil
	t.mu.Unlock()

	if t.cancel != nil {
		t.cancel()
	}
	for _, c := range clients {
		c.Close()
	}
	return t.server.Stop()
}

func (t *http1BenchTransport) Broadcast(data []byte) error { return t.server.Broadcast(data) }
func (t *http1BenchTransport) Send(id transport.ConnID, data []byte) error {
	return t.server.Send(id, data)
}
func (t *http1BenchTransport) Connections() int       { return t.server.Connections() }
func (t *http1BenchTransport) Stats() transport.Stats { return t.server.Stats() }
