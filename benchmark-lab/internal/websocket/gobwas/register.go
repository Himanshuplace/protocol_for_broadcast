package gobwas

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
	scenarios.Register("websocket-gobwas", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := NewGobwasServer(transport.TransportConfig{ListenAddr: addr}, logger)
		return &gobwasBenchTransport{server: srv, addr: addr, cfg: cfg, logger: logger}, nil
	})
}

type gobwasBenchTransport struct {
	server  *GobwasServer
	addr    string
	clients []*GobwasClient
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     scenarios.ScenarioConfig
	logger  *zap.Logger
}

func (t *gobwasBenchTransport) Start() error {
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
		c := NewGobwasClient(t.addr, t.cfg.RecvHandler, t.logger)
		if err := c.Connect(t.ctx); err != nil {
			return fmt.Errorf("gobwas bench: connect client %d: %w", i, err)
		}
		t.clients = append(t.clients, c)
	}
	return waitForConns(t.server, count)
}

func (t *gobwasBenchTransport) Stop() error {
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

func (t *gobwasBenchTransport) Broadcast(data []byte) error { return t.server.Broadcast(data) }
func (t *gobwasBenchTransport) Send(id transport.ConnID, data []byte) error {
	return t.server.Send(id, data)
}
func (t *gobwasBenchTransport) Connections() int       { return t.server.Connections() }
func (t *gobwasBenchTransport) Stats() transport.Stats { return t.server.Stats() }
