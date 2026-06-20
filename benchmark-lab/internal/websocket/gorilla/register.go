package gorilla

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
	scenarios.Register("websocket-gorilla", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := NewGorillaServer(transport.TransportConfig{ListenAddr: addr}, logger)
		return &gorillaBenchTransport{server: srv, addr: addr, cfg: cfg, logger: logger}, nil
	})
}

type gorillaBenchTransport struct {
	server  *GorillaServer
	addr    string
	clients []*GorillaClient
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     scenarios.ScenarioConfig
	logger  *zap.Logger
}

func (t *gorillaBenchTransport) Start() error {
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
		c := NewGorillaClient(t.addr, t.cfg.RecvHandler, t.logger)
		if err := c.Connect(t.ctx); err != nil {
			return fmt.Errorf("gorilla bench: connect client %d: %w", i, err)
		}
		t.clients = append(t.clients, c)
	}
	return waitForConns(t.server, count)
}

func (t *gorillaBenchTransport) Stop() error {
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

func (t *gorillaBenchTransport) Broadcast(data []byte) error { return t.server.Broadcast(data) }
func (t *gorillaBenchTransport) Send(id transport.ConnID, data []byte) error {
	return t.server.Send(id, data)
}
func (t *gorillaBenchTransport) Connections() int       { return t.server.Connections() }
func (t *gorillaBenchTransport) Stats() transport.Stats { return t.server.Stats() }
