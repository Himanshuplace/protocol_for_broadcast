package udp

import (
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
	scenarios.Register("udp", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := NewUDPServer(addr,
			WithServerLogger(logger),
		)
		return &udpBenchTransport{server: srv, cfg: cfg, logger: logger}, nil
	})
}

type udpBenchTransport struct {
	server  *UDPServer
	clients []*UDPClient
	mu      sync.Mutex
	cfg     scenarios.ScenarioConfig
	logger  *zap.Logger
}

func (t *udpBenchTransport) Start() error {
	if err := t.server.Start(); err != nil {
		return err
	}

	addr := t.server.Addr().String()
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
		c := NewUDPClient(opts...)
		if err := c.Dial(addr); err != nil {
			return fmt.Errorf("udp bench: connect client %d: %w", i, err)
		}
		t.clients = append(t.clients, c)
	}
	return waitForConns(t.server, count)
}

func (t *udpBenchTransport) Stop() error {
	t.mu.Lock()
	clients := t.clients
	t.clients = nil
	t.mu.Unlock()

	for _, c := range clients {
		c.Close()
	}
	return t.server.Stop()
}

func (t *udpBenchTransport) Broadcast(data []byte) error { return t.server.Broadcast(data) }
func (t *udpBenchTransport) Send(id transport.ConnID, data []byte) error {
	return t.server.Send(id, data)
}
func (t *udpBenchTransport) Connections() int        { return t.server.Connections() }
func (t *udpBenchTransport) Stats() transport.Stats  { return t.server.Stats() }
