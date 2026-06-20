package tcp

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

// waitForConns polls until srv reports at least want active connections or 10s pass.
// This is needed because several client Connect() calls return before the server-side
// handler goroutine has registered the connection in its registry.
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
	scenarios.Register("tcp", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := NewTCPServer(addr, WithTCPServerLogger(logger))
		return &tcpBenchTransport{server: srv, cfg: cfg, logger: logger}, nil
	})
}

// tcpBenchTransport wraps TCPServer and spawns ReceiverCount clients internally
// so the scenario runner gets an end-to-end benchmark without external processes.
type tcpBenchTransport struct {
	server  *TCPServer
	clients []*TCPClient
	mu      sync.Mutex
	cfg     scenarios.ScenarioConfig
	logger  *zap.Logger
}

func (t *tcpBenchTransport) Start() error {
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
		var opts []TCPClientOption
		opts = append(opts, WithTCPClientLogger(t.logger))
		if t.cfg.RecvHandler != nil {
			opts = append(opts, WithTCPClientRecvHandler(t.cfg.RecvHandler))
		}
		c := NewTCPClient(opts...)
		if err := c.Dial(addr); err != nil {
			return fmt.Errorf("tcp bench: connect client %d: %w", i, err)
		}
		t.clients = append(t.clients, c)
	}
	return waitForConns(t.server, count)
}

func (t *tcpBenchTransport) Stop() error {
	t.mu.Lock()
	clients := t.clients
	t.clients = nil
	t.mu.Unlock()

	for _, c := range clients {
		_ = c.Close()
	}
	return t.server.Stop()
}

func (t *tcpBenchTransport) Broadcast(data []byte) error  { return t.server.Broadcast(data) }
func (t *tcpBenchTransport) Send(id transport.ConnID, data []byte) error {
	return t.server.Send(id, data)
}
func (t *tcpBenchTransport) Connections() int { return t.server.Connections() }
func (t *tcpBenchTransport) Stats() transport.Stats { return t.server.Stats() }
