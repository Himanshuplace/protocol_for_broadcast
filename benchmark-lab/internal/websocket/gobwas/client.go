package gobwas

import (
	"bufio"
	"context"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/gobwas/ws"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

const (
	gobwasMaxReconnectAttempts = 10
	gobwasReconnectBaseDelay   = 100 * time.Millisecond
	gobwasReconnectMaxDelay    = 10 * time.Second
)

// GobwasClient is a WebSocket client backed by gobwas/ws.
// It reconnects automatically with exponential backoff on connection loss.
type GobwasClient struct {
	addr    string
	handler transport.RecvHandler
	logger  *zap.Logger

	mu      sync.Mutex
	conn    net.Conn
	br      *bufio.Reader
	writeCh chan []byte
	ctx     context.Context
	cancel  context.CancelFunc
	connMu  sync.Mutex // serialises concurrent writes
}

// NewGobwasClient creates a client that will connect to ws://addr/ws.
func NewGobwasClient(addr string, handler transport.RecvHandler, logger *zap.Logger) *GobwasClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &GobwasClient{
		addr:    addr,
		handler: handler,
		logger:  logger,
		writeCh: make(chan []byte, gobwasWriteChanCap),
	}
}

// Connect dials the server and starts background pumps.
// Blocks until the first connection attempt succeeds or all retries are exhausted.
func (c *GobwasClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	conn, br, err := c.dialWithRetry(c.ctx, gobwasMaxReconnectAttempts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.br = br
	c.mu.Unlock()

	go c.writePump(c.ctx)
	go c.readPump(c.ctx)
	return nil
}

// dialWithRetry dials with exponential backoff.
func (c *GobwasClient) dialWithRetry(ctx context.Context, maxAttempts int) (net.Conn, *bufio.Reader, error) {
	url := "ws://" + c.addr + "/ws"
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := gobwasReconnectDelay(attempt)
			c.logger.Info("gobwas client reconnecting",
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		start := time.Now()
		conn, br, _, err := ws.Dial(ctx, url)
		if err == nil {
			c.logger.Info("gobwas client connected",
				zap.String("addr", c.addr),
				zap.Duration("handshake", time.Since(start)))
			return conn, br, nil
		}
		lastErr = err
		c.logger.Warn("gobwas client dial failed",
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}
	return nil, nil, lastErr
}

// gobwasReconnectDelay computes exponential backoff with full jitter.
func gobwasReconnectDelay(attempt int) time.Duration {
	cap := float64(gobwasReconnectMaxDelay)
	base := float64(gobwasReconnectBaseDelay)
	exp := math.Min(cap, base*math.Pow(2, float64(attempt)))
	return time.Duration(rand.Float64() * exp)
}

// readPump reads inbound frames and dispatches to the handler.
// On error it attempts to reconnect.
func (c *GobwasClient) readPump(ctx context.Context) {
	for {
		c.mu.Lock()
		conn := c.conn
		br := c.br
		c.mu.Unlock()
		if conn == nil {
			return
		}

		for {
			_ = conn.SetReadDeadline(time.Now().Add(gobwasReadTimeout))
			frame, err := ws.ReadFrame(br)
			recvAt := time.Now()
			if err != nil {
				c.logger.Debug("gobwas client read error", zap.Error(err))
				break
			}

			switch frame.Header.OpCode {
			case ws.OpClose:
				return
			case ws.OpPing:
				c.connMu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
				_ = ws.WriteFrame(conn, ws.NewPongFrame(frame.Payload))
				c.connMu.Unlock()
				continue
			case ws.OpPong:
				continue
			case ws.OpBinary, ws.OpText:
				// Fall through.
			default:
				continue
			}

			// Server frames are unmasked (server → client).
			if c.handler != nil {
				data := make([]byte, len(frame.Payload))
				copy(data, frame.Payload)
				c.handler("client", data, recvAt)
			}
		}

		// Connection lost — try to reconnect.
		select {
		case <-ctx.Done():
			return
		default:
		}
		reconn := time.Now()
		newConn, newBr, err := c.dialWithRetry(ctx, gobwasMaxReconnectAttempts)
		if err != nil {
			c.logger.Error("gobwas client reconnect exhausted", zap.Error(err))
			return
		}
		c.logger.Info("gobwas client reconnected", zap.Duration("took", time.Since(reconn)))
		c.mu.Lock()
		c.conn = newConn
		c.br = newBr
		conn = newConn
		br = newBr
		c.mu.Unlock()
	}
}

// writePump drains writeCh and sends binary frames.
func (c *GobwasClient) writePump(ctx context.Context) {
	ticker := time.NewTicker(gobwasPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-c.writeCh:
			if !ok {
				return
			}
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()
			if conn == nil {
				continue
			}
			c.connMu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
			err := ws.WriteFrame(conn, ws.NewBinaryFrame(data))
			c.connMu.Unlock()
			if err != nil {
				c.logger.Debug("gobwas client write error", zap.Error(err))
			}
		case <-ticker.C:
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()
			if conn == nil {
				continue
			}
			c.connMu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
			_ = ws.WriteFrame(conn, ws.NewPingFrame(nil))
			c.connMu.Unlock()
		}
	}
}

// Send enqueues data for sending. Non-blocking; returns error if channel is full.
func (c *GobwasClient) Send(data []byte) error {
	select {
	case c.writeCh <- data:
		return nil
	default:
		return transport.ErrBroadcastFailed
	}
}

// Close gracefully shuts down the client.
func (c *GobwasClient) Close() {
	c.mu.Lock()
	cancel := c.cancel
	conn := c.conn
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if conn != nil {
		c.connMu.Lock()
		_ = ws.WriteFrame(conn, ws.NewCloseFrame(nil))
		c.connMu.Unlock()
		conn.Close()
	}
}
