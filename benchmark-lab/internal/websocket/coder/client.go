package coder

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	nhws "github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

const (
	coderMaxReconnectAttempts = 10
	coderReconnectBaseDelay   = 100 * time.Millisecond
	coderReconnectMaxDelay    = 10 * time.Second
)

// CoderClient is a WebSocket client backed by coder/websocket.
// It reconnects automatically with exponential backoff on connection loss.
type CoderClient struct {
	addr    string
	id      transport.ConnID
	handler transport.RecvHandler
	logger  *zap.Logger

	mu      sync.Mutex
	conn    *nhws.Conn
	writeCh chan []byte
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewCoderClient creates a client that will connect to ws://addr/ws.
func NewCoderClient(addr string, handler transport.RecvHandler, logger *zap.Logger) *CoderClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &CoderClient{
		addr:    addr,
		id:      transport.ConnID(uuid.NewString()),
		handler: handler,
		logger:  logger,
		writeCh: make(chan []byte, coderWriteChanCap),
	}
}

// Connect dials the server and starts background pumps.
// Blocks until the first connection attempt succeeds or all retries are exhausted.
func (c *CoderClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	conn, err := c.dialWithRetry(c.ctx, coderMaxReconnectAttempts)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	go c.writePump(c.ctx)
	go c.readPump(c.ctx)
	return nil
}

// dialWithRetry dials with exponential backoff.
func (c *CoderClient) dialWithRetry(ctx context.Context, maxAttempts int) (*nhws.Conn, error) {
	url := "ws://" + c.addr + "/ws"
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := coderReconnectDelay(attempt)
			c.logger.Info("coder client reconnecting",
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		start := time.Now()
		conn, _, err := nhws.Dial(ctx, url, &nhws.DialOptions{
			CompressionMode: nhws.CompressionDisabled,
		})
		if err == nil {
			c.logger.Info("coder client connected",
				zap.String("addr", c.addr),
				zap.Duration("handshake", time.Since(start)))
			return conn, nil
		}
		lastErr = err
		c.logger.Warn("coder client dial failed",
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}
	return nil, lastErr
}

// coderReconnectDelay computes exponential backoff with full jitter.
func coderReconnectDelay(attempt int) time.Duration {
	cap := float64(coderReconnectMaxDelay)
	base := float64(coderReconnectBaseDelay)
	exp := math.Min(cap, base*math.Pow(2, float64(attempt)))
	return time.Duration(rand.Float64() * exp)
}

// readPump reads inbound messages and dispatches to the handler.
// On error it reconnects.
func (c *CoderClient) readPump(ctx context.Context) {
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn == nil {
			return
		}
		conn.SetReadLimit(1 << 20)

		for {
			readCtx, readCancel := context.WithTimeout(ctx, coderReadTimeout)
			msgType, data, err := conn.Read(readCtx)
			recvAt := time.Now()
			readCancel()
			if err != nil {
				c.logger.Debug("coder client read error", zap.Error(err))
				break
			}
			if msgType != nhws.MessageBinary {
				continue
			}
			if c.handler != nil {
				c.handler(c.id, data, recvAt)
			}
		}

		// Connection lost — try to reconnect.
		select {
		case <-ctx.Done():
			return
		default:
		}
		reconn := time.Now()
		newConn, err := c.dialWithRetry(ctx, coderMaxReconnectAttempts)
		if err != nil {
			c.logger.Error("coder client reconnect exhausted", zap.Error(err))
			return
		}
		c.logger.Info("coder client reconnected", zap.Duration("took", time.Since(reconn)))
		c.mu.Lock()
		c.conn = newConn
		c.mu.Unlock()
	}
}

// writePump drains writeCh and sends binary frames.
func (c *CoderClient) writePump(ctx context.Context) {
	ticker := time.NewTicker(coderPingInterval)
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
			writeCtx, writeCancel := context.WithTimeout(ctx, coderWriteTimeout)
			err := conn.Write(writeCtx, nhws.MessageBinary, data)
			writeCancel()
			if err != nil {
				c.logger.Debug("coder client write error", zap.Error(err))
			}
		case <-ticker.C:
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()
			if conn == nil {
				continue
			}
			pingCtx, pingCancel := context.WithTimeout(ctx, coderWriteTimeout)
			_ = conn.Ping(pingCtx)
			pingCancel()
		}
	}
}

// Send enqueues data for sending. Non-blocking; returns error if channel is full.
func (c *CoderClient) Send(data []byte) error {
	select {
	case c.writeCh <- data:
		return nil
	default:
		return transport.ErrBroadcastFailed
	}
}

// Close gracefully shuts down the client.
func (c *CoderClient) Close() {
	c.mu.Lock()
	cancel := c.cancel
	conn := c.conn
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close(nhws.StatusNormalClosure, "client closing")
	}
}
