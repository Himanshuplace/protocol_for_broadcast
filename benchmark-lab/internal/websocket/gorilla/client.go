package gorilla

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

const (
	maxReconnectAttempts = 10
	reconnectBaseDelay   = 100 * time.Millisecond
	reconnectMaxDelay    = 10 * time.Second
)

// GorillaClient is a WebSocket client backed by gorilla/websocket.
// It reconnects automatically with exponential backoff on connection loss.
type GorillaClient struct {
	addr    string
	handler transport.RecvHandler
	logger  *zap.Logger

	mu      sync.Mutex
	conn    *websocket.Conn
	writeCh chan []byte
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewGorillaClient creates a client that will connect to ws://addr/ws.
func NewGorillaClient(addr string, handler transport.RecvHandler, logger *zap.Logger) *GorillaClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &GorillaClient{
		addr:    addr,
		handler: handler,
		logger:  logger,
		writeCh: make(chan []byte, writeChanCap),
	}
}

// Connect dials the server and starts background pumps.
// It blocks until the first connection attempt succeeds or all retries are exhausted.
func (c *GorillaClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	conn, err := c.dialWithRetry(c.ctx, maxReconnectAttempts)
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

// dialWithRetry attempts to dial with exponential backoff.
func (c *GorillaClient) dialWithRetry(ctx context.Context, maxAttempts int) (*websocket.Conn, error) {
	url := "ws://" + c.addr + "/ws"
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := reconnectDelay(attempt)
			c.logger.Info("gorilla client reconnecting",
				zap.Int("attempt", attempt),
				zap.Duration("delay", delay))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		start := time.Now()
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
		if err == nil {
			elapsed := time.Since(start)
			c.logger.Info("gorilla client connected",
				zap.String("addr", c.addr),
				zap.Duration("handshake", elapsed))
			return conn, nil
		}
		lastErr = err
		c.logger.Warn("gorilla client dial failed",
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}
	return nil, lastErr
}

// reconnectDelay computes the delay for the given attempt number using
// exponential backoff with full jitter: delay = rand(0, min(cap, base * 2^attempt)).
func reconnectDelay(attempt int) time.Duration {
	cap := float64(reconnectMaxDelay)
	base := float64(reconnectBaseDelay)
	exp := math.Min(cap, base*math.Pow(2, float64(attempt)))
	jittered := rand.Float64() * exp
	return time.Duration(jittered)
}

// readPump reads inbound messages and dispatches them to the handler.
// On error it attempts to reconnect.
func (c *GorillaClient) readPump(ctx context.Context) {
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn == nil {
			return
		}

		conn.SetReadLimit(maxMessageSize)
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(pongWait))
		})

		for {
			msgType, data, err := conn.ReadMessage()
			recvAt := time.Now()
			if err != nil {
				c.logger.Debug("gorilla client read error", zap.Error(err))
				break
			}
			if msgType != websocket.BinaryMessage {
				continue
			}
			if c.handler != nil {
				c.handler("client", data, recvAt)
			}
		}

		// Connection lost — attempt reconnect.
		select {
		case <-ctx.Done():
			return
		default:
		}
		reconnStart := time.Now()
		newConn, err := c.dialWithRetry(ctx, maxReconnectAttempts)
		if err != nil {
			c.logger.Error("gorilla client reconnect exhausted", zap.Error(err))
			return
		}
		reconnDur := time.Since(reconnStart)
		c.logger.Info("gorilla client reconnected", zap.Duration("took", reconnDur))
		c.mu.Lock()
		c.conn = newConn
		c.mu.Unlock()
	}
}

// writePump drains writeCh and sends frames to the WebSocket.
func (c *GorillaClient) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
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
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				c.logger.Debug("gorilla client write error", zap.Error(err))
			}
		case <-ticker.C:
			c.mu.Lock()
			conn := c.conn
			c.mu.Unlock()
			if conn == nil {
				continue
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			_ = conn.WriteMessage(websocket.PingMessage, nil)
		}
	}
}

// Send enqueues data for sending. Non-blocking; returns error if channel is full.
func (c *GorillaClient) Send(data []byte) error {
	select {
	case c.writeCh <- data:
		return nil
	default:
		return transport.ErrBroadcastFailed
	}
}

// Close gracefully closes the client connection.
func (c *GorillaClient) Close() {
	c.mu.Lock()
	cancel := c.cancel
	conn := c.conn
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}
}
