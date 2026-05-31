// Package gorilla implements a WebSocket transport using github.com/gorilla/websocket.
//
// Architecture:
//   - Each connection has a dedicated readPump and writePump goroutine.
//   - writePump drains a buffered channel (cap 256) so Broadcast never blocks on slow clients.
//   - Ping/pong keepalive fires every 30 seconds to detect stale connections early.
//   - Broadcast takes a snapshot of the registry before iterating so the registry lock
//     is held only during the snapshot, not during potentially-slow channel sends.
package gorilla

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
	"go.uber.org/zap"

	"github.com/google/uuid"
)

const (
	writeChanCap  = 256
	pingInterval  = 30 * time.Second
	pongWait      = 60 * time.Second
	writeWait     = 10 * time.Second
	maxMessageSize = 1 << 20 // 1 MB
)

// gorillaConn wraps a single gorilla WebSocket connection.
type gorillaConn struct {
	conn    *websocket.Conn
	writeCh chan []byte
	id      transport.ConnID
	cancel  context.CancelFunc
}

// GorillaServer is a WebSocket server backed by gorilla/websocket.
// It satisfies transport.Transport.
type GorillaServer struct {
	transport.BaseTransport

	addr     string
	registry transport.Registry[*gorillaConn]
	upgrader websocket.Upgrader
	httpSrv  *http.Server
	logger   *zap.Logger

	cfg transport.TransportConfig

	// counters
	seqCounter atomic.Uint64
	sent       atomic.Uint64
	recv       atomic.Uint64
	bytesSent  atomic.Uint64
	bytesRecv  atomic.Uint64
}

// NewGorillaServer creates a GorillaServer ready to be started.
func NewGorillaServer(cfg transport.TransportConfig, logger *zap.Logger) *GorillaServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	readBuf := cfg.ReadBufSize
	if readBuf == 0 {
		readBuf = 65536
	}
	writeBuf := cfg.WriteBufSize
	if writeBuf == 0 {
		writeBuf = 65536
	}

	s := &GorillaServer{
		addr:   cfg.ListenAddr,
		cfg:    cfg,
		logger: logger,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  readBuf,
			WriteBufferSize: writeBuf,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
	s.SetProtocol("websocket/gorilla")
	return s
}

// Start implements transport.Transport.
func (s *GorillaServer) Start() error {
	if s.IsStarted() {
		return transport.ErrAlreadyStarted
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.serveWS)

	s.httpSrv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	s.MarkStarted()
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("gorilla http server error", zap.Error(err))
		}
	}()
	s.logger.Info("gorilla websocket server started", zap.String("addr", s.addr))
	return nil
}

// Stop implements transport.Transport.
func (s *GorillaServer) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(ctx)
	s.MarkStopped()
	s.logger.Info("gorilla websocket server stopped")
	return err
}

// serveWS upgrades an HTTP request to a WebSocket connection.
func (s *GorillaServer) serveWS(w http.ResponseWriter, r *http.Request) {
	wsConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("websocket upgrade failed", zap.Error(err))
		return
	}

	id := transport.ConnID(uuid.New().String())
	ctx, cancel := context.WithCancel(r.Context())

	gc := &gorillaConn{
		conn:    wsConn,
		writeCh: make(chan []byte, writeChanCap),
		id:      id,
		cancel:  cancel,
	}

	// Configure pong handler and read deadline.
	wsConn.SetReadLimit(maxMessageSize)
	_ = wsConn.SetReadDeadline(time.Now().Add(pongWait))
	wsConn.SetPongHandler(func(string) error {
		return wsConn.SetReadDeadline(time.Now().Add(pongWait))
	})

	s.registry.Add(id, gc)
	if s.cfg.OnConnect != nil {
		s.cfg.OnConnect(id)
	}

	go s.writePump(gc, ctx)
	s.readPump(gc, ctx) // blocks until conn closes

	// Cleanup after readPump returns.
	cancel()
	s.registry.Remove(id)
	wsConn.Close()
	if s.cfg.OnDisconnect != nil {
		s.cfg.OnDisconnect(id, nil)
	}
}

// readPump reads inbound messages from the WebSocket connection.
// It exits when the connection is closed or the context is cancelled.
func (s *GorillaServer) readPump(gc *gorillaConn, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgType, data, err := gc.conn.ReadMessage()
		recvAt := time.Now()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Debug("gorilla read error", zap.String("id", string(gc.id)), zap.Error(err))
			}
			return
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		s.recv.Add(1)
		s.bytesRecv.Add(uint64(len(data)))
		if s.cfg.OnRecv != nil {
			s.cfg.OnRecv(gc.id, data, recvAt)
		}
	}
}

// writePump drains the writeCh and sends frames to the WebSocket.
// It also sends periodic pings to keep the connection alive.
func (s *GorillaServer) writePump(gc *gorillaConn, ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-gc.writeCh:
			if !ok {
				return
			}
			_ = gc.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := gc.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				s.logger.Debug("gorilla write error", zap.String("id", string(gc.id)), zap.Error(err))
				gc.cancel()
				return
			}
			s.sent.Add(1)
			s.bytesSent.Add(uint64(len(data)))
		case <-ticker.C:
			_ = gc.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := gc.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				gc.cancel()
				return
			}
		}
	}
}

// Broadcast implements transport.Transport.
// It takes a snapshot to avoid holding registry locks during channel sends.
func (s *GorillaServer) Broadcast(data []byte) error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	frame := wire.Encode(s.seqCounter.Add(1), time.Now().UnixNano(), data)
	conns := s.registry.Snapshot()
	for _, gc := range conns {
		select {
		case gc.writeCh <- frame:
		default:
			// Slow consumer — drop the message rather than blocking the broadcast path.
			s.logger.Debug("gorilla write channel full, dropping message",
				zap.String("id", string(gc.id)))
		}
	}
	return nil
}

// Send implements transport.Transport.
func (s *GorillaServer) Send(id transport.ConnID, data []byte) error {
	gc, ok := s.registry.Get(id)
	if !ok {
		return transport.ErrClientNotFound
	}
	select {
	case gc.writeCh <- data:
		return nil
	default:
		return transport.ErrBroadcastFailed
	}
}

// Connections implements transport.Transport.
func (s *GorillaServer) Connections() int {
	return s.registry.Len()
}

// Stats implements transport.Transport.
func (s *GorillaServer) Stats() transport.Stats {
	st := s.BaseStats()
	st.Connections = s.registry.Len()
	st.Sent = s.sent.Load()
	st.Received = s.recv.Load()
	st.BytesSent = s.bytesSent.Load()
	st.BytesRecv = s.bytesRecv.Load()
	return st
}
