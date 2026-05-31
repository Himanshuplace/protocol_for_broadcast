// Package coder implements a WebSocket transport using github.com/coder/websocket.
//
// Architecture:
//   - coder/websocket is context-aware: every read and write accepts a context,
//     so clean shutdown is driven entirely by context cancellation rather than
//     explicit close calls.
//   - Each connection spawns a readPump (this goroutine) and a writePump
//     (background goroutine).  The writePump drains a per-connection buffered
//     channel of capacity 256 so that Broadcast never blocks on slow clients.
//   - Keepalive is handled by sending Ping frames every 30 s from the
//     writePump; coder/websocket automatically sends Pong replies on the
//     client's behalf, so no explicit pong handler is needed.
//   - Compression is disabled to remove per-message allocation from the hot path.
package coder

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	nhws "github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
	"go.uber.org/zap"
)

const (
	coderWriteChanCap = 256
	coderPingInterval = 30 * time.Second
	coderWriteTimeout = 10 * time.Second
	coderReadTimeout  = 60 * time.Second
)

// coderConn holds per-connection state.
type coderConn struct {
	conn    *nhws.Conn
	writeCh chan []byte
	id      transport.ConnID
	cancel  context.CancelFunc
}

// CoderServer is a WebSocket server backed by coder/websocket.
// It satisfies transport.Transport.
type CoderServer struct {
	transport.BaseTransport

	addr     string
	registry transport.Registry[*coderConn]
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

// NewCoderServer creates a CoderServer ready to be started.
func NewCoderServer(cfg transport.TransportConfig, logger *zap.Logger) *CoderServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &CoderServer{
		addr:   cfg.ListenAddr,
		cfg:    cfg,
		logger: logger,
	}
	s.SetProtocol("websocket/coder")
	return s
}

// Start implements transport.Transport.
func (s *CoderServer) Start() error {
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
			s.logger.Error("coder http server error", zap.Error(err))
		}
	}()
	s.logger.Info("coder websocket server started", zap.String("addr", s.addr))
	return nil
}

// Stop implements transport.Transport.
func (s *CoderServer) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(ctx)
	s.MarkStopped()
	s.logger.Info("coder websocket server stopped")
	return err
}

// serveWS accepts a WebSocket upgrade and handles the connection lifecycle.
func (s *CoderServer) serveWS(w http.ResponseWriter, r *http.Request) {
	wsConn, err := nhws.Accept(w, r, &nhws.AcceptOptions{
		InsecureSkipVerify: true, // allow cross-origin; benchmarks run same host
		CompressionMode:    nhws.CompressionDisabled,
	})
	if err != nil {
		s.logger.Warn("coder accept failed", zap.Error(err))
		return
	}

	id := transport.ConnID(uuid.New().String())
	ctx, cancel := context.WithCancel(context.Background())

	cc := &coderConn{
		conn:    wsConn,
		writeCh: make(chan []byte, coderWriteChanCap),
		id:      id,
		cancel:  cancel,
	}

	// coder/websocket default read limit is 32768; raise it for benchmarks.
	wsConn.SetReadLimit(1 << 20)

	s.registry.Add(id, cc)
	if s.cfg.OnConnect != nil {
		s.cfg.OnConnect(id)
	}

	go s.writePump(cc, ctx)
	s.readPump(cc, ctx)

	cancel()
	s.registry.Remove(id)
	// Close with normal closure; ignore error (connection may already be gone).
	_ = wsConn.Close(nhws.StatusNormalClosure, "server shutdown")
	if s.cfg.OnDisconnect != nil {
		s.cfg.OnDisconnect(id, nil)
	}
}

// readPump reads inbound messages and dispatches them to the handler.
func (s *CoderServer) readPump(cc *coderConn, ctx context.Context) {
	for {
		readCtx, readCancel := context.WithTimeout(ctx, coderReadTimeout)
		msgType, data, err := cc.conn.Read(readCtx)
		recvAt := time.Now()
		readCancel()
		if err != nil {
			// Check if the parent context was cancelled (clean shutdown).
			select {
			case <-ctx.Done():
			default:
				s.logger.Debug("coder read error",
					zap.String("id", string(cc.id)), zap.Error(err))
			}
			return
		}
		if msgType != nhws.MessageBinary {
			continue
		}
		s.recv.Add(1)
		s.bytesRecv.Add(uint64(len(data)))
		if s.cfg.OnRecv != nil {
			s.cfg.OnRecv(cc.id, data, recvAt)
		}
	}
}

// writePump drains the writeCh and sends binary frames.
// It also sends periodic pings to keep the connection alive.
func (s *CoderServer) writePump(cc *coderConn, ctx context.Context) {
	ticker := time.NewTicker(coderPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-cc.writeCh:
			if !ok {
				return
			}
			writeCtx, writeCancel := context.WithTimeout(ctx, coderWriteTimeout)
			err := cc.conn.Write(writeCtx, nhws.MessageBinary, data)
			writeCancel()
			if err != nil {
				s.logger.Debug("coder write error",
					zap.String("id", string(cc.id)), zap.Error(err))
				cc.cancel()
				return
			}
			s.sent.Add(1)
			s.bytesSent.Add(uint64(len(data)))
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, coderWriteTimeout)
			err := cc.conn.Ping(pingCtx)
			pingCancel()
			if err != nil {
				cc.cancel()
				return
			}
		}
	}
}

// Broadcast implements transport.Transport.
func (s *CoderServer) Broadcast(data []byte) error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	frame := wire.Encode(s.seqCounter.Add(1), time.Now().UnixNano(), data)
	conns := s.registry.Snapshot()
	for _, cc := range conns {
		select {
		case cc.writeCh <- frame:
		default:
			s.logger.Debug("coder write channel full, dropping message",
				zap.String("id", string(cc.id)))
		}
	}
	return nil
}

// Send implements transport.Transport.
func (s *CoderServer) Send(id transport.ConnID, data []byte) error {
	cc, ok := s.registry.Get(id)
	if !ok {
		return transport.ErrClientNotFound
	}
	select {
	case cc.writeCh <- data:
		return nil
	default:
		return transport.ErrBroadcastFailed
	}
}

// Connections implements transport.Transport.
func (s *CoderServer) Connections() int {
	return s.registry.Len()
}

// Stats implements transport.Transport.
func (s *CoderServer) Stats() transport.Stats {
	st := s.BaseStats()
	st.Connections = s.registry.Len()
	st.Sent = s.sent.Load()
	st.Received = s.recv.Load()
	st.BytesSent = s.bytesSent.Load()
	st.BytesRecv = s.bytesRecv.Load()
	return st
}
