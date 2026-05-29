// Package gobwas implements a WebSocket transport using github.com/gobwas/ws.
//
// Architecture:
//   - gobwas/ws operates on raw net.Conn, giving zero-allocation frame I/O.
//   - Each accepted connection runs a single goroutine that handles both reads
//     and writes.  Writes are serialised with a per-connection mutex so that
//     the broadcast path (many goroutines) and the keepalive path (a single
//     ticker goroutine) never race on the underlying net.Conn.
//   - Broadcast snapshots the registry before iterating so the registry lock
//     is released before any network I/O begins.
//   - Ping frames are sent every 30 s; the read loop handles incoming pong
//     frames to keep the TCP connection alive through NAT / load-balancers.
package gobwas

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/google/uuid"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

const (
	gobwasPingInterval  = 30 * time.Second
	gobwasWriteTimeout  = 10 * time.Second
	gobwasReadTimeout   = 60 * time.Second
	gobwasWriteChanCap  = 256
)

// gobwasConn holds the state for one connected client.
type gobwasConn struct {
	conn    net.Conn
	br      *bufio.ReadWriter // buffered I/O returned by UpgradeHTTP
	writeCh chan []byte
	id      transport.ConnID
	mu      sync.Mutex // serialises concurrent writes on the raw net.Conn
	cancel  context.CancelFunc
}

// GobwasServer is a WebSocket server backed by gobwas/ws.
// It satisfies transport.Transport.
type GobwasServer struct {
	transport.BaseTransport

	addr     string
	registry transport.Registry[*gobwasConn]
	httpSrv  *http.Server
	logger   *zap.Logger

	cfg transport.TransportConfig

	// counters
	sent      atomic.Uint64
	recv      atomic.Uint64
	bytesSent atomic.Uint64
	bytesRecv atomic.Uint64
}

// NewGobwasServer creates a GobwasServer ready to be started.
func NewGobwasServer(cfg transport.TransportConfig, logger *zap.Logger) *GobwasServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &GobwasServer{
		addr:   cfg.ListenAddr,
		cfg:    cfg,
		logger: logger,
	}
	s.SetProtocol("websocket/gobwas")
	return s
}

// Start implements transport.Transport.
func (s *GobwasServer) Start() error {
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
			s.logger.Error("gobwas http server error", zap.Error(err))
		}
	}()
	s.logger.Info("gobwas websocket server started", zap.String("addr", s.addr))
	return nil
}

// Stop implements transport.Transport.
func (s *GobwasServer) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(ctx)
	s.MarkStopped()
	s.logger.Info("gobwas websocket server stopped")
	return err
}

// serveWS upgrades an HTTP connection to WebSocket using gobwas zero-alloc upgrade.
func (s *GobwasServer) serveWS(w http.ResponseWriter, r *http.Request) {
	// UpgradeHTTP hijacks the connection; after this call w/r are unusable.
	conn, brw, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		s.logger.Warn("gobwas upgrade failed", zap.Error(err))
		return
	}

	id := transport.ConnID(uuid.New().String())
	ctx, cancel := context.WithCancel(context.Background())

	gc := &gobwasConn{
		conn:    conn,
		br:      brw,
		writeCh: make(chan []byte, gobwasWriteChanCap),
		id:      id,
		cancel:  cancel,
	}

	s.registry.Add(id, gc)
	if s.cfg.OnConnect != nil {
		s.cfg.OnConnect(id)
	}

	// writePump runs in a background goroutine; readPump runs in this goroutine.
	go s.writePump(gc, ctx)
	s.readPump(gc, ctx)

	// Cleanup after readPump returns.
	cancel()
	s.registry.Remove(id)
	conn.Close()
	if s.cfg.OnDisconnect != nil {
		s.cfg.OnDisconnect(id, nil)
	}
}

// readPump reads WebSocket frames from the connection.
// It exits when the connection is closed or an error occurs.
func (s *GobwasServer) readPump(gc *gobwasConn, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = gc.conn.SetReadDeadline(time.Now().Add(gobwasReadTimeout))
		frame, err := ws.ReadFrame(gc.br)
		recvAt := time.Now()
		if err != nil {
			s.logger.Debug("gobwas read error",
				zap.String("id", string(gc.id)), zap.Error(err))
			return
		}

		// Handle control frames inline.
		switch frame.Header.OpCode {
		case ws.OpClose:
			// Echo close frame back then exit.
			gc.mu.Lock()
			_ = gc.conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
			_ = ws.WriteFrame(gc.conn, ws.NewCloseFrame(nil))
			gc.mu.Unlock()
			return
		case ws.OpPing:
			gc.mu.Lock()
			_ = gc.conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
			_ = ws.WriteFrame(gc.conn, ws.NewPongFrame(frame.Payload))
			gc.mu.Unlock()
			continue
		case ws.OpPong:
			// Keepalive pong — update read deadline (already done above).
			continue
		case ws.OpBinary, ws.OpText:
			// Data frame — fall through to dispatch.
		default:
			continue
		}

		// Client frames are masked; unmask before processing.
		if frame.Header.Masked {
			ws.Cipher(frame.Payload, frame.Header.Mask, 0)
		}

		s.recv.Add(1)
		s.bytesRecv.Add(uint64(len(frame.Payload)))
		if s.cfg.OnRecv != nil {
			// Copy payload: frame.Payload is a transient buffer.
			data := make([]byte, len(frame.Payload))
			copy(data, frame.Payload)
			s.cfg.OnRecv(gc.id, data, recvAt)
		}
	}
}

// writePump drains writeCh and sends binary frames.
// It also sends periodic pings for keepalive.
func (s *GobwasServer) writePump(gc *gobwasConn, ctx context.Context) {
	ticker := time.NewTicker(gobwasPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-gc.writeCh:
			if !ok {
				return
			}
			gc.mu.Lock()
			_ = gc.conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
			err := ws.WriteFrame(gc.conn, ws.NewBinaryFrame(data))
			gc.mu.Unlock()
			if err != nil {
				s.logger.Debug("gobwas write error",
					zap.String("id", string(gc.id)), zap.Error(err))
				gc.cancel()
				return
			}
			s.sent.Add(1)
			s.bytesSent.Add(uint64(len(data)))
		case <-ticker.C:
			gc.mu.Lock()
			_ = gc.conn.SetWriteDeadline(time.Now().Add(gobwasWriteTimeout))
			err := ws.WriteFrame(gc.conn, ws.NewPingFrame(nil))
			gc.mu.Unlock()
			if err != nil {
				gc.cancel()
				return
			}
		}
	}
}

// Broadcast implements transport.Transport.
func (s *GobwasServer) Broadcast(data []byte) error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	conns := s.registry.Snapshot()
	for _, gc := range conns {
		select {
		case gc.writeCh <- data:
		default:
			s.logger.Debug("gobwas write channel full, dropping message",
				zap.String("id", string(gc.id)))
		}
	}
	return nil
}

// Send implements transport.Transport.
func (s *GobwasServer) Send(id transport.ConnID, data []byte) error {
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
func (s *GobwasServer) Connections() int {
	return s.registry.Len()
}

// Stats implements transport.Transport.
func (s *GobwasServer) Stats() transport.Stats {
	st := s.BaseStats()
	st.Connections = s.registry.Len()
	st.Sent = s.sent.Load()
	st.Received = s.recv.Load()
	st.BytesSent = s.bytesSent.Load()
	st.BytesRecv = s.bytesRecv.Load()
	return st
}
