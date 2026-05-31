// Package http2 implements a server-push streaming transport over HTTP/2 (h2c).
//
// Protocol:
//   - POST /register  → assigns a ConnID, returned as a plain-text body.
//   - GET  /stream?id=<connID> → opens a long-lived HTTP/2 response stream.
//     Messages are framed with a 4-byte big-endian length prefix (same as http1).
//
// HTTP/2 specifics:
//   - Uses unencrypted HTTP/2 (h2c) via the Go 1.25 net/http Protocols field.
//     This allows the benchmark to measure protocol overhead without TLS cost.
//   - Multiple subscribers can share one TCP connection through HTTP/2 stream
//     multiplexing — each GET /stream creates a new HTTP/2 stream.
//   - ResponseWriter.Flush() causes the HTTP/2 framer to emit a DATA frame
//     immediately; without it, data may be buffered until the write buffer fills.
//
// Shutdown:
//   - http.Server.Shutdown() sends GOAWAY to all clients and drains active requests.
//   - Each stream handler detects context cancellation (from the server shutdown or
//     explicit per-connection cancel) and exits.
package http2

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
	"go.uber.org/zap"
)

const (
	http2StreamChanCap = 256
)

// streamEntry holds the per-subscriber write channel for a streaming HTTP/2 request.
type streamEntry struct {
	ch     chan []byte
	id     transport.ConnID
	cancel context.CancelFunc
}

// HTTP2Server is an HTTP/2 (h2c) streaming server.
// It satisfies transport.Transport.
type HTTP2Server struct {
	transport.BaseTransport

	addr    string
	httpSrv *http.Server
	streams sync.Map // ConnID → *streamEntry
	logger  *zap.Logger

	cfg transport.TransportConfig

	// counters
	seqCounter atomic.Uint64
	sent       atomic.Uint64
	recv       atomic.Uint64
	bytesSent  atomic.Uint64
	bytesRecv  atomic.Uint64
	connCount  atomic.Int64
}

// NewHTTP2Server creates an HTTP2Server ready to be started.
func NewHTTP2Server(cfg transport.TransportConfig, logger *zap.Logger) *HTTP2Server {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &HTTP2Server{
		addr:   cfg.ListenAddr,
		cfg:    cfg,
		logger: logger,
	}
	s.SetProtocol("http/2")
	return s
}

// Start implements transport.Transport.
// It starts an h2c (unencrypted HTTP/2) server using the Go 1.25 Protocols API.
func (s *HTTP2Server) Start() error {
	if s.IsStarted() {
		return transport.ErrAlreadyStarted
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/stream", s.handleStream)

	// Use the Go 1.25 native h2c support: set Protocols to accept both
	// HTTP/1.1 (for the initial handshake / health checks) and unencrypted HTTP/2.
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)

	s.httpSrv = &http.Server{
		Addr:      s.addr,
		Handler:   mux,
		Protocols: protos,
	}

	s.MarkStarted()
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http2 server error", zap.Error(err))
		}
	}()
	s.logger.Info("http2 server started (h2c)", zap.String("addr", s.addr))
	return nil
}

// Stop implements transport.Transport.
func (s *HTTP2Server) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	// Cancel all active stream handlers first so they can clean up gracefully
	// before Shutdown() closes the underlying connections.
	s.streams.Range(func(_, v any) bool {
		if e, ok := v.(*streamEntry); ok {
			e.cancel()
		}
		return true
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.httpSrv.Shutdown(ctx)
	s.MarkStopped()
	s.logger.Info("http2 server stopped")
	return err
}

// handleRegister assigns a ConnID to the caller and returns it as a text body.
// POST /register
func (s *HTTP2Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := transport.ConnID(uuid.New().String())
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, string(id))
}

// handleStream is the long-lived HTTP/2 streaming endpoint.
// GET /stream?id=<connID>
func (s *HTTP2Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := transport.ConnID(r.URL.Query().Get("id"))
	if id == "" {
		http.Error(w, "missing id parameter", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	entry := &streamEntry{
		ch:     make(chan []byte, http2StreamChanCap),
		id:     id,
		cancel: cancel,
	}
	s.streams.Store(id, entry)
	s.connCount.Add(1)
	if s.cfg.OnConnect != nil {
		s.cfg.OnConnect(id)
	}

	// Write response headers and flush to establish the stream.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	defer func() {
		cancel()
		s.streams.Delete(id)
		s.connCount.Add(-1)
		if s.cfg.OnDisconnect != nil {
			s.cfg.OnDisconnect(id, nil)
		}
	}()

	var lenBuf [4]byte

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-entry.ch:
			if !ok {
				return
			}
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
			if _, err := w.Write(lenBuf[:]); err != nil {
				s.logger.Debug("http2 write len error",
					zap.String("id", string(id)), zap.Error(err))
				return
			}
			if _, err := w.Write(data); err != nil {
				s.logger.Debug("http2 write data error",
					zap.String("id", string(id)), zap.Error(err))
				return
			}
			flusher.Flush()
			s.sent.Add(1)
			s.bytesSent.Add(4 + uint64(len(data)))
		}
	}
}

// Broadcast implements transport.Transport.
func (s *HTTP2Server) Broadcast(data []byte) error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	frame := wire.Encode(s.seqCounter.Add(1), time.Now().UnixNano(), data)
	s.streams.Range(func(_, v any) bool {
		if e, ok := v.(*streamEntry); ok {
			select {
			case e.ch <- frame:
			default:
				s.logger.Debug("http2 stream channel full, dropping message",
					zap.String("id", string(e.id)))
			}
		}
		return true
	})
	return nil
}

// Send implements transport.Transport.
func (s *HTTP2Server) Send(id transport.ConnID, data []byte) error {
	v, ok := s.streams.Load(id)
	if !ok {
		return transport.ErrClientNotFound
	}
	e := v.(*streamEntry)
	select {
	case e.ch <- data:
		return nil
	default:
		return transport.ErrBroadcastFailed
	}
}

// Connections implements transport.Transport.
func (s *HTTP2Server) Connections() int {
	return int(s.connCount.Load())
}

// Stats implements transport.Transport.
func (s *HTTP2Server) Stats() transport.Stats {
	st := s.BaseStats()
	st.Connections = s.Connections()
	st.Sent = s.sent.Load()
	st.Received = s.recv.Load()
	st.BytesSent = s.bytesSent.Load()
	st.BytesRecv = s.bytesRecv.Load()
	return st
}

// handleInbound accepts client-to-server messages (optional uplink path).
func (s *HTTP2Server) handleInbound(w http.ResponseWriter, r *http.Request) {
	id := transport.ConnID(r.Header.Get("X-Conn-ID"))
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	recvAt := time.Now()
	s.recv.Add(1)
	s.bytesRecv.Add(uint64(len(data)))
	if s.cfg.OnRecv != nil {
		s.cfg.OnRecv(id, data, recvAt)
	}
	w.WriteHeader(http.StatusNoContent)
}
