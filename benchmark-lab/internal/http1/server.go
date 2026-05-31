// Package http1 implements a long-poll/chunked-streaming server transport over HTTP/1.1.
//
// Protocol:
//   - POST /register  → assigns a ConnID, returns it as a plain-text body.
//   - GET  /stream?id=<connID> → keeps the HTTP/1.1 connection open and writes
//     framed messages as chunked body data.  Each message is prefixed with a
//     4-byte big-endian length so the client can delimit messages inside the
//     chunked stream.
//
// Framing:
//
//	[4 bytes big-endian uint32 length][length bytes of wire-encoded payload]
//
// The outer chunked-transfer encoding is handled automatically by net/http when
// the handler calls Flusher.Flush() without setting a Content-Length.  The 4-byte
// prefix is an application-level delimiter needed because chunk boundaries are an
// implementation detail of the HTTP layer and are NOT reliable message boundaries.
//
// Shutdown:
//   - HTTP server Shutdown() closes all idle connections.
//   - Each active /stream handler detects context cancellation and exits cleanly,
//     removing itself from the streams map.
package http1

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
	http1StreamChanCap = 256
	http1WriteTimeout  = 10 * time.Second
)

// streamEntry holds the per-connection write channel for a streaming HTTP/1.1 client.
type streamEntry struct {
	ch     chan []byte
	id     transport.ConnID
	cancel context.CancelFunc
}

// HTTP1Server is an HTTP/1.1 chunked-streaming server.
// It satisfies transport.Transport.
type HTTP1Server struct {
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

// NewHTTP1Server creates an HTTP1Server ready to be started.
func NewHTTP1Server(cfg transport.TransportConfig, logger *zap.Logger) *HTTP1Server {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &HTTP1Server{
		addr:   cfg.ListenAddr,
		cfg:    cfg,
		logger: logger,
	}
	s.SetProtocol("http/1.1")
	return s
}

// Start implements transport.Transport.
func (s *HTTP1Server) Start() error {
	if s.IsStarted() {
		return transport.ErrAlreadyStarted
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/stream", s.handleStream)

	s.httpSrv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	s.MarkStarted()
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http1 server error", zap.Error(err))
		}
	}()
	s.logger.Info("http1 server started", zap.String("addr", s.addr))
	return nil
}

// Stop implements transport.Transport.
func (s *HTTP1Server) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Cancel all active stream handlers.
	s.streams.Range(func(_, v any) bool {
		if e, ok := v.(*streamEntry); ok {
			e.cancel()
		}
		return true
	})
	err := s.httpSrv.Shutdown(ctx)
	s.MarkStopped()
	s.logger.Info("http1 server stopped")
	return err
}

// handleRegister assigns a ConnID to the connecting client and returns it.
// POST /register
func (s *HTTP1Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := transport.ConnID(uuid.New().String())
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, string(id))
}

// handleStream starts a long-lived chunked HTTP/1.1 stream for the client.
// GET /stream?id=<connID>
func (s *HTTP1Server) handleStream(w http.ResponseWriter, r *http.Request) {
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
		ch:     make(chan []byte, http1StreamChanCap),
		id:     id,
		cancel: cancel,
	}
	s.streams.Store(id, entry)
	s.connCount.Add(1)
	if s.cfg.OnConnect != nil {
		s.cfg.OnConnect(id)
	}

	// Signal to the client that the stream is open and ready.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
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

	// 4-byte length prefix buffer, reused across messages.
	var lenBuf [4]byte

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-entry.ch:
			if !ok {
				return
			}
			// Write 4-byte big-endian length prefix.
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
			if _, err := w.Write(lenBuf[:]); err != nil {
				s.logger.Debug("http1 write len error",
					zap.String("id", string(id)), zap.Error(err))
				return
			}
			if _, err := w.Write(data); err != nil {
				s.logger.Debug("http1 write data error",
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
func (s *HTTP1Server) Broadcast(data []byte) error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	frame := wire.Encode(s.seqCounter.Add(1), time.Now().UnixNano(), data)
	s.streams.Range(func(_, v any) bool {
		if e, ok := v.(*streamEntry); ok {
			select {
			case e.ch <- frame:
			default:
				s.logger.Debug("http1 stream channel full, dropping message",
					zap.String("id", string(e.id)))
			}
		}
		return true
	})
	return nil
}

// Send implements transport.Transport.
func (s *HTTP1Server) Send(id transport.ConnID, data []byte) error {
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
func (s *HTTP1Server) Connections() int {
	return int(s.connCount.Load())
}

// Stats implements transport.Transport.
func (s *HTTP1Server) Stats() transport.Stats {
	st := s.BaseStats()
	st.Connections = s.Connections()
	st.Sent = s.sent.Load()
	st.Received = s.recv.Load()
	st.BytesSent = s.bytesSent.Load()
	st.BytesRecv = s.bytesRecv.Load()
	return st
}

// handleInbound is called by the client POST /inbound endpoint (optional, for
// server-to-server benchmarks that also measure uplink).  Not wired by default.
func (s *HTTP1Server) handleInbound(w http.ResponseWriter, r *http.Request) {
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
