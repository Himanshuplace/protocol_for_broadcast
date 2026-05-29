// Package http3 provides an HTTP/3-over-QUIC transport implementation.
//
// The server supports three broadcast modes selected via the Mode field:
//   - "stream"       – HTTP GET long-poll with 4-byte length prefix per frame
//   - "unidirstream" – QUIC unidirectional streams per client
//   - "datagram"     – QUIC datagrams (unreliable, low-latency)
package http3

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"go.uber.org/zap"

	tlsinternal "github.com/himanshuplace/protocol_for_broadcast/internal/tls"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

const (
	h3WriteChanCap = 256
	h3LenBytes     = 4
)

// streamConn holds the per-subscriber channel and context for stream mode.
type streamConn struct {
	ch     chan []byte
	cancel context.CancelFunc
	id     transport.ConnID
}

// quicConn holds the QUIC connection for unidirstream / datagram modes.
type quicConn struct {
	conn   *quic.Conn
	stream *quic.SendStream // lazily opened for unidirstream
	mu     sync.Mutex
	id     transport.ConnID
	cancel context.CancelFunc
}

// HTTP3Server implements transport.Transport using HTTP/3 over QUIC.
type HTTP3Server struct {
	transport.BaseTransport

	addr    string
	Mode    string // "stream" | "unidirstream" | "datagram"
	h3srv   *http3.Server
	logger  *zap.Logger
	metrics *metrics.Recorder

	// stream mode: per-subscriber write channels keyed by ConnID
	streams sync.Map // ConnID -> *streamConn

	// unidirstream / datagram mode: QUIC connections
	quicConns sync.Map // ConnID -> *quicConn

	seqCounter  atomic.Uint64
	lostCounter atomic.Uint64
	connCounter atomic.Uint64

	done   chan struct{}
	wg     sync.WaitGroup
	onRecv transport.RecvHandler

	mux *http.ServeMux
}

// ServerOption is a functional option for HTTP3Server.
type ServerOption func(*HTTP3Server)

// WithLogger attaches a zap logger.
func WithLogger(l *zap.Logger) ServerOption {
	return func(s *HTTP3Server) { s.logger = l }
}

// WithRecorder attaches a metrics recorder.
func WithRecorder(r *metrics.Recorder) ServerOption {
	return func(s *HTTP3Server) { s.metrics = r }
}

// WithRecvHandler sets the inbound frame callback.
func WithRecvHandler(h transport.RecvHandler) ServerOption {
	return func(s *HTTP3Server) { s.onRecv = h }
}

// WithMode sets the broadcast mode ("stream", "unidirstream", "datagram").
func WithMode(m string) ServerOption {
	return func(s *HTTP3Server) { s.Mode = m }
}

// NewHTTP3Server creates a new HTTP3Server listening on addr.
func NewHTTP3Server(addr string, opts ...ServerOption) *HTTP3Server {
	s := &HTTP3Server{
		addr: addr,
		Mode: "stream",
		done: make(chan struct{}),
	}
	s.SetProtocol("http3")
	for _, o := range opts {
		o(s)
	}
	if s.logger == nil {
		s.logger = zap.NewNop()
	}
	if s.metrics == nil {
		s.metrics = metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "http3/server",
			Protocol: "http3",
			Scenario: "default",
		})
	}
	return s
}

// Start initializes TLS, registers HTTP handlers, and starts the HTTP/3 server.
func (s *HTTP3Server) Start() error {
	if s.IsStarted() {
		return transport.ErrAlreadyStarted
	}

	cert, err := tlsinternal.GenerateSelfSigned()
	if err != nil {
		return fmt.Errorf("http3: generate TLS cert: %w", err)
	}
	tlsCfg := tlsinternal.ServerTLSConfig(cert, http3.NextProtoH3)

	quicCfg := &quic.Config{
		MaxIncomingStreams: 10000,
		Allow0RTT:         true,
		EnableDatagrams:   true,
	}

	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/stream", s.handleStream)

	s.h3srv = &http3.Server{
		Addr:            s.addr,
		TLSConfig:       tlsCfg,
		QUICConfig:      quicCfg,
		Handler:         s.mux,
		EnableDatagrams: true,
	}

	s.done = make(chan struct{})
	s.MarkStarted()
	s.metrics.Start()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.h3srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			select {
			case <-s.done:
			default:
				s.logger.Error("http3: server error", zap.Error(err))
			}
		}
	}()

	s.logger.Info("http3 server started",
		zap.String("addr", s.addr),
		zap.String("mode", s.Mode))
	return nil
}

// Stop gracefully shuts down the server.
func (s *HTTP3Server) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	close(s.done)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.h3srv.Shutdown(ctx)

	// close all stream connections
	s.streams.Range(func(k, v any) bool {
		sc := v.(*streamConn)
		sc.cancel()
		return true
	})

	s.wg.Wait()
	s.metrics.Stop()
	s.MarkStopped()
	s.logger.Info("http3 server stopped")
	return nil
}

// Broadcast sends data to all connected clients according to the Mode.
func (s *HTTP3Server) Broadcast(data []byte) error {
	seq := s.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	size := len(frame)

	switch s.Mode {
	case "stream":
		framed := prependLen(frame)
		s.streams.Range(func(k, v any) bool {
			sc := v.(*streamConn)
			select {
			case sc.ch <- framed:
				s.metrics.RecordSend(seq, size)
			default:
				s.lostCounter.Add(1)
			}
			return true
		})

	case "datagram":
		s.quicConns.Range(func(k, v any) bool {
			qc := v.(*quicConn)
			if err := (*qc.conn).SendDatagram(frame); err != nil {
				s.logger.Debug("http3: datagram send failed",
					zap.String("id", string(qc.id)), zap.Error(err))
				s.lostCounter.Add(1)
			} else {
				s.metrics.RecordSend(seq, size)
			}
			return true
		})

	case "unidirstream":
		s.quicConns.Range(func(k, v any) bool {
			qc := v.(*quicConn)
			go s.sendUniStream(qc, frame, seq, size)
			return true
		})
	}
	return nil
}

// sendUniStream opens (or reuses) a unidirectional QUIC stream and writes data.
func (s *HTTP3Server) sendUniStream(qc *quicConn, frame []byte, seq uint64, size int) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if qc.stream == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		str, err := (*qc.conn).OpenUniStreamSync(ctx)
		if err != nil {
			s.logger.Debug("http3: open unistream failed",
				zap.String("id", string(qc.id)), zap.Error(err))
			s.lostCounter.Add(1)
			return
		}
		qc.stream = str
	}

	framed := prependLen(frame)
	if _, err := qc.stream.Write(framed); err != nil {
		s.logger.Debug("http3: unistream write failed",
			zap.String("id", string(qc.id)), zap.Error(err))
		qc.stream = nil
		s.lostCounter.Add(1)
		return
	}
	s.metrics.RecordSend(seq, size)
}

// Send delivers data to a single identified client.
func (s *HTTP3Server) Send(id transport.ConnID, data []byte) error {
	seq := s.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)

	if sc, ok := s.streams.Load(id); ok {
		framed := prependLen(frame)
		select {
		case sc.(*streamConn).ch <- framed:
			s.metrics.RecordSend(seq, len(framed))
			return nil
		default:
			s.lostCounter.Add(1)
			return fmt.Errorf("http3: send to %s: channel full", id)
		}
	}
	return transport.ErrClientNotFound
}

// Connections returns the current number of connected clients.
func (s *HTTP3Server) Connections() int {
	n := 0
	s.streams.Range(func(_, _ any) bool { n++; return true })
	s.quicConns.Range(func(_, _ any) bool { n++; return true })
	return n
}

// Stats returns a point-in-time performance snapshot.
func (s *HTTP3Server) Stats() transport.Stats {
	snap := s.metrics.Snapshot()
	base := s.BaseStats()
	lat := snap.Latency
	return transport.Stats{
		Protocol:      base.Protocol,
		Connections:   s.Connections(),
		Sent:          snap.MsgSent,
		Received:      snap.MsgRecv,
		Lost:          snap.Seq.Lost + s.lostCounter.Load(),
		Duplicated:    snap.Seq.Duplicated,
		Reordered:     snap.Seq.Reordered,
		BytesSent:     snap.BytesSent,
		BytesRecv:     snap.BytesRecv,
		MinLatencyNs:  lat.Min.Nanoseconds(),
		AvgLatencyNs:  lat.Mean.Nanoseconds(),
		P50LatencyNs:  lat.P50.Nanoseconds(),
		P95LatencyNs:  lat.P95.Nanoseconds(),
		P99LatencyNs:  lat.P99.Nanoseconds(),
		P999LatencyNs: lat.P999.Nanoseconds(),
		MaxLatencyNs:  lat.Max.Nanoseconds(),
		CPUPercent:    snap.Resources.CPUAvg,
		MemBytes:      snap.Resources.MemAvg,
		Goroutines:    snap.Resources.GoroutineAvg,
		FDs:           snap.Resources.FDAvg,
		Uptime:        base.Uptime,
		SnapshotAt:    base.SnapshotAt,
	}
}

// handleStream handles GET /stream — long-lived SSE-style byte stream over HTTP/3.
func (s *HTTP3Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	clientID := r.URL.Query().Get("id")
	if clientID == "" {
		clientID = fmt.Sprintf("h3-%d", s.connCounter.Add(1))
	}
	id := transport.ConnID(clientID)

	ctx, cancel := context.WithCancel(r.Context())
	sc := &streamConn{
		ch:     make(chan []byte, h3WriteChanCap),
		cancel: cancel,
		id:     id,
	}
	s.streams.Store(id, sc)
	defer func() {
		s.streams.Delete(id)
		cancel()
	}()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	s.logger.Debug("http3: stream client connected", zap.String("id", string(id)))

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case data, ok := <-sc.ch:
			if !ok {
				return
			}
			if _, err := w.Write(data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// prependLen prepends a 4-byte big-endian length to frame.
func prependLen(frame []byte) []byte {
	out := make([]byte, h3LenBytes+len(frame))
	binary.BigEndian.PutUint32(out[:h3LenBytes], uint32(len(frame)))
	copy(out[h3LenBytes:], frame)
	return out
}

// Addr returns the server's listen address (resolved).
func (s *HTTP3Server) Addr() net.Addr {
	if s.h3srv == nil {
		return nil
	}
	a, _ := net.ResolveUDPAddr("udp", s.addr)
	return a
}
