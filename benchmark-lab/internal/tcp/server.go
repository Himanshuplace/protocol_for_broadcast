// Package tcp provides a TCP transport implementation for the benchmark platform.
//
// Architecture:
//   - TCPServer creates N listeners using SO_REUSEPORT (N = min(NumCPU, 4)).
//     Each listener has its own accept goroutine, distributing connections across
//     kernel accept queues for reduced lock contention.
//   - Each accepted connection runs a readPump and writePump goroutine.
//   - Broadcast fans out to all active connections via per-connection write channels.
//   - All frames are prefixed with a 4-byte big-endian uint32 length field.
//   - TCP_NODELAY is enabled on every connection to minimize send latency.
package tcp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

const (
	tcpWriteChanCap = 256
	tcpReadBufSize  = 65536
	tcpWriteBufSize = 65536
	tcpLenBytes     = 4   // 4-byte big-endian length prefix per frame
	tcpMaxFrameSize = 8 * 1024 * 1024 // 8 MiB guard against malformed frames
)

// tcpConn wraps a net.Conn with a write channel and lifecycle management.
type tcpConn struct {
	conn    net.Conn
	writeCh chan []byte
	id      transport.ConnID
	cancel  context.CancelFunc
}

// TCPServer is a multi-listener TCP server that implements transport.Transport.
// Zero value is not usable; construct via NewTCPServer.
type TCPServer struct {
	transport.BaseTransport

	addr      string
	listeners []net.Listener
	registry  transport.Registry[*tcpConn]
	metrics   *metrics.Recorder
	logger    *zap.Logger

	// seqCounter is the per-server monotonically increasing send sequence number.
	seqCounter atomic.Uint64

	// lostCounter counts datagrams dropped due to full write channels.
	lostCounter atomic.Uint64

	// connCounter generates unique per-connection identifiers.
	connCounter atomic.Uint64

	// done signals all accept loops to stop.
	done chan struct{}
	wg   sync.WaitGroup

	// onRecv is called for every received wire frame (may be nil).
	onRecv transport.RecvHandler
}

// ServerOption is a functional option for TCPServer.
type ServerOption func(*TCPServer)

// WithTCPServerLogger attaches a zap logger to the server.
func WithTCPServerLogger(l *zap.Logger) ServerOption {
	return func(s *TCPServer) { s.logger = l }
}

// WithTCPServerRecorder attaches a metrics recorder to the server.
func WithTCPServerRecorder(r *metrics.Recorder) ServerOption {
	return func(s *TCPServer) { s.metrics = r }
}

// WithTCPServerRecvHandler sets the callback invoked for each received frame.
func WithTCPServerRecvHandler(h transport.RecvHandler) ServerOption {
	return func(s *TCPServer) { s.onRecv = h }
}

// NewTCPServer creates a new TCPServer that will listen on addr (e.g., "0.0.0.0:9001").
func NewTCPServer(addr string, opts ...ServerOption) *TCPServer {
	s := &TCPServer{
		addr: addr,
		done: make(chan struct{}),
	}
	s.SetProtocol("tcp")
	for _, o := range opts {
		o(s)
	}
	if s.logger == nil {
		s.logger = zap.NewNop()
	}
	if s.metrics == nil {
		s.metrics = metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "tcp/server",
			Protocol: "tcp",
			Scenario: "default",
		})
	}
	return s
}

// Start opens N TCP listeners and begins accepting connections.
// N = min(runtime.NumCPU(), 4). On Linux, SO_REUSEPORT is set so the kernel
// distributes incoming connections across the listeners without userspace coordination.
func (s *TCPServer) Start() error {
	if s.IsStarted() {
		return transport.ErrAlreadyStarted
	}

	n := runtime.NumCPU()
	if n > 4 {
		n = 4
	}

	// For the first listener, let OS pick a port if addr ends in ":0".
	// Subsequent listeners bind to the same resolved port.
	var resolvedAddr string

	for i := 0; i < n; i++ {
		bindAddr := s.addr
		if i > 0 && resolvedAddr != "" {
			bindAddr = resolvedAddr
		}

		lc := net.ListenConfig{
			Control: func(network, address string, c syscall.RawConn) error {
				return setReusePort(c)
			},
		}
		ln, err := lc.Listen(context.Background(), "tcp", bindAddr)
		if err != nil {
			// Close already-opened listeners.
			for _, existing := range s.listeners {
				_ = existing.Close()
			}
			s.listeners = nil
			return fmt.Errorf("tcp: listen %q (shard %d): %w", bindAddr, i, err)
		}

		if i == 0 {
			// Capture the actual address (port resolved from :0).
			resolvedAddr = ln.Addr().String()
		}

		s.listeners = append(s.listeners, ln)
	}

	s.MarkStarted()
	s.metrics.Start()

	// Recreate done channel in case Start is called after Stop.
	s.done = make(chan struct{})

	for _, ln := range s.listeners {
		s.wg.Add(1)
		go s.acceptLoop(ln)
	}

	s.logger.Info("tcp server started",
		zap.String("addr", resolvedAddr),
		zap.Int("listeners", n))
	return nil
}

// Stop closes all listeners and waits for all goroutines to exit.
func (s *TCPServer) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	close(s.done)

	// Close listeners to unblock Accept() calls.
	for _, ln := range s.listeners {
		_ = ln.Close()
	}

	// Close all active connections to unblock their read/write pumps.
	s.registry.Range(func(_ transport.ConnID, c *tcpConn) bool {
		c.cancel()
		_ = c.conn.Close()
		return true
	})

	s.wg.Wait()
	s.metrics.Stop()
	s.MarkStopped()
	s.logger.Info("tcp server stopped")
	return nil
}

// Broadcast sends data to all currently connected clients.
// The frame is encoded with a wire header and a 4-byte length prefix.
// If a client's write channel is full, the message is dropped and Lost is incremented.
func (s *TCPServer) Broadcast(data []byte) error {
	seq := s.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	framed := prependLength(frame)
	size := len(framed)

	peers := s.registry.Snapshot()
	if len(peers) == 0 {
		return nil
	}

	for _, p := range peers {
		select {
		case p.writeCh <- framed:
			s.metrics.RecordSend(seq, size)
		default:
			// Write channel full — drop and count as lost.
			s.lostCounter.Add(1)
		}
	}
	return nil
}

// Send delivers data to one specific client identified by id.
func (s *TCPServer) Send(id transport.ConnID, data []byte) error {
	peer, ok := s.registry.Get(id)
	if !ok {
		return transport.ErrClientNotFound
	}
	seq := s.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	framed := prependLength(frame)

	select {
	case peer.writeCh <- framed:
		s.metrics.RecordSend(seq, len(framed))
		return nil
	default:
		s.lostCounter.Add(1)
		return fmt.Errorf("tcp: send to %s: write channel full", id)
	}
}

// Connections returns the number of currently connected peers.
func (s *TCPServer) Connections() int {
	return s.registry.Len()
}

// Stats returns a point-in-time snapshot of transport statistics.
func (s *TCPServer) Stats() transport.Stats {
	snap := s.metrics.Snapshot()
	base := s.BaseStats()
	latSnap := snap.Latency
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
		MinLatencyNs:  latSnap.Min.Nanoseconds(),
		AvgLatencyNs:  latSnap.Mean.Nanoseconds(),
		P50LatencyNs:  latSnap.P50.Nanoseconds(),
		P95LatencyNs:  latSnap.P95.Nanoseconds(),
		P99LatencyNs:  latSnap.P99.Nanoseconds(),
		P999LatencyNs: latSnap.P999.Nanoseconds(),
		MaxLatencyNs:  latSnap.Max.Nanoseconds(),
		CPUPercent:    snap.Resources.CPUAvg,
		MemBytes:      snap.Resources.MemAvg,
		Goroutines:    snap.Resources.GoroutineAvg,
		FDs:           snap.Resources.FDAvg,
		Uptime:        base.Uptime,
		SnapshotAt:    base.SnapshotAt,
	}
}

// Addr returns the local address of the first listener.
// Only valid after Start().
func (s *TCPServer) Addr() net.Addr {
	if len(s.listeners) == 0 {
		return nil
	}
	return s.listeners[0].Addr()
}

// acceptLoop accepts connections on the given listener until done is closed.
func (s *TCPServer) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.logger.Warn("tcp: accept error", zap.Error(err))
				// Back off briefly to avoid hot-spinning on persistent errors.
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

// handleConn configures a new connection and launches its read/write pumps.
func (s *TCPServer) handleConn(raw net.Conn) {
	// Enable TCP_NODELAY — disables Nagle's algorithm for lowest latency.
	if tc, ok := raw.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetReadBuffer(tcpReadBufSize)
		_ = tc.SetWriteBuffer(tcpWriteBufSize)
	}

	id := transport.ConnID(fmt.Sprintf("tcp-%d", s.connCounter.Add(1)))
	ctx, cancel := context.WithCancel(context.Background())

	tc := &tcpConn{
		conn:    raw,
		writeCh: make(chan []byte, tcpWriteChanCap),
		id:      id,
		cancel:  cancel,
	}
	s.registry.Add(id, tc)

	s.logger.Debug("tcp: client connected",
		zap.String("id", string(id)),
		zap.String("remote", raw.RemoteAddr().String()))

	s.wg.Add(2)
	go s.readPump(ctx, tc)
	go s.writePump(ctx, tc)
}

// readPump reads length-prefixed frames from the connection and dispatches them.
func (s *TCPServer) readPump(ctx context.Context, tc *tcpConn) {
	defer s.wg.Done()
	defer func() {
		tc.cancel()
		_ = tc.conn.Close()
		s.registry.Remove(tc.id)
		s.logger.Debug("tcp: client disconnected", zap.String("id", string(tc.id)))
	}()

	lenBuf := make([]byte, tcpLenBytes)
	// Reuse a single pooled payload buffer; grow as needed.
	payloadBuf := make([]byte, tcpReadBufSize)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read the 4-byte length prefix.
		if _, err := io.ReadFull(tc.conn, lenBuf); err != nil {
			return // connection closed or error
		}

		frameLen := binary.BigEndian.Uint32(lenBuf)
		if frameLen == 0 || frameLen > tcpMaxFrameSize {
			s.logger.Warn("tcp: invalid frame length",
				zap.Uint32("len", frameLen),
				zap.String("id", string(tc.id)))
			return
		}

		// Grow buffer if needed.
		if int(frameLen) > len(payloadBuf) {
			payloadBuf = make([]byte, frameLen)
		}
		payload := payloadBuf[:frameLen]

		if _, err := io.ReadFull(tc.conn, payload); err != nil {
			return
		}
		recvAt := time.Now()

		if len(payload) < wire.HeaderLen {
			continue
		}
		frame, _, err := wire.Decode(payload)
		if err != nil {
			s.logger.Debug("tcp: decode error",
				zap.String("id", string(tc.id)), zap.Error(err))
			continue
		}

		s.metrics.RecordRecv(frame.SeqNum, frame.SendNs, int(frameLen), recvAt.UnixNano())

		if s.onRecv != nil {
			s.onRecv(tc.id, payload, recvAt)
		}
	}
}

// writePump drains the write channel and sends frames to the connection.
func (s *TCPServer) writePump(ctx context.Context, tc *tcpConn) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-tc.writeCh:
			if !ok {
				return
			}
			if _, err := tc.conn.Write(data); err != nil {
				return
			}
		}
	}
}

// prependLength returns a new slice containing the 4-byte big-endian length
// of frame followed by frame itself.
func prependLength(frame []byte) []byte {
	out := make([]byte, tcpLenBytes+len(frame))
	binary.BigEndian.PutUint32(out[:tcpLenBytes], uint32(len(frame)))
	copy(out[tcpLenBytes:], frame)
	return out
}
