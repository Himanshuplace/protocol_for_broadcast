// Package udp provides a UDP transport implementation for the benchmark platform.
//
// Architecture:
//   - UDPServer: connectionless server that treats every sender address as a "peer".
//   - Peers are discovered on first packet and evicted after 30 s of silence (TTL).
//   - On Linux, recvmmsg/sendmmsg are used for batch I/O (up to 64 packets/syscall).
//   - On other platforms the server falls back to ReadFromUDP/WriteToUDP.
//   - A 16-shard Registry[*udpPeer] is used for concurrent peer management.
package udp

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

const (
	peerTTL        = 30 * time.Second
	cleanupInterval = 5 * time.Second
	recvBufSize    = 4 * 1024 * 1024 // 4 MiB SO_RCVBUF / SO_SNDBUF
	maxBatchSize   = 64              // max packets per recvmmsg call
	maxUDPSize     = 65536
)

// udpPeer represents a registered UDP peer (client).
type udpPeer struct {
	addr     *net.UDPAddr
	lastSeen time.Time
	connID   transport.ConnID
}

// UDPServer is a UDP transport server that implements transport.Transport.
// Zero value is not usable; construct via NewUDPServer.
type UDPServer struct {
	transport.BaseTransport

	addr    string
	conn    *net.UDPConn
	metrics *metrics.Recorder
	logger  *zap.Logger

	// registry maps connID -> *udpPeer across 16 shards.
	registry transport.Registry[*udpPeer]

	// pool of receive buffers ([maxUDPSize]byte) to reduce allocations.
	pool sync.Pool

	// seqCounter is the monotonically increasing send sequence number.
	seqCounter atomic.Uint64

	// lostCounter tracks datagrams dropped during broadcast (sendmmsg errors).
	lostCounter atomic.Uint64

	// done signals all background goroutines to stop.
	done chan struct{}
	wg   sync.WaitGroup

	// onRecv is called for every received wire frame (may be nil).
	onRecv transport.RecvHandler
}

// ServerOption is a functional option for UDPServer.
type ServerOption func(*UDPServer)

// WithServerLogger attaches a zap logger to the server.
func WithServerLogger(l *zap.Logger) ServerOption {
	return func(s *UDPServer) { s.logger = l }
}

// WithServerRecorder attaches a metrics recorder to the server.
func WithServerRecorder(r *metrics.Recorder) ServerOption {
	return func(s *UDPServer) { s.metrics = r }
}

// WithServerRecvHandler sets the callback invoked for each received frame.
func WithServerRecvHandler(h transport.RecvHandler) ServerOption {
	return func(s *UDPServer) { s.onRecv = h }
}

// NewUDPServer creates a new UDPServer that will listen on addr (e.g., "0.0.0.0:9000").
func NewUDPServer(addr string, opts ...ServerOption) *UDPServer {
	s := &UDPServer{
		addr: addr,
		done: make(chan struct{}),
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, maxUDPSize)
				return &b
			},
		},
	}
	s.SetProtocol("udp")
	for _, o := range opts {
		o(s)
	}
	if s.logger == nil {
		s.logger = zap.NewNop()
	}
	if s.metrics == nil {
		s.metrics = metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "udp/server",
			Protocol: "udp",
			Scenario: "default",
		})
	}
	return s
}

// Start opens the UDP socket and begins the receive loop.
func (s *UDPServer) Start() error {
	if s.IsStarted() {
		return transport.ErrAlreadyStarted
	}

	udpAddr, err := net.ResolveUDPAddr("udp", s.addr)
	if err != nil {
		return fmt.Errorf("udp: resolve addr %q: %w", s.addr, err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("udp: listen %q: %w", s.addr, err)
	}
	s.conn = conn

	// Enlarge socket buffers to reduce drop under bursts.
	if err := setSocketBuf(conn, recvBufSize); err != nil {
		s.logger.Warn("udp: setSocketBuf failed", zap.Error(err))
	}

	s.MarkStarted()
	s.metrics.Start()

	// done channel may have been exhausted by a previous Stop(); recreate it.
	s.done = make(chan struct{})

	s.wg.Add(2)
	go s.receiveLoop()
	go s.cleanupLoop()

	s.logger.Info("udp server started", zap.String("addr", s.addr),
		zap.String("os", runtime.GOOS))
	return nil
}

// Stop closes the socket and waits for background goroutines to exit.
func (s *UDPServer) Stop() error {
	if !s.IsStarted() {
		return transport.ErrNotStarted
	}
	close(s.done)
	// Closing the conn unblocks the receive loop.
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.wg.Wait()
	s.metrics.Stop()
	s.MarkStopped()
	s.logger.Info("udp server stopped")
	return nil
}

// Broadcast sends data to all currently registered peers.
// On Linux it uses sendmmsg for batch sending; otherwise it loops WriteToUDP.
func (s *UDPServer) Broadcast(data []byte) error {
	seq := s.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	size := len(frame)

	peers := s.registry.Snapshot()
	if len(peers) == 0 {
		return nil
	}

	addrs := make([]*net.UDPAddr, 0, len(peers))
	for _, p := range peers {
		addrs = append(addrs, p.addr)
	}

	var sendErr error
	if runtime.GOOS == "linux" {
		sendErr = batchSend(s.conn, addrs, frame)
	} else {
		for _, addr := range addrs {
			if _, err := s.conn.WriteToUDP(frame, addr); err != nil {
				s.lostCounter.Add(1)
				sendErr = err
			}
		}
	}

	s.metrics.RecordSend(seq, size*len(peers))
	return sendErr
}

// Send sends data to a single peer identified by connID.
func (s *UDPServer) Send(id transport.ConnID, data []byte) error {
	peer, ok := s.registry.Get(id)
	if !ok {
		return transport.ErrClientNotFound
	}

	seq := s.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	_, err := s.conn.WriteToUDP(frame, peer.addr)
	if err != nil {
		return fmt.Errorf("udp: send to %s: %w", id, err)
	}
	s.metrics.RecordSend(seq, len(frame))
	return nil
}

// Connections returns the number of currently registered peers.
func (s *UDPServer) Connections() int {
	return s.registry.Len()
}

// Stats returns a point-in-time snapshot of transport statistics.
func (s *UDPServer) Stats() transport.Stats {
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

// Addr returns the local address the server is listening on.
// Only valid after Start() has been called.
func (s *UDPServer) Addr() net.Addr {
	if s.conn == nil {
		return nil
	}
	return s.conn.LocalAddr()
}

// receiveLoop is the main receive goroutine. It dispatches to the platform-specific
// batch receive function on Linux, or falls back to ReadFromUDP elsewhere.
func (s *UDPServer) receiveLoop() {
	defer s.wg.Done()

	if runtime.GOOS == "linux" {
		s.receiveLoopLinux()
	} else {
		s.receiveLoopGeneric()
	}
}

// receiveLoopGeneric is the non-Linux receive loop using conn.ReadFromUDP.
func (s *UDPServer) receiveLoopGeneric() {
	bufp := s.pool.Get().(*[]byte)
	buf := *bufp
	defer func() {
		*bufp = buf
		s.pool.Put(bufp)
	}()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		_ = s.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			select {
			case <-s.done:
				return
			default:
				s.logger.Warn("udp: ReadFromUDP error", zap.Error(err))
				continue
			}
		}
		recvAt := time.Now()
		s.handlePacket(buf[:n], addr, recvAt)
	}
}

// receiveLoopLinux uses batchRecv (recvmmsg) to read multiple datagrams per syscall.
func (s *UDPServer) receiveLoopLinux() {
	// Allocate batch buffers once.
	bufs := make([][]byte, maxBatchSize)
	addrs := make([]*net.UDPAddr, maxBatchSize)
	for i := range bufs {
		bufs[i] = make([]byte, maxUDPSize)
	}

	for {
		select {
		case <-s.done:
			return
		default:
		}

		_ = s.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := batchRecv(s.conn, bufs, addrs)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			select {
			case <-s.done:
				return
			default:
				s.logger.Warn("udp: batchRecv error", zap.Error(err))
				continue
			}
		}

		recvAt := time.Now()
		for i := 0; i < n; i++ {
			s.handlePacket(bufs[i], addrs[i], recvAt)
		}
	}
}

// handlePacket processes one received UDP datagram.
func (s *UDPServer) handlePacket(data []byte, addr *net.UDPAddr, recvAt time.Time) {
	if addr == nil || len(data) == 0 {
		return
	}

	connID := transport.ConnID("udp-" + addr.String())

	// Register peer if unknown, or refresh lastSeen.
	if peer, ok := s.registry.Get(connID); ok {
		peer.lastSeen = recvAt
	} else {
		s.registry.Add(connID, &udpPeer{
			addr:     addr,
			lastSeen: recvAt,
			connID:   connID,
		})
		s.logger.Debug("udp: new peer registered", zap.String("id", string(connID)))
	}

	// Heartbeat / registration packets have no wire frame — skip decode.
	if len(data) < wire.HeaderLen {
		return
	}

	frame, _, err := wire.Decode(data)
	if err != nil {
		s.logger.Debug("udp: decode error", zap.String("from", addr.String()), zap.Error(err))
		return
	}

	s.metrics.RecordRecv(frame.SeqNum, frame.SendNs, len(data), recvAt.UnixNano())

	if s.onRecv != nil {
		s.onRecv(connID, data, recvAt)
	}
}

// cleanupLoop periodically evicts peers that have not sent a packet within peerTTL.
func (s *UDPServer) cleanupLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.evictStalePeers()
		}
	}
}

// evictStalePeers removes peers whose lastSeen is older than peerTTL.
func (s *UDPServer) evictStalePeers() {
	now := time.Now()
	var stale []transport.ConnID
	s.registry.Range(func(id transport.ConnID, p *udpPeer) bool {
		if now.Sub(p.lastSeen) > peerTTL {
			stale = append(stale, id)
		}
		return true
	})
	for _, id := range stale {
		s.registry.Remove(id)
		s.logger.Debug("udp: peer evicted (TTL)", zap.String("id", string(id)))
	}
}

// isTimeout reports whether err is a net.Error with Timeout() == true.
func isTimeout(err error) bool {
	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}
	return false
}
