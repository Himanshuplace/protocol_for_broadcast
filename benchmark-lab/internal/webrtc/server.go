package webrtc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v3"
	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

// ChannelMode controls DataChannel reliability.
type ChannelMode string

const (
	ModeReliable       ChannelMode = "reliable"
	ModeUnreliable     ChannelMode = "unreliable"
	ModePartialReliable ChannelMode = "partial-reliable"
)

type rtcPeer struct {
	id      string
	pc      *webrtc.PeerConnection
	dc      *webrtc.DataChannel
	mu      sync.Mutex
}

// WebRTCServer broadcasts market tick data via WebRTC DataChannels.
type WebRTCServer struct {
	signalingAddr string
	mode          ChannelMode
	signaling     *SignalingServer

	peers     sync.Map // id -> *rtcPeer
	peersMu   sync.RWMutex

	sent      atomic.Uint64
	recv      atomic.Uint64
	lost      atomic.Uint64
	sentBytes atomic.Uint64
	recvBytes atomic.Uint64

	metrics *metrics.Recorder
	logger  *zap.Logger
}

// NewWebRTCServer creates a WebRTC broadcasting server with a built-in signaling HTTP server.
func NewWebRTCServer(signalingAddr string, mode ChannelMode, rec *metrics.Recorder, logger *zap.Logger) *WebRTCServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	if mode == "" {
		mode = ModeReliable
	}
	s := &WebRTCServer{
		signalingAddr: signalingAddr,
		mode:          mode,
		metrics:       rec,
		logger:        logger,
	}
	s.signaling = NewSignalingServer(signalingAddr, logger, s.onPeerConnected)
	return s
}

func (s *WebRTCServer) Start() error {
	return s.signaling.Start()
}

func (s *WebRTCServer) Stop() error {
	s.peers.Range(func(key, val any) bool {
		p := val.(*rtcPeer)
		p.pc.Close()
		return true
	})
	return s.signaling.Stop()
}

func (s *WebRTCServer) onPeerConnected(id string, pc *webrtc.PeerConnection) {
	s.logger.Info("webrtc: peer connected", zap.String("id", id))

	var (
		maxRet  *uint16
		ordered *bool
	)
	falseVal := false
	zeroVal := uint16(0)
	twoVal := uint16(2)

	switch s.mode {
	case ModeUnreliable:
		ordered = &falseVal
		maxRet = &zeroVal
	case ModePartialReliable:
		ordered = &falseVal
		maxRet = &twoVal
	default: // reliable
		// defaults: ordered=true, no retransmit limit
	}

	dcInit := &webrtc.DataChannelInit{
		Ordered:        ordered,
		MaxRetransmits: maxRet,
	}

	dc, err := pc.CreateDataChannel("benchmark", dcInit)
	if err != nil {
		s.logger.Error("webrtc: create datachannel failed", zap.Error(err))
		pc.Close()
		return
	}

	peer := &rtcPeer{id: id, pc: pc, dc: dc}
	s.peers.Store(id, peer)

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		s.recv.Add(1)
		s.recvBytes.Add(uint64(len(msg.Data)))
	})

	dc.OnClose(func() {
		s.logger.Debug("webrtc: datachannel closed", zap.String("id", id))
		s.peers.Delete(id)
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.peers.Delete(id)
		}
	})
}

func (s *WebRTCServer) Broadcast(data []byte) error {
	s.sent.Add(1)
	s.sentBytes.Add(uint64(len(data)))

	s.peers.Range(func(key, val any) bool {
		peer := val.(*rtcPeer)
		peer.mu.Lock()
		defer peer.mu.Unlock()

		if peer.dc == nil || peer.dc.ReadyState() != webrtc.DataChannelStateOpen {
			s.lost.Add(1)
			return true
		}
		if err := peer.dc.Send(data); err != nil {
			s.lost.Add(1)
		}
		return true
	})
	return nil
}

func (s *WebRTCServer) Send(id transport.ConnID, data []byte) error {
	val, ok := s.peers.Load(string(id))
	if !ok {
		return fmt.Errorf("webrtc: unknown peer %s", id)
	}
	peer := val.(*rtcPeer)
	peer.mu.Lock()
	defer peer.mu.Unlock()

	if peer.dc == nil || peer.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return fmt.Errorf("webrtc: datachannel not open for %s", id)
	}
	return peer.dc.Send(data)
}

func (s *WebRTCServer) Connections() int {
	count := 0
	s.peers.Range(func(_, _ any) bool { count++; return true })
	return count
}

func (s *WebRTCServer) Stats() transport.Stats {
	return transport.Stats{
		Protocol:    "webrtc",
		Connections: s.Connections(),
		Sent:        s.sent.Load(),
		Received:    s.recv.Load(),
		Lost:        s.lost.Load(),
		BytesSent:   s.sentBytes.Load(),
		BytesRecv:   s.recvBytes.Load(),
		SnapshotAt:  time.Now(),
	}
}
