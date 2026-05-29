package webrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v3"
	"go.uber.org/zap"
)

// SignalingServer handles WebRTC SDP and ICE candidate exchange.
type SignalingServer struct {
	addr    string
	httpSrv *http.Server
	mux     *http.ServeMux

	pending sync.Map // offerID -> *pendingConn
	idGen   atomic.Uint64
	logger  *zap.Logger

	// callback when a new PeerConnection is established
	onConnect func(id string, pc *webrtc.PeerConnection)
}

type pendingConn struct {
	id         string
	pc         *webrtc.PeerConnection
	candidates []webrtc.ICECandidateInit
	mu         sync.Mutex
	connected  chan struct{}
}

type offerRequest struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"` // "offer"
}

type answerResponse struct {
	SDP  string `json:"sdp"`
	Type string `json:"type"` // "answer"
	ID   string `json:"id"`
}

type iceRequest struct {
	ID        string `json:"id"`
	Candidate string `json:"candidate"`
	SDPMid    string `json:"sdpMid"`
	SDPIndex  uint16 `json:"sdpMLineIndex"`
}

func NewSignalingServer(addr string, logger *zap.Logger, onConnect func(id string, pc *webrtc.PeerConnection)) *SignalingServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &SignalingServer{addr: addr, logger: logger, onConnect: onConnect}
	mux := http.NewServeMux()
	mux.HandleFunc("/webrtc/offer", s.handleOffer)
	mux.HandleFunc("/webrtc/ice", s.handleICE)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	s.mux = mux
	s.httpSrv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return s
}

func (s *SignalingServer) Start() error {
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("signaling: listen: %w", err)
	}
	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Error("signaling server error", zap.Error(err))
		}
	}()
	return nil
}

func (s *SignalingServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpSrv.Shutdown(ctx)
}

func (s *SignalingServer) handleOffer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req offerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	id := fmt.Sprintf("webrtc-%d", s.idGen.Add(1))
	pending := &pendingConn{id: id, pc: pc, connected: make(chan struct{})}
	s.pending.Store(id, pending)

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  req.SDP,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Drain any ICE candidates that arrived before SetRemoteDescription
	pending.mu.Lock()
	for _, c := range pending.candidates {
		_ = pc.AddICECandidate(c)
	}
	pending.candidates = nil
	pending.mu.Unlock()

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	<-gatherComplete

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected && s.onConnect != nil {
			s.pending.Delete(id)
			s.onConnect(id, pc)
			close(pending.connected)
		}
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.pending.Delete(id)
		}
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(answerResponse{
		SDP:  pc.LocalDescription().SDP,
		Type: "answer",
		ID:   id,
	})
}

func (s *SignalingServer) handleICE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req iceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	val, ok := s.pending.Load(req.ID)
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	p := val.(*pendingConn)
	cand := webrtc.ICECandidateInit{
		Candidate:        req.Candidate,
		SDPMid:           &req.SDPMid,
		SDPMLineIndex:    &req.SDPIndex,
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pc.RemoteDescription() != nil {
		_ = p.pc.AddICECandidate(cand)
	} else {
		p.candidates = append(p.candidates, cand)
	}
	w.WriteHeader(http.StatusOK)
}
