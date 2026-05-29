package sse

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// replayBuf holds the last N events for Last-Event-ID reconnect support.
const replayBufSize = 1000

type sseEvent struct {
	id   uint64
	data []byte // base64-encoded wire frame
}

type sseStream struct {
	id      transport.ConnID
	ch      chan sseEvent
	flusher http.Flusher
	w       http.ResponseWriter
	cancel  context.CancelFunc
}

// SSEServer implements text/event-stream broadcasting.
// Supports Last-Event-ID reconnect with a 1000-event replay buffer.
type SSEServer struct {
	addr    string
	httpSrv *http.Server
	mux     *http.ServeMux

	streams   sync.Map // ConnID -> *sseStream
	streamsMu sync.RWMutex

	// replay ring buffer
	replayMu  sync.RWMutex
	replayBuf [replayBufSize]sseEvent
	replayIdx int
	replayLen int

	seqCounter atomic.Uint64
	sentBytes  atomic.Uint64
	recvBytes  atomic.Uint64
	sent       atomic.Uint64
	recv       atomic.Uint64
	lost       atomic.Uint64

	metrics *metrics.Recorder
	logger  *zap.Logger

	started    bool
	startedMu  sync.Mutex
	shutdownCh chan struct{}
}

// NewSSEServer creates an SSE server on addr.
func NewSSEServer(addr string, rec *metrics.Recorder, logger *zap.Logger) *SSEServer {
	if logger == nil {
		logger = zap.NewNop()
	}
	s := &SSEServer{
		addr:       addr,
		metrics:    rec,
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	s.mux = mux
	s.httpSrv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return s
}

func (s *SSEServer) Start() error {
	s.startedMu.Lock()
	defer s.startedMu.Unlock()
	if s.started {
		return fmt.Errorf("sse: already started")
	}
	s.started = true
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("sse: listen %s: %w", s.addr, err)
	}
	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.logger.Error("sse server error", zap.Error(err))
		}
	}()
	s.logger.Info("sse: server started", zap.String("addr", s.addr))
	return nil
}

func (s *SSEServer) Stop() error {
	close(s.shutdownCh)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpSrv.Shutdown(ctx)
}

func (s *SSEServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	id := transport.ConnID(fmt.Sprintf("sse-%d", s.seqCounter.Add(1)))
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, string(id))
}

func (s *SSEServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	connID := r.URL.Query().Get("id")
	if connID == "" {
		connID = fmt.Sprintf("sse-auto-%d", s.seqCounter.Add(1))
	}
	id := transport.ConnID(connID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Replay missed events if Last-Event-ID provided
	lastID := r.Header.Get("Last-Event-ID")
	if lastID != "" {
		var lastSeq uint64
		fmt.Sscanf(lastID, "%d", &lastSeq)
		s.replayFrom(w, flusher, lastSeq)
	}

	ctx, cancel := context.WithCancel(r.Context())
	stream := &sseStream{
		id:      id,
		ch:      make(chan sseEvent, 256),
		flusher: flusher,
		w:       w,
		cancel:  cancel,
	}
	s.streams.Store(id, stream)
	defer func() {
		s.streams.Delete(id)
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.shutdownCh:
			return
		case ev, ok := <-stream.ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.id, ev.data)
			flusher.Flush()
			s.recv.Add(1)
		}
	}
}

func (s *SSEServer) replayFrom(w http.ResponseWriter, flusher http.Flusher, afterSeq uint64) {
	s.replayMu.RLock()
	defer s.replayMu.RUnlock()
	for i := 0; i < s.replayLen; i++ {
		idx := (s.replayIdx - s.replayLen + i + replayBufSize) % replayBufSize
		ev := s.replayBuf[idx]
		if ev.id > afterSeq {
			fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.id, ev.data)
		}
	}
	flusher.Flush()
}

func (s *SSEServer) addToReplay(ev sseEvent) {
	s.replayMu.Lock()
	s.replayBuf[s.replayIdx] = ev
	s.replayIdx = (s.replayIdx + 1) % replayBufSize
	if s.replayLen < replayBufSize {
		s.replayLen++
	}
	s.replayMu.Unlock()
}

func (s *SSEServer) Broadcast(data []byte) error {
	frame, _, err := wire.Decode(data)
	if err != nil || len(frame.Payload) == 0 {
		// treat as raw data
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	seq := s.seqCounter.Add(1)
	ev := sseEvent{id: seq, data: []byte(encoded)}

	s.addToReplay(ev)
	s.sent.Add(1)
	s.sentBytes.Add(uint64(len(data)))

	var dropped int
	s.streams.Range(func(key, val any) bool {
		stream := val.(*sseStream)
		select {
		case stream.ch <- ev:
		default:
			dropped++
			s.lost.Add(1)
		}
		return true
	})
	return nil
}

func (s *SSEServer) Send(id transport.ConnID, data []byte) error {
	val, ok := s.streams.Load(id)
	if !ok {
		return fmt.Errorf("sse: unknown conn %s", id)
	}
	stream := val.(*sseStream)
	encoded := base64.StdEncoding.EncodeToString(data)
	seq := s.seqCounter.Add(1)
	ev := sseEvent{id: seq, data: []byte(encoded)}
	select {
	case stream.ch <- ev:
		s.sent.Add(1)
	default:
		s.lost.Add(1)
	}
	return nil
}

func (s *SSEServer) Connections() int {
	count := 0
	s.streams.Range(func(_, _ any) bool { count++; return true })
	return count
}

func (s *SSEServer) Stats() transport.Stats {
	return transport.Stats{
		Protocol:    "sse",
		Connections: s.Connections(),
		Sent:        s.sent.Load(),
		Received:    s.recv.Load(),
		Lost:        s.lost.Load(),
		BytesSent:   s.sentBytes.Load(),
		BytesRecv:   s.recvBytes.Load(),
		SnapshotAt:  time.Now(),
	}
}
