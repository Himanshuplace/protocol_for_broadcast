package webtransport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	wt "github.com/quic-go/webtransport-go"
	"go.uber.org/zap"

	itls "github.com/himanshuplace/protocol_for_broadcast/internal/tls"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

// Mode controls the WebTransport data delivery primitive.
type Mode string

const (
	ModeUniStream  Mode = "unidirstream" // server->client uni streams (default)
	ModeBidiStream Mode = "bidistream"   // bidirectional streams
	ModeDatagrams  Mode = "datagram"     // QUIC datagrams (unreliable)
)

type wtSession struct {
	id      transport.ConnID
	session *wt.Session
	mu      sync.Mutex
	cancel  context.CancelFunc
}

// WebTransportServer broadcasts over WebTransport (HTTP/3 + QUIC).
type WebTransportServer struct {
	addr   string
	mode   Mode
	server *wt.Server
	h3     *http3.Server

	sessions   sync.Map // ConnID -> *wtSession
	seqCounter atomic.Uint64
	sent       atomic.Uint64
	recv       atomic.Uint64
	lost       atomic.Uint64
	sentBytes  atomic.Uint64

	metrics *metrics.Recorder
	logger  *zap.Logger

	started   bool
	startedMu sync.Mutex
}

// NewWebTransportServer creates a WebTransport server.
func NewWebTransportServer(addr string, mode Mode, rec *metrics.Recorder, logger *zap.Logger) (*WebTransportServer, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if mode == "" {
		mode = ModeUniStream
	}

	cert, err := itls.GenerateSelfSigned()
	if err != nil {
		return nil, fmt.Errorf("webtransport: gen cert: %w", err)
	}
	tlsCfg := itls.ServerTLSConfig(cert, "h3")

	s := &WebTransportServer{
		addr:    addr,
		mode:    mode,
		metrics: rec,
		logger:  logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/webtransport", s.handleSession)

	h3Srv := &http3.Server{
		Addr:      addr,
		TLSConfig: tlsCfg,
		Handler:   mux,
		QUICConfig: &quic.Config{
			EnableDatagrams: true,
		},
	}
	wtSrv := &wt.Server{
		H3:          h3Srv,
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	s.h3 = h3Srv
	s.server = wtSrv
	return s, nil
}

func (s *WebTransportServer) Start() error {
	s.startedMu.Lock()
	defer s.startedMu.Unlock()
	if s.started {
		return fmt.Errorf("webtransport: already started")
	}
	s.started = true
	go func() {
		if err := s.h3.ListenAndServe(); err != nil {
			s.logger.Error("webtransport server error", zap.Error(err))
		}
	}()
	s.logger.Info("webtransport: server started", zap.String("addr", s.addr), zap.String("mode", string(s.mode)))
	return nil
}

func (s *WebTransportServer) Stop() error {
	return s.h3.Close()
}

func (s *WebTransportServer) handleSession(w http.ResponseWriter, r *http.Request) {
	session, err := s.server.Upgrade(w, r)
	if err != nil {
		s.logger.Error("webtransport: upgrade failed", zap.Error(err))
		return
	}

	id := transport.ConnID(fmt.Sprintf("wt-%d", s.seqCounter.Add(1)))
	ctx, cancel := context.WithCancel(r.Context())
	wtSess := &wtSession{id: id, session: session, cancel: cancel}
	s.sessions.Store(id, wtSess)
	defer func() {
		s.sessions.Delete(id)
		cancel()
	}()

	s.logger.Debug("webtransport: client connected", zap.String("id", string(id)))

	switch s.mode {
	case ModeDatagrams:
		for {
			data, err := session.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			s.recv.Add(1)
			_ = data
		}
	case ModeBidiStream:
		for {
			stream, err := session.AcceptStream(ctx)
			if err != nil {
				return
			}
			go s.readBidiStream(ctx, stream)
		}
	default: // ModeUniStream: server sends only; wait for session close
		<-ctx.Done()
	}
}

func (s *WebTransportServer) readBidiStream(ctx context.Context, stream *wt.Stream) {
	buf := make([]byte, 65536)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			s.recv.Add(1)
		}
		if err != nil {
			return
		}
	}
}

func (s *WebTransportServer) Broadcast(data []byte) error {
	s.sent.Add(1)
	s.sentBytes.Add(uint64(len(data)))

	s.sessions.Range(func(key, val any) bool {
		wtSess := val.(*wtSession)
		wtSess.mu.Lock()
		defer wtSess.mu.Unlock()

		var err error
		ctx := context.Background()
		switch s.mode {
		case ModeDatagrams:
			err = wtSess.session.SendDatagram(data)
		case ModeUniStream:
			var stream *wt.SendStream
			stream, err = wtSess.session.OpenUniStreamSync(ctx)
			if err == nil {
				_, err = stream.Write(data)
				stream.Close()
			}
		case ModeBidiStream:
			var stream *wt.Stream
			stream, err = wtSess.session.OpenStreamSync(ctx)
			if err == nil {
				_, err = stream.Write(data)
			}
		}
		if err != nil {
			s.lost.Add(1)
		}
		return true
	})
	return nil
}

func (s *WebTransportServer) Send(id transport.ConnID, data []byte) error {
	val, ok := s.sessions.Load(id)
	if !ok {
		return fmt.Errorf("webtransport: unknown conn %s", id)
	}
	wtSess := val.(*wtSession)
	wtSess.mu.Lock()
	defer wtSess.mu.Unlock()

	ctx := context.Background()
	switch s.mode {
	case ModeDatagrams:
		return wtSess.session.SendDatagram(data)
	default:
		stream, err := wtSess.session.OpenUniStreamSync(ctx)
		if err != nil {
			return err
		}
		_, err = stream.Write(data)
		stream.Close()
		return err
	}
}

func (s *WebTransportServer) Connections() int {
	count := 0
	s.sessions.Range(func(_, _ any) bool { count++; return true })
	return count
}

func (s *WebTransportServer) Stats() transport.Stats {
	return transport.Stats{
		Protocol:    "webtransport",
		Connections: s.Connections(),
		Sent:        s.sent.Load(),
		Received:    s.recv.Load(),
		Lost:        s.lost.Load(),
		BytesSent:   s.sentBytes.Load(),
		SnapshotAt:  time.Now(),
	}
}

// ServerTLSConfig returns a TLS config used by the server (exposed for client use).
func ServerTLSConfig() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true} //nolint:gosec
}
