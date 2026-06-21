package webtransport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/quic-go/quic-go"
	wt "github.com/quic-go/webtransport-go"
	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// WebTransportClient connects to a WebTransport server.
type WebTransportClient struct {
	serverAddr string
	id         transport.ConnID
	mode       Mode
	handler    transport.RecvHandler
	recorder   *metrics.Recorder
	logger     *zap.Logger

	dialer  *wt.Dialer
	session *wt.Session

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWebTransportClient creates a WebTransport client.
func NewWebTransportClient(serverAddr string, mode Mode, handler transport.RecvHandler, rec *metrics.Recorder, logger *zap.Logger) *WebTransportClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	if mode == "" {
		mode = ModeUniStream
	}
	return &WebTransportClient{
		serverAddr: serverAddr,
		id:         transport.ConnID(uuid.NewString()),
		mode:       mode,
		handler:    handler,
		recorder:   rec,
		logger:     logger,
		dialer: &wt.Dialer{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
			QUICConfig: &quic.Config{
				EnableDatagrams:                  true,
				EnableStreamResetPartialDelivery: true,
			},
		},
	}
}

// Connect establishes the WebTransport session and starts receiving.
func (c *WebTransportClient) Connect(ctx context.Context) error {
	url := fmt.Sprintf("https://%s/webtransport", c.serverAddr)
	rsp, session, err := c.dialer.Dial(ctx, url, http.Header{})
	if err != nil {
		return fmt.Errorf("webtransport client: dial %s: %w", url, err)
	}
	if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("webtransport client: unexpected status %d", rsp.StatusCode)
	}
	c.session = session

	ctx2, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go c.receiveLoop(ctx2)
	return nil
}

func (c *WebTransportClient) receiveLoop(ctx context.Context) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		switch c.mode {
		case ModeDatagrams:
			data, err := c.session.ReceiveDatagram(ctx)
			if err != nil {
				return
			}
			c.process(data)
		case ModeUniStream:
			stream, err := c.session.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			go c.readUniStream(stream)
		case ModeBidiStream:
			stream, err := c.session.AcceptStream(ctx)
			if err != nil {
				return
			}
			go c.readBidiStream(stream)
		}
	}
}

func (c *WebTransportClient) readUniStream(stream *wt.ReceiveStream) {
	buf := make([]byte, 65536)
	var accumulated []byte
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			accumulated = append(accumulated, buf[:n]...)
		}
		if err != nil {
			if len(accumulated) > 0 {
				c.process(accumulated)
			}
			return
		}
	}
}

func (c *WebTransportClient) readBidiStream(stream *wt.Stream) {
	buf := make([]byte, 65536)
	var accumulated []byte
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			accumulated = append(accumulated, buf[:n]...)
		}
		if err != nil {
			if len(accumulated) > 0 {
				c.process(accumulated)
			}
			return
		}
	}
}

func (c *WebTransportClient) process(data []byte) {
	recvAt := time.Now()
	frame, _, err := wire.Decode(data)
	if err == nil && c.recorder != nil {
		c.recorder.RecordRecv(frame.SeqNum, frame.SendNs, len(data), recvAt.UnixNano())
	}
	if c.handler != nil {
		c.handler(c.id, data, recvAt)
	}
}

// Close disconnects the client.
func (c *WebTransportClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.session != nil {
		c.session.CloseWithError(0, "close")
	}
	c.wg.Wait()
}
