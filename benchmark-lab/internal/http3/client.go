// Package http3 provides an HTTP/3-over-QUIC transport implementation.
package http3

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	h3ClientInitialBackoff = 100 * time.Millisecond
	h3ClientMaxBackoff     = 30 * time.Second
	h3ClientReadBuf        = 65536
)

// HTTP3Client connects to an HTTP3Server and reads broadcast frames.
type HTTP3Client struct {
	serverAddr string
	clientID   string
	Mode       string // "stream" | "unidirstream" | "datagram"

	roundTripper *http3.Transport
	logger       *zap.Logger
	metrics      *metrics.Recorder
	onRecv       transport.RecvHandler

	seqCounter  atomic.Uint64
	lostCounter atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	transport.BaseTransport
}

// ClientOption is a functional option for HTTP3Client.
type ClientOption func(*HTTP3Client)

// WithClientLogger attaches a zap logger.
func WithClientLogger(l *zap.Logger) ClientOption {
	return func(c *HTTP3Client) { c.logger = l }
}

// WithClientRecorder attaches a metrics recorder.
func WithClientRecorder(r *metrics.Recorder) ClientOption {
	return func(c *HTTP3Client) { c.metrics = r }
}

// WithClientRecvHandler sets the inbound frame callback.
func WithClientRecvHandler(h transport.RecvHandler) ClientOption {
	return func(c *HTTP3Client) { c.onRecv = h }
}

// WithClientMode sets the transport mode.
func WithClientMode(m string) ClientOption {
	return func(c *HTTP3Client) { c.Mode = m }
}

// WithClientID sets the client ID sent in the query string.
func WithClientID(id string) ClientOption {
	return func(c *HTTP3Client) { c.clientID = id }
}

// NewHTTP3Client creates a client connecting to serverAddr (e.g., "localhost:4433").
func NewHTTP3Client(serverAddr string, opts ...ClientOption) *HTTP3Client {
	c := &HTTP3Client{
		serverAddr: serverAddr,
		Mode:       "stream",
	}
	c.SetProtocol("http3-client")
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = zap.NewNop()
	}
	if c.metrics == nil {
		c.metrics = metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "http3/client",
			Protocol: "http3",
			Scenario: "default",
		})
	}
	if c.clientID == "" {
		c.clientID = fmt.Sprintf("client-%d", time.Now().UnixNano())
	}
	return c
}

// Start begins connecting to the server and reading frames.
func (c *HTTP3Client) Start() error {
	if c.IsStarted() {
		return transport.ErrAlreadyStarted
	}

	tlsCfg := tlsinternal.ClientTLSConfig(http3.NextProtoH3)

	c.roundTripper = &http3.Transport{
		TLSClientConfig: tlsCfg,
		QUICConfig: &quic.Config{
			EnableDatagrams: true,
		},
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.MarkStarted()
	c.metrics.Start()

	c.wg.Add(1)
	go c.connectLoop()
	return nil
}

// Stop terminates the client connection.
func (c *HTTP3Client) Stop() error {
	if !c.IsStarted() {
		return transport.ErrNotStarted
	}
	c.cancel()
	c.wg.Wait()
	if c.roundTripper != nil {
		_ = c.roundTripper.Close()
	}
	c.metrics.Stop()
	c.MarkStopped()
	return nil
}

// Broadcast is not implemented on the client side.
func (c *HTTP3Client) Broadcast(_ []byte) error { return nil }

// Send is not implemented on the client side.
func (c *HTTP3Client) Send(_ transport.ConnID, _ []byte) error { return nil }

// Connections returns 1 if connected, 0 otherwise.
func (c *HTTP3Client) Connections() int {
	if c.IsStarted() {
		return 1
	}
	return 0
}

// Stats returns a point-in-time performance snapshot.
func (c *HTTP3Client) Stats() transport.Stats {
	snap := c.metrics.Snapshot()
	base := c.BaseStats()
	lat := snap.Latency
	return transport.Stats{
		Protocol:      base.Protocol,
		Connections:   c.Connections(),
		Received:      snap.MsgRecv,
		Lost:          snap.Seq.Lost + c.lostCounter.Load(),
		BytesRecv:     snap.BytesRecv,
		MinLatencyNs:  lat.Min.Nanoseconds(),
		AvgLatencyNs:  lat.Mean.Nanoseconds(),
		P50LatencyNs:  lat.P50.Nanoseconds(),
		P95LatencyNs:  lat.P95.Nanoseconds(),
		P99LatencyNs:  lat.P99.Nanoseconds(),
		P999LatencyNs: lat.P999.Nanoseconds(),
		MaxLatencyNs:  lat.Max.Nanoseconds(),
		Uptime:        base.Uptime,
		SnapshotAt:    base.SnapshotAt,
	}
}

// connectLoop maintains the connection with exponential backoff on failure.
func (c *HTTP3Client) connectLoop() {
	defer c.wg.Done()

	backoff := h3ClientInitialBackoff
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		err := c.connect()
		if err == nil {
			backoff = h3ClientInitialBackoff
			continue
		}

		c.logger.Warn("http3 client: connection failed",
			zap.Error(err),
			zap.Duration("backoff", backoff))

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > h3ClientMaxBackoff {
			backoff = h3ClientMaxBackoff
		}
	}
}

// connect performs a single connection attempt and reads until error.
func (c *HTTP3Client) connect() error {
	url := fmt.Sprintf("https://%s/stream?id=%s", c.serverAddr, c.clientID)

	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.roundTripper.RoundTrip(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http3 client: unexpected status %d", resp.StatusCode)
	}

	return c.readLoop(resp.Body)
}

// readLoop reads 4-byte length-prefixed wire frames from the response body.
func (c *HTTP3Client) readLoop(r io.Reader) error {
	lenBuf := make([]byte, h3LenBytes)
	payloadBuf := make([]byte, h3ClientReadBuf)

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		if _, err := io.ReadFull(r, lenBuf); err != nil {
			return err
		}

		frameLen := binary.BigEndian.Uint32(lenBuf)
		if frameLen == 0 || frameLen > wire.MaxPayload+wire.HeaderLen {
			return fmt.Errorf("http3 client: invalid frame length %d", frameLen)
		}

		if int(frameLen) > len(payloadBuf) {
			payloadBuf = make([]byte, frameLen)
		}
		data := payloadBuf[:frameLen]

		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		recvAt := time.Now()

		frame, _, err := wire.Decode(data)
		if err != nil {
			c.logger.Debug("http3 client: decode error", zap.Error(err))
			continue
		}

		c.metrics.RecordRecv(frame.SeqNum, frame.SendNs, int(frameLen), recvAt.UnixNano())

		if c.onRecv != nil {
			cp := make([]byte, frameLen)
			copy(cp, data)
			c.onRecv(transport.ConnID(c.clientID), cp, recvAt)
		}
	}
}
