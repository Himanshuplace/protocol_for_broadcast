package http2

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

// HTTP2Client connects to an HTTP2Server using unencrypted HTTP/2 (h2c).
//
// Transport configuration:
//   - net/http.Transport with Protocols.SetUnencryptedHTTP2(true) instructs the
//     stdlib HTTP client to attempt HTTP/2 over plain TCP for http:// URLs.
//   - No TLS, no ALPN negotiation — the connection speaks HTTP/2 immediately.
type HTTP2Client struct {
	addr    string
	connID  transport.ConnID
	handler transport.RecvHandler
	logger  *zap.Logger

	httpClient *http.Client
	cancel     context.CancelFunc
}

// NewHTTP2Client creates an HTTP/2 streaming client.
func NewHTTP2Client(addr string, handler transport.RecvHandler, logger *zap.Logger) *HTTP2Client {
	if logger == nil {
		logger = zap.NewNop()
	}

	// Build a transport that speaks unencrypted HTTP/2 (h2c) for http:// URLs.
	protos := new(http.Protocols)
	protos.SetUnencryptedHTTP2(true)

	tr := &http.Transport{
		Protocols: protos,
	}

	return &HTTP2Client{
		addr:       addr,
		handler:    handler,
		logger:     logger,
		httpClient: &http.Client{Transport: tr, Timeout: 0},
	}
}

// Connect registers with the server and starts the background stream reader.
func (c *HTTP2Client) Connect(ctx context.Context) error {
	// Step 1: POST /register to get our ConnID.
	regURL := "http://" + c.addr + "/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, regURL, nil)
	if err != nil {
		return fmt.Errorf("http2 client: build register request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http2 client: register: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http2 client: register status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("http2 client: read register body: %w", err)
	}
	c.connID = transport.ConnID(string(body))
	c.logger.Info("http2 client registered", zap.String("id", string(c.connID)))

	// Step 2: start the long-lived GET /stream.
	streamCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go c.readStream(streamCtx)
	return nil
}

// readStream opens GET /stream?id=<connID> over HTTP/2 and reads framed messages.
func (c *HTTP2Client) readStream(ctx context.Context) {
	url := "http://" + c.addr + "/stream?id=" + string(c.connID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error("http2 client: build stream request", zap.Error(err))
		return
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
		default:
			c.logger.Error("http2 client: stream connect", zap.Error(err))
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Error("http2 client: stream bad status",
			zap.Int("status", resp.StatusCode))
		return
	}

	br := bufio.NewReaderSize(resp.Body, 65536)
	var lenBuf [4]byte

	for {
		// Read the 4-byte big-endian length prefix.
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			select {
			case <-ctx.Done():
				// Normal cancellation.
			default:
				c.logger.Debug("http2 client: read length", zap.Error(err))
			}
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		if msgLen == 0 || msgLen > 1<<20 {
			c.logger.Error("http2 client: invalid message length",
				zap.Uint32("len", msgLen))
			return
		}

		data := make([]byte, msgLen)
		if _, err := io.ReadFull(br, data); err != nil {
			select {
			case <-ctx.Done():
			default:
				c.logger.Debug("http2 client: read payload", zap.Error(err))
			}
			return
		}
		recvAt := time.Now()

		if c.handler != nil {
			c.handler(c.connID, data, recvAt)
		}
	}
}

// ConnID returns the assigned connection ID (valid after Connect).
func (c *HTTP2Client) ConnID() transport.ConnID { return c.connID }

// Close cancels the stream reader and closes idle connections.
func (c *HTTP2Client) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	if tr, ok := c.httpClient.Transport.(*http.Transport); ok {
		tr.CloseIdleConnections()
	}
}
