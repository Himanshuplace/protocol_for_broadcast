package http1

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

// HTTP1Client connects to an HTTP1Server and reads its chunked stream.
type HTTP1Client struct {
	addr    string
	connID  transport.ConnID
	handler transport.RecvHandler
	logger  *zap.Logger

	httpClient *http.Client
	cancel     context.CancelFunc
}

// NewHTTP1Client creates an HTTP/1.1 streaming client.
func NewHTTP1Client(addr string, handler transport.RecvHandler, logger *zap.Logger) *HTTP1Client {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &HTTP1Client{
		addr:       addr,
		handler:    handler,
		logger:     logger,
		httpClient: &http.Client{Timeout: 0}, // no timeout; stream is long-lived
	}
}

// Connect registers with the server, then starts reading the stream in the background.
// Blocks until the registration succeeds.
func (c *HTTP1Client) Connect(ctx context.Context) error {
	// Step 1: register to obtain a ConnID.
	regURL := "http://" + c.addr + "/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, regURL, nil)
	if err != nil {
		return fmt.Errorf("http1 client: build register request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http1 client: register: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http1 client: register status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("http1 client: read register body: %w", err)
	}
	c.connID = transport.ConnID(string(body))
	c.logger.Info("http1 client registered", zap.String("id", string(c.connID)))

	// Step 2: open the stream.
	streamCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	go c.readStream(streamCtx)
	return nil
}

// readStream opens GET /stream?id=<connID> and reads framed messages.
func (c *HTTP1Client) readStream(ctx context.Context) {
	url := "http://" + c.addr + "/stream?id=" + string(c.connID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.logger.Error("http1 client: build stream request", zap.Error(err))
		return
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error("http1 client: stream connect", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Error("http1 client: stream bad status",
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
				c.logger.Debug("http1 client: read length", zap.Error(err))
			}
			return
		}
		msgLen := binary.BigEndian.Uint32(lenBuf[:])
		if msgLen == 0 || msgLen > 1<<20 {
			c.logger.Error("http1 client: invalid message length",
				zap.Uint32("len", msgLen))
			return
		}

		// Read the message payload.
		data := make([]byte, msgLen)
		if _, err := io.ReadFull(br, data); err != nil {
			select {
			case <-ctx.Done():
			default:
				c.logger.Debug("http1 client: read payload", zap.Error(err))
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
func (c *HTTP1Client) ConnID() transport.ConnID { return c.connID }

// Close cancels the stream reader.
func (c *HTTP1Client) Close() {
	if c.cancel != nil {
		c.cancel()
	}
}
