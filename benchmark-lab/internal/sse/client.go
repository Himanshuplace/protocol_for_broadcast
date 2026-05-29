package sse

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// SSEClient connects to an SSEServer and receives event-stream messages.
type SSEClient struct {
	serverAddr string
	connID     transport.ConnID
	handler    transport.RecvHandler
	recorder   *metrics.Recorder
	logger     *zap.Logger
	httpClient *http.Client

	lastEventID atomic.Uint64
	received    atomic.Uint64
	recvBytes   atomic.Uint64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSSEClient creates a new SSE client.
func NewSSEClient(serverAddr string, handler transport.RecvHandler, rec *metrics.Recorder, logger *zap.Logger) *SSEClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SSEClient{
		serverAddr: serverAddr,
		handler:    handler,
		recorder:   rec,
		logger:     logger,
		httpClient: &http.Client{Timeout: 0}, // no timeout on stream
	}
}

// Connect registers with the server and starts receiving events.
func (c *SSEClient) Connect(ctx context.Context) error {
	// Register to get a connID
	resp, err := c.httpClient.Post("http://"+c.serverAddr+"/register", "text/plain", nil)
	if err != nil {
		return fmt.Errorf("sse client: register: %w", err)
	}
	defer resp.Body.Close()
	var idBuf [64]byte
	n, _ := resp.Body.Read(idBuf[:])
	c.connID = transport.ConnID(idBuf[:n])

	ctx2, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.wg.Add(1)
	go c.receiveLoop(ctx2)
	return nil
}

func (c *SSEClient) receiveLoop(ctx context.Context) {
	defer c.wg.Done()
	backoff := 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := c.connectStream(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Debug("sse: stream error, reconnecting", zap.Error(err), zap.Duration("backoff", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
		} else {
			backoff = 100 * time.Millisecond
		}
	}
}

func (c *SSEClient) connectStream(ctx context.Context) error {
	url := fmt.Sprintf("http://%s/events?id=%s", c.serverAddr, c.connID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	lastID := c.lastEventID.Load()
	if lastID > 0 {
		req.Header.Set("Last-Event-ID", fmt.Sprintf("%d", lastID))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	var currentID uint64
	var currentData string

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line := scanner.Text()
		if line == "" {
			// empty line = dispatch event
			if currentData != "" && currentID > 0 {
				c.processEvent(currentID, currentData)
				currentData = ""
				currentID = 0
			}
			continue
		}

		if strings.HasPrefix(line, "id: ") {
			fmt.Sscanf(line[4:], "%d", &currentID)
			c.lastEventID.Store(currentID)
		} else if strings.HasPrefix(line, "data: ") {
			currentData = line[6:]
		}
	}
	return scanner.Err()
}

func (c *SSEClient) processEvent(id uint64, data string) {
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		c.logger.Debug("sse client: base64 decode failed", zap.Error(err))
		return
	}
	recvAt := time.Now()
	c.received.Add(1)
	c.recvBytes.Add(uint64(len(raw)))

	frame, _, err := wire.Decode(raw)
	if err == nil && c.recorder != nil {
		c.recorder.RecordRecv(frame.SeqNum, frame.SendNs, len(raw), recvAt.UnixNano())
	}
	if c.handler != nil {
		c.handler(c.connID, raw, recvAt)
	}
}

// Close stops the client.
func (c *SSEClient) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// ConnID returns the connection identifier assigned by the server.
func (c *SSEClient) ConnID() transport.ConnID { return c.connID }
