// Package udp provides UDP transport implementations for the benchmark platform.
package udp

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

const (
	heartbeatInterval = 5 * time.Second
	clientBufSize     = maxUDPSize
)

// UDPClient is a UDP client that connects to a UDPServer.
// It sends a HELLO registration packet on connect, then sends heartbeats every
// heartbeatInterval to keep peer registration alive on the server.
type UDPClient struct {
	conn     *net.UDPConn
	recorder *metrics.Recorder
	logger   *zap.Logger
	handler  transport.RecvHandler

	// seqCounter is the per-client monotonically increasing send sequence number.
	seqCounter atomic.Uint64

	// done signals the background goroutines to stop.
	done chan struct{}
	wg   sync.WaitGroup
}

// ClientOption is a functional option for UDPClient.
type ClientOption func(*UDPClient)

// WithClientLogger attaches a zap logger to the client.
func WithClientLogger(l *zap.Logger) ClientOption {
	return func(c *UDPClient) { c.logger = l }
}

// WithClientRecorder attaches a metrics recorder to the client.
func WithClientRecorder(r *metrics.Recorder) ClientOption {
	return func(c *UDPClient) { c.recorder = r }
}

// WithClientRecvHandler sets the callback invoked for each received frame.
func WithClientRecvHandler(h transport.RecvHandler) ClientOption {
	return func(c *UDPClient) { c.handler = h }
}

// NewUDPClient creates a new UDPClient with the provided options.
// Call Dial() to connect to a server.
func NewUDPClient(opts ...ClientOption) *UDPClient {
	c := &UDPClient{
		done: make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = zap.NewNop()
	}
	if c.recorder == nil {
		c.recorder = metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "udp/client",
			Protocol: "udp",
			Scenario: "default",
		})
	}
	return c
}

// Dial connects to the UDP server at addr (e.g., "127.0.0.1:9000"), sends a
// HELLO registration packet, and starts the receive and heartbeat goroutines.
func (c *UDPClient) Dial(addr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("udp client: resolve %q: %w", addr, err)
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("udp client: dial %q: %w", addr, err)
	}
	c.conn = conn

	// Enlarge socket buffers to match server settings.
	if err := setSocketBuf(conn, recvBufSize); err != nil {
		c.logger.Warn("udp client: setSocketBuf failed", zap.Error(err))
	}

	// Send HELLO registration packet (below wire.HeaderLen so server treats it as heartbeat).
	if _, err := conn.Write([]byte("HELLO")); err != nil {
		_ = conn.Close()
		return fmt.Errorf("udp client: send HELLO: %w", err)
	}

	c.recorder.Start()

	// Reset done channel in case Dial is called after a previous Close.
	c.done = make(chan struct{})

	c.wg.Add(2)
	go c.receiveLoop()
	go c.heartbeatLoop()

	c.logger.Info("udp client connected", zap.String("remote", addr))
	return nil
}

// Send encodes data as a wire frame and writes it to the server.
func (c *UDPClient) Send(data []byte) error {
	if c.conn == nil {
		return fmt.Errorf("udp client: not connected")
	}
	seq := c.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	_, err := c.conn.Write(frame)
	if err != nil {
		return fmt.Errorf("udp client: send: %w", err)
	}
	c.recorder.RecordSend(seq, len(frame))
	return nil
}

// Close shuts down the client, stopping all goroutines and closing the socket.
func (c *UDPClient) Close() error {
	select {
	case <-c.done:
		// Already closed.
		return nil
	default:
	}
	close(c.done)
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.wg.Wait()
	c.recorder.Stop()
	c.logger.Info("udp client closed")
	return nil
}

// receiveLoop reads incoming datagrams from the server and dispatches them to
// the registered handler after recording receive metrics.
func (c *UDPClient) receiveLoop() {
	defer c.wg.Done()
	buf := make([]byte, clientBufSize)

	for {
		select {
		case <-c.done:
			return
		default:
		}

		_ = c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := c.conn.Read(buf)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			select {
			case <-c.done:
				return
			default:
				c.logger.Warn("udp client: read error", zap.Error(err))
				continue
			}
		}
		recvAt := time.Now()

		if n < wire.HeaderLen {
			// Heartbeat / control packet — ignore.
			continue
		}

		frame, _, err := wire.Decode(buf[:n])
		if err != nil {
			c.logger.Debug("udp client: decode error", zap.Error(err))
			continue
		}

		c.recorder.RecordRecv(frame.SeqNum, frame.SendNs, n, recvAt.UnixNano())

		if c.handler != nil {
			// ConnID is the client's own local address — unique per client socket,
			// so an aggregating recorder can track each subscriber independently.
			connID := transport.ConnID("udp-" + c.conn.LocalAddr().String())
			c.handler(connID, buf[:n], recvAt)
		}
	}
}

// heartbeatLoop sends an empty packet every heartbeatInterval to keep the peer
// registration alive on the server side.
func (c *UDPClient) heartbeatLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if _, err := c.conn.Write([]byte("HB")); err != nil {
				select {
				case <-c.done:
					return
				default:
					c.logger.Debug("udp client: heartbeat error", zap.Error(err))
				}
			}
		}
	}
}
