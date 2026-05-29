package tcp

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// TCPClient connects to a TCPServer and exchanges length-prefixed wire frames.
type TCPClient struct {
	conn     *net.TCPConn
	writeCh  chan []byte
	recorder *metrics.Recorder
	logger   *zap.Logger
	handler  transport.RecvHandler

	// seqCounter is the per-client monotonically increasing send sequence number.
	seqCounter atomic.Uint64

	// done signals background goroutines to stop.
	done chan struct{}
	wg   sync.WaitGroup
}

// TCPClientOption is a functional option for TCPClient.
type TCPClientOption func(*TCPClient)

// WithTCPClientLogger attaches a zap logger to the client.
func WithTCPClientLogger(l *zap.Logger) TCPClientOption {
	return func(c *TCPClient) { c.logger = l }
}

// WithTCPClientRecorder attaches a metrics recorder to the client.
func WithTCPClientRecorder(r *metrics.Recorder) TCPClientOption {
	return func(c *TCPClient) { c.recorder = r }
}

// WithTCPClientRecvHandler sets the callback invoked for each received frame.
func WithTCPClientRecvHandler(h transport.RecvHandler) TCPClientOption {
	return func(c *TCPClient) { c.handler = h }
}

// NewTCPClient creates a new TCPClient. Call Dial() to connect.
func NewTCPClient(opts ...TCPClientOption) *TCPClient {
	c := &TCPClient{
		writeCh: make(chan []byte, tcpWriteChanCap),
		done:    make(chan struct{}),
	}
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = zap.NewNop()
	}
	if c.recorder == nil {
		c.recorder = metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "tcp/client",
			Protocol: "tcp",
			Scenario: "default",
		})
	}
	return c
}

// Dial connects to addr (e.g., "127.0.0.1:9001"), sets TCP_NODELAY, and starts
// the read/write pump goroutines.
func (c *TCPClient) Dial(addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp client: resolve %q: %w", addr, err)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return fmt.Errorf("tcp client: dial %q: %w", addr, err)
	}

	// Enable TCP_NODELAY for lowest-latency sends.
	_ = conn.SetNoDelay(true)
	_ = conn.SetReadBuffer(tcpReadBufSize)
	_ = conn.SetWriteBuffer(tcpWriteBufSize)

	c.conn = conn

	// Reset done channel in case Dial is called after Close.
	c.done = make(chan struct{})
	c.recorder.Start()

	c.wg.Add(2)
	go c.readPump()
	go c.writePump()

	c.logger.Info("tcp client connected", zap.String("remote", addr))
	return nil
}

// Send encodes data as a length-prefixed wire frame and enqueues it for sending.
// Returns immediately; actual send happens in the write pump goroutine.
// Returns an error if the write channel is full (non-blocking).
func (c *TCPClient) Send(data []byte) error {
	if c.conn == nil {
		return fmt.Errorf("tcp client: not connected")
	}
	seq := c.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	framed := prependLength(frame)

	select {
	case c.writeCh <- framed:
		c.recorder.RecordSend(seq, len(framed))
		return nil
	default:
		return fmt.Errorf("tcp client: write channel full")
	}
}

// SendSync encodes data as a length-prefixed wire frame and writes it directly
// to the connection (blocking). Use for low-volume control messages.
func (c *TCPClient) SendSync(data []byte) error {
	if c.conn == nil {
		return fmt.Errorf("tcp client: not connected")
	}
	seq := c.seqCounter.Add(1)
	frame := wire.Encode(seq, time.Now().UnixNano(), data)
	framed := prependLength(frame)
	if _, err := c.conn.Write(framed); err != nil {
		return fmt.Errorf("tcp client: write: %w", err)
	}
	c.recorder.RecordSend(seq, len(framed))
	return nil
}

// Close shuts down the client, stopping all goroutines and closing the TCP connection.
func (c *TCPClient) Close() error {
	select {
	case <-c.done:
		return nil // already closed
	default:
	}
	close(c.done)
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.wg.Wait()
	c.recorder.Stop()
	c.logger.Info("tcp client closed")
	return nil
}

// readPump reads length-prefixed frames from the server and dispatches them to
// the registered handler.
func (c *TCPClient) readPump() {
	defer c.wg.Done()

	lenBuf := make([]byte, tcpLenBytes)
	payloadBuf := make([]byte, tcpReadBufSize)

	for {
		select {
		case <-c.done:
			return
		default:
		}

		// Read the 4-byte length prefix.
		if _, err := io.ReadFull(c.conn, lenBuf); err != nil {
			select {
			case <-c.done:
			default:
				c.logger.Debug("tcp client: read length error", zap.Error(err))
			}
			return
		}

		frameLen := binary.BigEndian.Uint32(lenBuf)
		if frameLen == 0 || frameLen > tcpMaxFrameSize {
			c.logger.Warn("tcp client: invalid frame length", zap.Uint32("len", frameLen))
			return
		}

		// Grow buffer if needed.
		if int(frameLen) > len(payloadBuf) {
			payloadBuf = make([]byte, frameLen)
		}
		payload := payloadBuf[:frameLen]

		if _, err := io.ReadFull(c.conn, payload); err != nil {
			select {
			case <-c.done:
			default:
				c.logger.Debug("tcp client: read payload error", zap.Error(err))
			}
			return
		}
		recvAt := time.Now()

		if len(payload) < wire.HeaderLen {
			continue
		}
		frame, _, err := wire.Decode(payload)
		if err != nil {
			c.logger.Debug("tcp client: decode error", zap.Error(err))
			continue
		}

		c.recorder.RecordRecv(frame.SeqNum, frame.SendNs, int(frameLen), recvAt.UnixNano())

		if c.handler != nil {
			connID := transport.ConnID("tcp-" + c.conn.RemoteAddr().String())
			c.handler(connID, payload, recvAt)
		}
	}
}

// writePump drains the write channel and sends frames to the server.
func (c *TCPClient) writePump() {
	defer c.wg.Done()
	for {
		select {
		case <-c.done:
			return
		case data, ok := <-c.writeCh:
			if !ok {
				return
			}
			if _, err := c.conn.Write(data); err != nil {
				select {
				case <-c.done:
				default:
					c.logger.Debug("tcp client: write error", zap.Error(err))
				}
				return
			}
		}
	}
}
