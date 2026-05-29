package webrtc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pion/webrtc/v3"
	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// WebRTCClient connects to a WebRTCServer via its signaling endpoint.
type WebRTCClient struct {
	signalingAddr string
	mode          ChannelMode
	handler       transport.RecvHandler
	recorder      *metrics.Recorder
	logger        *zap.Logger

	pc     *webrtc.PeerConnection
	dc     *webrtc.DataChannel
	connID string

	ready chan struct{}
	mu    sync.Mutex
}

// NewWebRTCClient creates a WebRTC client.
func NewWebRTCClient(signalingAddr string, mode ChannelMode, handler transport.RecvHandler, rec *metrics.Recorder, logger *zap.Logger) *WebRTCClient {
	if logger == nil {
		logger = zap.NewNop()
	}
	if mode == "" {
		mode = ModeReliable
	}
	return &WebRTCClient{
		signalingAddr: signalingAddr,
		mode:          mode,
		handler:       handler,
		recorder:      rec,
		logger:        logger,
		ready:         make(chan struct{}),
	}
}

// Connect performs the WebRTC signaling handshake and waits for data channel open.
func (c *WebRTCClient) Connect(ctx context.Context) error {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("webrtc client: new pc: %w", err)
	}
	c.pc = pc

	// Set up DataChannel receive handler
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		c.mu.Lock()
		c.dc = dc
		c.mu.Unlock()
		dc.OnOpen(func() {
			select {
			case <-c.ready:
			default:
				close(c.ready)
			}
		})
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			recvAt := time.Now()
			frame, _, ferr := wire.Decode(msg.Data)
			if ferr == nil && c.recorder != nil {
				c.recorder.RecordRecv(frame.SeqNum, frame.SendNs, len(msg.Data), recvAt.UnixNano())
			}
			if c.handler != nil {
				c.handler(transport.ConnID(c.connID), msg.Data, recvAt)
			}
		})
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("webrtc client: create offer: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("webrtc client: set local desc: %w", err)
	}
	<-gatherComplete

	// POST offer to signaling server
	body, _ := json.Marshal(offerRequest{SDP: pc.LocalDescription().SDP, Type: "offer"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+c.signalingAddr+"/webrtc/offer", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("webrtc client: post offer: %w", err)
	}
	defer resp.Body.Close()

	var ans answerResponse
	if err := json.NewDecoder(resp.Body).Decode(&ans); err != nil {
		return fmt.Errorf("webrtc client: decode answer: %w", err)
	}
	c.connID = ans.ID

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  ans.SDP,
	}); err != nil {
		return fmt.Errorf("webrtc client: set remote desc: %w", err)
	}

	// Wait for data channel to open
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ready:
	case <-time.After(30 * time.Second):
		return fmt.Errorf("webrtc client: timeout waiting for data channel")
	}
	return nil
}

// Send sends data to the server via the DataChannel.
func (c *WebRTCClient) Send(data []byte) error {
	c.mu.Lock()
	dc := c.dc
	c.mu.Unlock()
	if dc == nil {
		return fmt.Errorf("webrtc client: not connected")
	}
	return dc.Send(data)
}

// Close closes the PeerConnection.
func (c *WebRTCClient) Close() error {
	if c.pc != nil {
		return c.pc.Close()
	}
	return nil
}

// ConnID returns the connection identifier from the signaling server.
func (c *WebRTCClient) ConnID() string { return c.connID }
