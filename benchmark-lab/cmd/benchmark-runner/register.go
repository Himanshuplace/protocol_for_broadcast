// register.go wires every transport's server factory and client factory into
// the scenario runner. It is a separate file so main.go stays focused on CLI logic.
// The init() function runs once at process startup, before cobra parses any flags.
package main

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/http1"
	"github.com/himanshuplace/protocol_for_broadcast/internal/http2"
	"github.com/himanshuplace/protocol_for_broadcast/internal/http3"
	"github.com/himanshuplace/protocol_for_broadcast/internal/scenarios"
	"github.com/himanshuplace/protocol_for_broadcast/internal/sse"
	"github.com/himanshuplace/protocol_for_broadcast/internal/tcp"
	"github.com/himanshuplace/protocol_for_broadcast/internal/udp"
	"github.com/himanshuplace/protocol_for_broadcast/internal/webrtc"
	"github.com/himanshuplace/protocol_for_broadcast/internal/webtransport"
	wscoder "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/coder"
	wsgobwas "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gobwas"
	wsgorilla "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gorilla"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func init() {
	registerUDP()
	registerTCP()
	registerWebSocketGorilla()
	registerWebSocketGobwas()
	registerWebSocketCoder()
	registerHTTP1()
	registerHTTP2()
	registerHTTP3()
	registerSSE()
	registerWebTransport()
	registerWebRTC()
}

// ── UDP ──────────────────────────────────────────────────────────────────────

func registerUDP() {
	scenarios.Register("udp", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := udp.NewUDPServer(addr,
			udp.WithServerLogger(logger),
		)
		return srv, nil
	})

	scenarios.RegisterClient("udp", func(
		_ context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := udp.NewUDPClient(
			udp.WithClientRecvHandler(handler),
			udp.WithClientLogger(logger),
		)
		if err := c.Dial(serverAddr); err != nil {
			return nil, fmt.Errorf("udp client[%d]: %w", idx, err)
		}
		return func() { _ = c.Close() }, nil
	})
}

// ── TCP ──────────────────────────────────────────────────────────────────────

func registerTCP() {
	scenarios.Register("tcp", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := tcp.NewTCPServer(addr,
			tcp.WithTCPServerLogger(logger),
		)
		return srv, nil
	})

	scenarios.RegisterClient("tcp", func(
		_ context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := tcp.NewTCPClient(
			tcp.WithTCPClientRecvHandler(handler),
			tcp.WithTCPClientLogger(logger),
		)
		if err := c.Dial(serverAddr); err != nil {
			return nil, fmt.Errorf("tcp client[%d]: %w", idx, err)
		}
		return func() { _ = c.Close() }, nil
	})
}

// ── WebSocket / gorilla ───────────────────────────────────────────────────────

func registerWebSocketGorilla() {
	scenarios.Register("websocket-gorilla", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		tcfg := transport.TransportConfig{ListenAddr: addr}
		return wsgorilla.NewGorillaServer(tcfg, logger), nil
	})

	scenarios.RegisterClient("websocket-gorilla", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := wsgorilla.NewGorillaClient(serverAddr, handler, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("ws-gorilla client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── WebSocket / gobwas ────────────────────────────────────────────────────────

func registerWebSocketGobwas() {
	scenarios.Register("websocket-gobwas", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		tcfg := transport.TransportConfig{ListenAddr: addr}
		return wsgobwas.NewGobwasServer(tcfg, logger), nil
	})

	scenarios.RegisterClient("websocket-gobwas", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := wsgobwas.NewGobwasClient(serverAddr, handler, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("ws-gobwas client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── WebSocket / coder ─────────────────────────────────────────────────────────

func registerWebSocketCoder() {
	scenarios.Register("websocket-coder", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		tcfg := transport.TransportConfig{ListenAddr: addr}
		return wscoder.NewCoderServer(tcfg, logger), nil
	})

	scenarios.RegisterClient("websocket-coder", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := wscoder.NewCoderClient(serverAddr, handler, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("ws-coder client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── HTTP/1.1 ─────────────────────────────────────────────────────────────────

func registerHTTP1() {
	scenarios.Register("http1", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		tcfg := transport.TransportConfig{ListenAddr: addr}
		return http1.NewHTTP1Server(tcfg, logger), nil
	})

	scenarios.RegisterClient("http1", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := http1.NewHTTP1Client(serverAddr, handler, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("http1 client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── HTTP/2 (h2c) ─────────────────────────────────────────────────────────────

func registerHTTP2() {
	scenarios.Register("http2", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		tcfg := transport.TransportConfig{ListenAddr: addr}
		return http2.NewHTTP2Server(tcfg, logger), nil
	})

	scenarios.RegisterClient("http2", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := http2.NewHTTP2Client(serverAddr, handler, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("http2 client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── HTTP/3 + QUIC ─────────────────────────────────────────────────────────────

func registerHTTP3() {
	// HTTP/3 streaming mode (default)
	scenarios.Register("http3", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := http3.NewHTTP3Server(addr,
			http3.WithLogger(logger),
			http3.WithMode("stream"),
		)
		return srv, nil
	})

	scenarios.RegisterClient("http3", func(
		_ context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := http3.NewHTTP3Client(serverAddr,
			http3.WithClientLogger(logger),
			http3.WithClientRecorder(rec),
			http3.WithClientRecvHandler(handler),
			http3.WithClientMode("stream"),
			http3.WithClientID(fmt.Sprintf("client-%d", idx)),
		)
		if err := c.Start(); err != nil {
			return nil, fmt.Errorf("http3 client[%d]: %w", idx, err)
		}
		return func() { _ = c.Stop() }, nil
	})

	// QUIC unidirectional streams
	scenarios.Register("http3-unidirstream", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := http3.NewHTTP3Server(addr,
			http3.WithLogger(logger),
			http3.WithMode("unidirstream"),
		)
		return srv, nil
	})

	scenarios.RegisterClient("http3-unidirstream", func(
		_ context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := http3.NewHTTP3Client(serverAddr,
			http3.WithClientLogger(logger),
			http3.WithClientRecorder(rec),
			http3.WithClientRecvHandler(handler),
			http3.WithClientMode("unidirstream"),
			http3.WithClientID(fmt.Sprintf("client-%d", idx)),
		)
		if err := c.Start(); err != nil {
			return nil, fmt.Errorf("http3-unidirstream client[%d]: %w", idx, err)
		}
		return func() { _ = c.Stop() }, nil
	})

	// QUIC datagrams (unreliable, lowest latency)
	scenarios.Register("http3-datagram", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		srv := http3.NewHTTP3Server(addr,
			http3.WithLogger(logger),
			http3.WithMode("datagram"),
		)
		return srv, nil
	})

	scenarios.RegisterClient("http3-datagram", func(
		_ context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		c := http3.NewHTTP3Client(serverAddr,
			http3.WithClientLogger(logger),
			http3.WithClientRecorder(rec),
			http3.WithClientRecvHandler(handler),
			http3.WithClientMode("datagram"),
			http3.WithClientID(fmt.Sprintf("client-%d", idx)),
		)
		if err := c.Start(); err != nil {
			return nil, fmt.Errorf("http3-datagram client[%d]: %w", idx, err)
		}
		return func() { _ = c.Stop() }, nil
	})
}

// ── SSE ──────────────────────────────────────────────────────────────────────

func registerSSE() {
	scenarios.Register("sse", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		rec := metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "sse/server",
			Protocol: "sse",
			Scenario: cfg.Scenario,
		})
		return sse.NewSSEServer(addr, rec, logger), nil
	})

	scenarios.RegisterClient("sse", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		clientRec := metrics.NewRecorder(metrics.RecorderConfig{
			Label:    fmt.Sprintf("sse/client/%d", idx),
			Protocol: "sse",
			Scenario: cfg.Scenario,
		})
		c := sse.NewSSEClient(serverAddr, handler, clientRec, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("sse client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── WebTransport ─────────────────────────────────────────────────────────────

func registerWebTransport() {
	scenarios.Register("webtransport", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		addr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		rec := metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "webtransport/server",
			Protocol: "webtransport",
			Scenario: cfg.Scenario,
		})
		srv, err := webtransport.NewWebTransportServer(addr, webtransport.ModeUniStream, rec, logger)
		if err != nil {
			return nil, err
		}
		return srv, nil
	})

	scenarios.RegisterClient("webtransport", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		clientRec := metrics.NewRecorder(metrics.RecorderConfig{
			Label:    fmt.Sprintf("webtransport/client/%d", idx),
			Protocol: "webtransport",
			Scenario: cfg.Scenario,
		})
		c := webtransport.NewWebTransportClient(serverAddr, webtransport.ModeUniStream, handler, clientRec, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("webtransport client[%d]: %w", idx, err)
		}
		return func() { c.Close() }, nil
	})
}

// ── WebRTC DataChannels ───────────────────────────────────────────────────────

func registerWebRTC() {
	scenarios.Register("webrtc", func(cfg scenarios.ScenarioConfig, logger *zap.Logger) (transport.Transport, error) {
		// The WebRTC signaling server binds to serverPort.
		// The benchmark port (serverPort+1) handles media data.
		sigAddr := fmt.Sprintf("%s:%d", cfg.ServerAddr, cfg.ServerPort)
		rec := metrics.NewRecorder(metrics.RecorderConfig{
			Label:    "webrtc/server",
			Protocol: "webrtc",
			Scenario: cfg.Scenario,
		})
		return webrtc.NewWebRTCServer(sigAddr, webrtc.ModeReliable, rec, logger), nil
	})

	scenarios.RegisterClient("webrtc", func(
		ctx context.Context, cfg scenarios.ScenarioConfig, idx int,
		serverAddr string, rec *metrics.Recorder, logger *zap.Logger,
	) (func(), error) {
		handler := scenarios.DefaultRecvHandler(rec)
		clientRec := metrics.NewRecorder(metrics.RecorderConfig{
			Label:    fmt.Sprintf("webrtc/client/%d", idx),
			Protocol: "webrtc",
			Scenario: cfg.Scenario,
		})
		c := webrtc.NewWebRTCClient(serverAddr, webrtc.ModeReliable, handler, clientRec, logger)
		if err := c.Connect(ctx); err != nil {
			return nil, fmt.Errorf("webrtc client[%d]: %w", idx, err)
		}
		return func() { _ = c.Close() }, nil
	})
}
