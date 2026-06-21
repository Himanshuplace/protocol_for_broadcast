// websocket-server starts a WebSocket benchmark server (gorilla implementation)
// and actively broadcasts wire-framed market data to every connected subscriber.
//
// This is the target for k6 validation: start this server, point k6 at ws://host/ws,
// and k6 receives the same wire-framed broadcast stream the Go harness measures.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gorilla"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/generator"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9000", "listen address")
	rateLimit := flag.Int("rate", 10000, "broadcast rate in msgs/sec (0 = flood as fast as possible)")
	msgSize := flag.Int("msg-size", 1024, "payload size in bytes (wire header adds 24 bytes)")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	srv := gorilla.NewGorillaServer(transport.TransportConfig{ListenAddr: *addr}, logger)
	if err := srv.Start(); err != nil {
		logger.Fatal("start", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Publisher: generate payloads and broadcast at the configured rate.
	// The server's Broadcast() wire-encodes each frame (magic + seq + sendNs),
	// so subscribers — including k6 — get latency-measurable messages.
	go publish(ctx, srv, *rateLimit, *msgSize, logger)

	logger.Info("websocket broadcast server running",
		zap.String("addr", *addr),
		zap.Int("rate", *rateLimit),
		zap.Int("msg_size", *msgSize),
		zap.String("endpoint", fmt.Sprintf("ws://%s/ws", *addr)),
	)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = srv.Stop()
			return
		case <-ticker.C:
			s := srv.Stats()
			fmt.Printf("websocket-server: conns=%d broadcast_msgs=%d\n", s.Connections, s.Sent)
		}
	}
}

func publish(ctx context.Context, srv *gorilla.GorillaServer, rateLimit, msgSize int, logger *zap.Logger) {
	gen := generator.NewRandomGenerator()
	size := generator.Size(msgSize)

	var limiter *rate.Limiter
	if rateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(rateLimit), rateLimit)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}
		// Broadcast even with zero subscribers is a cheap no-op; this keeps the
		// publish rate steady so subscribers that join mid-run see full throughput.
		_ = srv.Broadcast(gen.Next(size))
	}
}
