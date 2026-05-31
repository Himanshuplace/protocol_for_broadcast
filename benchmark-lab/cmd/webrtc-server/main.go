// webrtc-server starts a WebRTC benchmark server with a built-in signaling server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/webrtc"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9000", "signaling server address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "webrtc-server", Protocol: "webrtc"})
	srv := webrtc.NewWebRTCServer(*addr, webrtc.ModeReliable, rec, logger)
	if err := srv.Start(); err != nil {
		logger.Fatal("start", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = srv.Stop()
			return
		case <-ticker.C:
			s := srv.Stats()
			fmt.Printf("webrtc-server: conns=%d sent=%d\n", s.Connections, s.Sent)
		}
	}
}
