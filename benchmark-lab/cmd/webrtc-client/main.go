package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/webrtc"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9009", "signaling server address")
	mode := flag.String("mode", "reliable", "channel mode: reliable|unreliable|partial-reliable")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{
		Label:    "webrtc/client",
		Protocol: "webrtc",
		Scenario: "standalone",
	})

	var received atomic.Uint64
	handler := func(_ transport.ConnID, data []byte, _ time.Time) {
		received.Add(1)
	}

	var chanMode webrtc.ChannelMode
	switch *mode {
	case "unreliable":
		chanMode = webrtc.ModeUnreliable
	case "partial-reliable":
		chanMode = webrtc.ModePartialReliable
	default:
		chanMode = webrtc.ModeReliable
	}

	client := webrtc.NewWebRTCClient(*addr, chanMode, handler, rec, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		logger.Error("connect failed", zap.Error(err))
		os.Exit(1)
	}

	<-ctx.Done()
	if err := client.Close(); err != nil {
		logger.Error("close error", zap.Error(err))
	}
	fmt.Printf("received %d messages\n", received.Load())
}
