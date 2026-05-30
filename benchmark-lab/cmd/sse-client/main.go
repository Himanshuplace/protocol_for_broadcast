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

	"github.com/himanshuplace/protocol_for_broadcast/internal/sse"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9007", "server address")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	var received atomic.Uint64
	handler := func(_ transport.ConnID, _ []byte, _ time.Time) {
		received.Add(1)
	}

	rec := metrics.NewRecorder(metrics.RecorderConfig{
		Label:    "sse/client",
		Protocol: "sse",
		Scenario: "standalone",
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client := sse.NewSSEClient(*addr, handler, rec, logger)
	if err := client.Connect(ctx); err != nil {
		logger.Error("connect failed", zap.String("addr", *addr), zap.Error(err))
		os.Exit(1)
	}
	logger.Info("connected", zap.String("addr", *addr))

	<-ctx.Done()
	client.Close()
	fmt.Printf("received %d messages\n", received.Load())
}
