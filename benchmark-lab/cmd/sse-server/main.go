package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/sse"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9007", "listen address")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{
		Label:    "sse/server",
		Protocol: "sse",
		Scenario: "standalone",
	})

	srv := sse.NewSSEServer(*addr, rec, logger)

	if err := srv.Start(); err != nil {
		logger.Error("server failed to start", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("server started", zap.String("addr", *addr), zap.String("protocol", "sse"))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	<-ctx.Done()
	cancel()

	if err := srv.Stop(); err != nil {
		logger.Error("server stop error", zap.Error(err))
	}
	logger.Info("server stopped")
}
