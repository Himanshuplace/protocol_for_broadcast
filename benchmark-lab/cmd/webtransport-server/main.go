package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/webtransport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9008", "listen address")
	mode := flag.String("mode", "unidirstream", "transport mode: unidirstream|bidistream|datagram")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{
		Label:    "webtransport/server",
		Protocol: "webtransport",
		Scenario: "standalone",
	})

	var wtMode webtransport.Mode
	switch *mode {
	case "bidistream":
		wtMode = webtransport.ModeBidiStream
	case "datagram":
		wtMode = webtransport.ModeDatagrams
	default:
		wtMode = webtransport.ModeUniStream
	}

	srv, err := webtransport.NewWebTransportServer(*addr, wtMode, rec, logger)
	if err != nil {
		logger.Error("failed to create server", zap.Error(err))
		os.Exit(1)
	}

	if err := srv.Start(); err != nil {
		logger.Error("server failed to start", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("server started", zap.String("addr", *addr), zap.String("mode", *mode))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	<-ctx.Done()
	cancel()

	if err := srv.Stop(); err != nil {
		logger.Error("server stop error", zap.Error(err))
	}
	logger.Info("server stopped")
}
