package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/himanshuplace/protocol_for_broadcast/internal/http2"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9005", "listen address")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	cfg := transport.TransportConfig{ListenAddr: *addr}
	srv := http2.NewHTTP2Server(cfg, logger)

	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start server: %v\n", err)
		os.Exit(1)
	}
	logger.Info("server started", zap.String("addr", *addr))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	if err := srv.Stop(); err != nil {
		logger.Error("error stopping server", zap.Error(err))
	}
	logger.Info("server stopped")
}
