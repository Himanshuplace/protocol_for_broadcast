package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/himanshuplace/protocol_for_broadcast/internal/http3"
	"go.uber.org/zap"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9006", "listen address")
	mode := flag.String("mode", "stream", "broadcast mode: stream|unidirstream|datagram")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	srv := http3.NewHTTP3Server(
		*addr,
		http3.WithLogger(logger),
		http3.WithMode(*mode),
	)

	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start server: %v\n", err)
		os.Exit(1)
	}
	logger.Info("server started", zap.String("addr", *addr), zap.String("mode", *mode))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	if err := srv.Stop(); err != nil {
		logger.Error("error stopping server", zap.Error(err))
	}
	logger.Info("server stopped")
}
