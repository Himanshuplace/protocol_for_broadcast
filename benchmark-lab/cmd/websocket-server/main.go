package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	wscoder "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/coder"
	wsgobwas "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gobwas"
	wsgorilla "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gorilla"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"go.uber.org/zap"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9003", "listen address")
	mode := flag.String("mode", "gorilla", "websocket implementation: gorilla|gobwas|coder")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	cfg := transport.TransportConfig{ListenAddr: *addr}

	type server interface {
		Start() error
		Stop() error
	}

	var srv server
	switch *mode {
	case "gorilla":
		srv = wsgorilla.NewGorillaServer(cfg, logger)
	case "gobwas":
		srv = wsgobwas.NewGobwasServer(cfg, logger)
	case "coder":
		srv = wscoder.NewCoderServer(cfg, logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q; valid choices: gorilla|gobwas|coder\n", *mode)
		os.Exit(1)
	}

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
