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

	wscoder "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/coder"
	wsgobwas "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gobwas"
	wsgorilla "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gorilla"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9003", "server address")
	mode := flag.String("mode", "gorilla", "websocket implementation: gorilla|gobwas|coder")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	var received atomic.Uint64
	handler := func(_ transport.ConnID, data []byte, _ time.Time) {
		received.Add(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch *mode {
	case "gobwas":
		client := wsgobwas.NewGobwasClient(*addr, handler, logger)
		if err := client.Connect(ctx); err != nil {
			logger.Error("connect failed", zap.Error(err))
			os.Exit(1)
		}
		<-ctx.Done()
		client.Close()

	case "coder":
		client := wscoder.NewCoderClient(*addr, handler, logger)
		if err := client.Connect(ctx); err != nil {
			logger.Error("connect failed", zap.Error(err))
			os.Exit(1)
		}
		<-ctx.Done()
		client.Close()

	default: // gorilla
		client := wsgorilla.NewGorillaClient(*addr, handler, logger)
		if err := client.Connect(ctx); err != nil {
			logger.Error("connect failed", zap.Error(err))
			os.Exit(1)
		}
		<-ctx.Done()
		client.Close()
	}

	fmt.Printf("received %d messages\n", received.Load())
}
