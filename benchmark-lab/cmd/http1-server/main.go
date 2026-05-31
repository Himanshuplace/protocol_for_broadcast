// http1-server starts an HTTP/1.1 chunked-streaming benchmark server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/http1"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9000", "listen address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	srv := http1.NewHTTP1Server(transport.TransportConfig{ListenAddr: *addr}, logger)
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
			fmt.Printf("http1-server: conns=%d sent=%d\n", s.Connections, s.Sent)
		}
	}
}
