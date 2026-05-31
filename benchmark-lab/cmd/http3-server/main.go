// http3-server starts an HTTP/3-over-QUIC benchmark server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/http3"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9443", "listen address (UDP)")
	mode := flag.String("mode", "stream", "broadcast mode: stream|unidirstream|datagram")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	srv := http3.NewHTTP3Server(*addr,
		http3.WithLogger(logger),
		http3.WithMode(*mode),
	)
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
			fmt.Printf("http3-server: conns=%d sent=%d\n", s.Connections, s.Sent)
		}
	}
}
