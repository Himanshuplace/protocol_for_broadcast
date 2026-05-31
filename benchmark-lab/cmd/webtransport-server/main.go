// webtransport-server starts a WebTransport benchmark server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/webtransport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9443", "listen address (UDP/QUIC)")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "webtransport-server", Protocol: "webtransport"})
	srv, err := webtransport.NewWebTransportServer(*addr, webtransport.ModeUniStream, rec, logger)
	if err != nil {
		logger.Fatal("create server", zap.Error(err))
	}
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
			fmt.Printf("webtransport-server: conns=%d sent=%d\n", s.Connections, s.Sent)
		}
	}
}
