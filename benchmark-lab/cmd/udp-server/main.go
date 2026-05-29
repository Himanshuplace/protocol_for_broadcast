// udp-server starts a UDP benchmark server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/udp"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:9001", "listen address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "udp-server", Protocol: "udp"})
	srv := udp.NewUDPServer(*addr,
		udp.WithServerRecorder(rec),
		udp.WithServerLogger(logger),
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
			srv.Stop() //nolint:errcheck
			return
		case <-ticker.C:
			stats := srv.Stats()
			fmt.Printf("udp-server: conns=%d sent=%d recv=%d lost=%d\n",
				stats.Connections, stats.Sent, stats.Received, stats.Lost)
		}
	}
}
