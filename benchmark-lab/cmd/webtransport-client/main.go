// webtransport-client connects to a WebTransport benchmark server.
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
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9443", "server address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "webtransport-client", Protocol: "webtransport"})
	rec.Start()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c := webtransport.NewWebTransportClient(*addr, webtransport.ModeUniStream,
		func(_ transport.ConnID, data []byte, recvAt time.Time) {
			seq, sendNs, _, err := wire.DecodeHeader(data)
			if err != nil {
				return
			}
			rec.RecordRecv(seq, sendNs, len(data), recvAt.UnixNano())
		}, nil, logger)

	if err := c.Connect(ctx); err != nil {
		logger.Fatal("connect", zap.Error(err))
	}
	defer c.Close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			snap := rec.Snapshot()
			fmt.Printf("webtransport-client final: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
			return
		case <-ticker.C:
			snap := rec.Snapshot()
			fmt.Printf("webtransport-client: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
		}
	}
}
