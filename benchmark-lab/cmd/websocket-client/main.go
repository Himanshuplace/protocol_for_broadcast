// websocket-client connects to a WebSocket benchmark server (gorilla implementation).
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gorilla"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "ws-client", Protocol: "websocket-gorilla"})
	rec.Start()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c := gorilla.NewGorillaClient(*addr, func(_ transport.ConnID, data []byte, recvAt time.Time) {
		seq, sendNs, _, err := wire.DecodeHeader(data)
		if err != nil {
			return
		}
		rec.RecordRecv(seq, sendNs, len(data), recvAt.UnixNano())
	}, logger)

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
			fmt.Printf("websocket-client final: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
			return
		case <-ticker.C:
			snap := rec.Snapshot()
			fmt.Printf("websocket-client: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
		}
	}
}
