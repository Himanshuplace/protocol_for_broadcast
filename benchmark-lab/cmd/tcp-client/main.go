// tcp-client connects to a TCP benchmark server and measures receive throughput.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/tcp"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "server address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "tcp-client", Protocol: "tcp"})
	rec.Start()

	c := tcp.NewTCPClient(
		tcp.WithTCPClientLogger(logger),
		tcp.WithTCPClientRecorder(rec),
		tcp.WithTCPClientRecvHandler(func(_ transport.ConnID, data []byte, recvAt time.Time) {
			seq, sendNs, _, err := wire.DecodeHeader(data)
			if err != nil {
				return
			}
			rec.RecordRecv(seq, sendNs, len(data), recvAt.UnixNano())
		}),
	)
	if err := c.Dial(*addr); err != nil {
		logger.Fatal("dial", zap.Error(err))
	}
	defer c.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			snap := rec.Snapshot()
			fmt.Printf("tcp-client final: recv=%d msg/s=%.0f lat_avg=%s\n",
				snap.MsgRecv, snap.MsgPerSec, snap.Latency.Mean)
			return
		case <-ticker.C:
			snap := rec.Snapshot()
			fmt.Printf("tcp-client: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
		}
	}
}
