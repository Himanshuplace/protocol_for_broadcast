// http3-client connects to an HTTP/3 benchmark server.
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
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9443", "server address")
	mode := flag.String("mode", "stream", "broadcast mode: stream|unidirstream|datagram")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{Label: "http3-client", Protocol: "http3"})
	rec.Start()

	c := http3.NewHTTP3Client(*addr,
		http3.WithClientLogger(logger),
		http3.WithClientMode(*mode),
		http3.WithClientRecvHandler(func(_ transport.ConnID, data []byte, recvAt time.Time) {
			seq, sendNs, _, err := wire.DecodeHeader(data)
			if err != nil {
				return
			}
			rec.RecordRecv(seq, sendNs, len(data), recvAt.UnixNano())
		}),
	)

	if err := c.Start(); err != nil {
		logger.Fatal("start", zap.Error(err))
	}
	defer c.Stop() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			snap := rec.Snapshot()
			fmt.Printf("http3-client final: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
			return
		case <-ticker.C:
			snap := rec.Snapshot()
			fmt.Printf("http3-client: recv=%d msg/s=%.0f\n", snap.MsgRecv, snap.MsgPerSec)
		}
	}
}
