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

	"github.com/himanshuplace/protocol_for_broadcast/internal/webtransport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9008", "server address")
	mode := flag.String("mode", "unidirstream", "transport mode: unidirstream|bidistream|datagram")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	rec := metrics.NewRecorder(metrics.RecorderConfig{
		Label:    "webtransport/client",
		Protocol: "webtransport",
		Scenario: "standalone",
	})

	var received atomic.Uint64
	handler := func(_ transport.ConnID, data []byte, _ time.Time) {
		received.Add(1)
	}

	var wtMode webtransport.Mode
	switch *mode {
	case "bidistream":
		wtMode = webtransport.ModeBidiStream
	case "datagram":
		wtMode = webtransport.ModeDatagrams
	default:
		wtMode = webtransport.ModeUniStream
	}

	client := webtransport.NewWebTransportClient(*addr, wtMode, handler, rec, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		logger.Error("connect failed", zap.Error(err))
		os.Exit(1)
	}

	<-ctx.Done()
	client.Close()
	fmt.Printf("received %d messages\n", received.Load())
}
