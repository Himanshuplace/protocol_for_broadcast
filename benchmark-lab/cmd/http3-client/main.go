package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/internal/http3"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9006", "server address")
	mode := flag.String("mode", "stream", "transport mode: stream|unidirstream|datagram")
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

	client := http3.NewHTTP3Client(
		*addr,
		http3.WithClientLogger(logger),
		http3.WithClientRecvHandler(handler),
		http3.WithClientMode(*mode),
	)

	if err := client.Start(); err != nil {
		logger.Error("start failed", zap.Error(err))
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	if err := client.Stop(); err != nil {
		logger.Error("stop error", zap.Error(err))
	}
	fmt.Printf("received %d messages\n", received.Load())
}
