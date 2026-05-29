// tcp-server starts the tcp benchmark server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:910046", "listen address")
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	_ = logger
	_ = addr
	_ = fmt.Sprintf

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	_ = time.Second
	<-ctx.Done()
}
