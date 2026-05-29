// http1-client - benchmark client/server stub.
package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"
)

func main() {
	_ = flag.String("addr", "127.0.0.1:9000", "server address")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
}
