// http3-server - benchmark server stub.
package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"
)

func main() {
	_ = flag.String("addr", "0.0.0.0:9000", "listen address")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	<-ctx.Done()
}
