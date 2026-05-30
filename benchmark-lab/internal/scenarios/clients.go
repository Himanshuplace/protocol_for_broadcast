package scenarios

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// ClientFactory creates one receiver client for a benchmark scenario.
// idx is the zero-based client index. serverAddr is "host:port".
// The factory MUST wire RecordRecv calls into recorder for every received frame.
// Returns a close function (never nil on success) and any connect error.
type ClientFactory func(
	ctx context.Context,
	cfg ScenarioConfig,
	idx int,
	serverAddr string,
	recorder *metrics.Recorder,
	logger *zap.Logger,
) (closeFunc func(), err error)

var (
	clientFactoryMu sync.RWMutex
	clientFactories = map[string]ClientFactory{}
)

// RegisterClient adds a client factory for the named protocol.
func RegisterClient(name string, f ClientFactory) {
	clientFactoryMu.Lock()
	clientFactories[name] = f
	clientFactoryMu.Unlock()
}

// DefaultRecvHandler returns a RecvHandler that decodes wire frames and records
// receive metrics into rec. Use this as the inbound message callback for all clients.
func DefaultRecvHandler(rec *metrics.Recorder) transport.RecvHandler {
	return func(_ transport.ConnID, data []byte, recvAt time.Time) {
		if len(data) < wire.HeaderLen {
			return
		}
		seq, sendNs, _, err := wire.DecodeHeader(data)
		if err != nil {
			return
		}
		rec.RecordRecv(seq, sendNs, len(data), recvAt.UnixNano())
	}
}

// connectClients spawns n client goroutines using the registered ClientFactory for
// cfg.Protocol. Returns all successfully created close-functions; logs a warning if
// no factory is registered (server-only mode). Connects in parallel with a concurrency
// cap of 64 to avoid overwhelming the server on large scenarios.
func (r *ScenarioRunner) connectClients(ctx context.Context, n int) (closeFuncs []func()) {
	if n <= 0 {
		return nil
	}

	clientFactoryMu.RLock()
	factory, ok := clientFactories[r.cfg.Protocol]
	clientFactoryMu.RUnlock()

	if !ok {
		r.logger.Warn("no client factory for protocol; benchmarking server-side only",
			zap.String("protocol", r.cfg.Protocol))
		return nil
	}

	serverAddr := fmt.Sprintf("%s:%d", r.cfg.ServerAddr, r.cfg.ServerPort)

	var mu sync.Mutex
	var wg sync.WaitGroup

	// Limit concurrency to avoid thundering-herd on large ReceiverCount values.
	const maxConcurrent = 64
	sem := make(chan struct{}, maxConcurrent)

	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			cf, err := factory(ctx, r.cfg, idx, serverAddr, r.recorder, r.logger)
			if err != nil {
				r.logger.Debug("client connect failed",
					zap.String("protocol", r.cfg.Protocol),
					zap.Int("idx", idx),
					zap.Error(err))
				return
			}
			mu.Lock()
			closeFuncs = append(closeFuncs, cf)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	r.logger.Info("clients connected",
		zap.String("protocol", r.cfg.Protocol),
		zap.Int("requested", n),
		zap.Int("connected", len(closeFuncs)))

	return closeFuncs
}
