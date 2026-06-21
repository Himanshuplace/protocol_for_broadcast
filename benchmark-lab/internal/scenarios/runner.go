package scenarios

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/generator"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/market"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/metrics"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/transport"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// ScenarioConfig defines one complete benchmark run.
type ScenarioConfig struct {
	Protocol       string
	Scenario       string
	ReceiverCount  int
	SenderCount    int
	Duration       time.Duration
	WarmupDuration time.Duration
	MsgSize        int
	GeneratorType  string // "random"|"sequential"|"json"|"binary"|"market"
	RateLimit      int    // msgs/sec, 0 = flood
	NetworkProfile string // "clean"|"loss1"|...
	BroadcastStrat string // "naive"|"workerpool"|"sharded"|"epoll"
	ServerAddr     string
	ServerPort     int
	LogLevel       string
	// RecvHandler is injected by the runner so factories can route client-side
	// receives back into the runner's recorder for latency and throughput tracking.
	RecvHandler transport.RecvHandler
	// TraceWriter, if set, receives a CSV per-packet trace of the measurement
	// phase (one row per received packet, per client). Best used with RateLimit
	// set, since a flood produces millions of rows.
	TraceWriter io.Writer
}

// TransportFactory creates a transport from a config.
type TransportFactory func(cfg ScenarioConfig, logger *zap.Logger) (transport.Transport, error)

var (
	factoryMu sync.RWMutex
	factories = map[string]TransportFactory{}
)

// Register adds a transport factory. Call from init() in each transport package.
func Register(name string, f TransportFactory) {
	factoryMu.Lock()
	factories[name] = f
	factoryMu.Unlock()
}

// ScenarioRunner orchestrates one benchmark scenario end-to-end.
type ScenarioRunner struct {
	cfg      ScenarioConfig
	recorder *metrics.Recorder
	logger   *zap.Logger
	traceOn  atomic.Bool // gates per-packet trace to the measurement phase
}

// NewRunner creates a ScenarioRunner.
func NewRunner(cfg ScenarioConfig, logger *zap.Logger) *ScenarioRunner {
	if logger == nil {
		logger = zap.NewNop()
	}
	label := fmt.Sprintf("%s/scenario-%s/%dB/%s", cfg.Protocol, cfg.Scenario, cfg.MsgSize, cfg.NetworkProfile)
	return &ScenarioRunner{
		cfg: cfg,
		recorder: metrics.NewRecorder(metrics.RecorderConfig{
			Label:    label,
			Protocol: cfg.Protocol,
			Scenario: cfg.Scenario,
		}),
		logger: logger,
	}
}

// Run executes the benchmark scenario and returns all measured metrics.
func (r *ScenarioRunner) Run(ctx context.Context) (*collector.RunResult, error) {
	factoryMu.RLock()
	factory, ok := factories[r.cfg.Protocol]
	factoryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("scenario runner: unknown protocol %q", r.cfg.Protocol)
	}

	// Optional per-packet CSV trace, captured only during the measurement phase.
	var (
		traceMu  sync.Mutex
		traceBuf *bufio.Writer
		traceCnt int
	)
	const traceCap = 5_000_000 // guard against runaway files on flood runs
	if r.cfg.TraceWriter != nil {
		traceBuf = bufio.NewWriter(r.cfg.TraceWriter)
		fmt.Fprintln(traceBuf, "protocol,client_id,seq,send_unixnano,recv_unixnano,latency_us")
		defer func() {
			traceMu.Lock()
			_ = traceBuf.Flush()
			traceMu.Unlock()
		}()
	}

	// Inject a RecvHandler so factories can route client receives into this recorder.
	// Each receiver is tracked per connection (RecordRecvFrom) so that with N
	// subscribers the same broadcast seq is not counted as N-1 duplicates and
	// throughput is reported per subscriber rather than N× inflated.
	r.cfg.RecvHandler = func(id transport.ConnID, data []byte, recvAt time.Time) {
		seq, sendNs, _, err := wire.DecodeHeader(data)
		if err != nil {
			return
		}
		recvNs := recvAt.UnixNano()
		r.recorder.RecordRecvFrom(string(id), seq, sendNs, len(data), recvNs)
		if traceBuf != nil && r.traceOn.Load() {
			traceMu.Lock()
			if traceCnt < traceCap {
				fmt.Fprintf(traceBuf, "%s,%s,%d,%d,%d,%d\n",
					r.cfg.Protocol, id, seq, sendNs, recvNs, (recvNs-sendNs)/1000)
				traceCnt++
			}
			traceMu.Unlock()
		}
	}

	srv, err := factory(r.cfg, r.logger)
	if err != nil {
		return nil, fmt.Errorf("scenario runner: create transport: %w", err)
	}

	if err := srv.Start(); err != nil {
		return nil, fmt.Errorf("scenario runner: start transport: %w", err)
	}
	defer srv.Stop() //nolint:errcheck

	// Allow server to initialize
	time.Sleep(50 * time.Millisecond)

	gen := r.makeGenerator()
	startedAt := time.Now()

	// Warmup phase
	if r.cfg.WarmupDuration > 0 {
		wCtx, wCancel := context.WithTimeout(ctx, r.cfg.WarmupDuration)
		r.logger.Info("scenario: warmup", zap.Duration("duration", r.cfg.WarmupDuration))
		r.broadcastLoop(wCtx, srv, gen, true)
		wCancel()
	}

	// Measurement phase
	r.recorder.Reset()
	r.recorder.Start()
	mCtx, mCancel := context.WithTimeout(ctx, r.cfg.Duration)
	defer mCancel()
	r.logger.Info("scenario: measuring",
		zap.String("protocol", r.cfg.Protocol),
		zap.String("scenario", r.cfg.Scenario),
		zap.Duration("duration", r.cfg.Duration),
	)
	r.traceOn.Store(true)
	r.broadcastLoop(mCtx, srv, gen, false)
	r.traceOn.Store(false)
	r.recorder.Stop()

	endedAt := time.Now()
	snap := r.recorder.Snapshot()
	stats := srv.Stats()
	elapsed := endedAt.Sub(startedAt)

	perClient := buildPerClient(r.recorder.ConnSnapshots())
	var totalLost int64
	var lossRatePct float64
	if len(perClient) > 0 {
		var sumPct float64
		for _, pc := range perClient {
			totalLost += pc.Lost
			sumPct += pc.LossRatePct
		}
		lossRatePct = sumPct / float64(len(perClient))
	} else {
		totalLost = int64(stats.Lost)
		if s := int64(snap.MsgSent); s > 0 {
			lossRatePct = float64(snap.Seq.Lost) / float64(s) * 100
		}
	}

	return &collector.RunResult{
		Protocol:       r.cfg.Protocol,
		Scenario:       r.cfg.Scenario,
		MsgSize:        r.cfg.MsgSize,
		GeneratorType:  r.cfg.GeneratorType,
		NetProfile:     r.cfg.NetworkProfile,
		BroadcastStrat: r.cfg.BroadcastStrat,
		ReceiverCount:  r.cfg.ReceiverCount,
		SenderCount:    r.cfg.SenderCount,
		DurationS:      int(r.cfg.Duration.Seconds()),
		WarmupS:        int(r.cfg.WarmupDuration.Seconds()),
		StartedAt:      startedAt,
		EndedAt:        endedAt,
		GoVersion:      runtime.Version(),
		OSArch:         runtime.GOOS + "/" + runtime.GOARCH,

		LatMinNs:    snap.Latency.Min.Nanoseconds(),
		LatAvgNs:    snap.Latency.Mean.Nanoseconds(),
		LatP50Ns:    snap.Latency.P50.Nanoseconds(),
		LatP95Ns:    snap.Latency.P95.Nanoseconds(),
		LatP99Ns:    snap.Latency.P99.Nanoseconds(),
		LatP999Ns:   snap.Latency.P999.Nanoseconds(),
		LatMaxNs:    snap.Latency.Max.Nanoseconds(),
		LatStddevNs: snap.Latency.StdDev.Nanoseconds(),

		MsgsPerSec:    snap.MsgPerSec,
		BytesPerSec:   snap.BytesPerSec,
		TotalMsgsSent: int64(snap.MsgSent),
		TotalMsgsRecv: int64(snap.MsgRecv),

		MsgsLost:       totalLost,
		LossRatePct:    lossRatePct,
		MsgsReordered:  int64(snap.Seq.Reordered),
		MsgsDuplicated: int64(snap.Seq.Duplicated),

		CPUPctAvg:     snap.Resources.CPUAvg,
		CPUPctP99:     snap.Resources.CPUP99,
		MemBytesAvg:   int64(snap.Resources.MemAvg),
		MemBytesMax:   int64(snap.Resources.MemMax),
		GoroutinesAvg: int32(snap.Resources.GoroutineAvg),
		GoroutinesMax: int32(snap.Resources.GoroutineMax),
		FDCountAvg:    int32(snap.Resources.FDAvg),
		FDCountMax:    int32(snap.Resources.FDMax),

		HandshakeAvgNs: snap.Handshake.Mean.Nanoseconds(),
		HandshakeP99Ns: snap.Handshake.P99.Nanoseconds(),
		ReconnectAvgNs: snap.Reconnect.Mean.Nanoseconds(),
		ReconnectP99Ns: snap.Reconnect.P99.Nanoseconds(),

		PerClient: perClient,

		Config: map[string]any{
			"elapsed_s": elapsed.Seconds(),
		},
	}, nil
}

// buildPerClient converts per-connection snapshots into per-client stats.
//
// Loss is measured RELATIVE to the fastest client: globalMax is the highest
// sequence number any subscriber received (a proxy for the last broadcast), and
// each client's loss counts the sequence numbers from its first received seq up
// to globalMax that it did not receive. This answers "which client missed what,"
// without needing the server's internal counter, and never penalizes a client
// for sequence numbers broadcast before it joined.
func buildPerClient(conns []metrics.ConnSnapshot) []collector.ClientStat {
	if len(conns) == 0 {
		return nil
	}
	var globalMax uint64
	for _, c := range conns {
		if c.LastSeq > globalMax {
			globalMax = c.LastSeq
		}
	}
	sort.Slice(conns, func(i, j int) bool { return conns[i].ConnID < conns[j].ConnID })

	out := make([]collector.ClientStat, 0, len(conns))
	for _, c := range conns {
		var lost int64
		var lossPct float64
		if c.Delivered > 0 && c.FirstSeq > 0 {
			expected := int64(globalMax-c.FirstSeq) + 1
			lost = expected - int64(c.Delivered)
			if lost < 0 {
				lost = 0
			}
			if expected > 0 {
				lossPct = float64(lost) / float64(expected) * 100
			}
		}
		out = append(out, collector.ClientStat{
			ClientID:    c.ConnID,
			MsgRecv:     int64(c.MsgRecv),
			Delivered:   int64(c.Delivered),
			Lost:        lost,
			Duplicated:  int64(c.Duplicated),
			Reordered:   int64(c.Reordered),
			LossRatePct: lossPct,
			FirstSeq:    c.FirstSeq,
			LastSeq:     c.LastSeq,
			LatP50Ns:    c.Latency.P50.Nanoseconds(),
			LatP99Ns:    c.Latency.P99.Nanoseconds(),
			LatMaxNs:    c.Latency.Max.Nanoseconds(),
		})
	}
	return out
}

func (r *ScenarioRunner) broadcastLoop(ctx context.Context, srv transport.Transport, gen func() []byte, warmup bool) {
	var limiter *rate.Limiter
	if r.cfg.RateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(r.cfg.RateLimit), r.cfg.RateLimit)
	}

	var sent atomic.Uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				return
			}
		}

		data := gen()
		if err := srv.Broadcast(data); err != nil {
			continue
		}

		n := sent.Add(1)
		if !warmup {
			r.recorder.RecordSend(n, len(data))
		}
	}
}

func (r *ScenarioRunner) makeGenerator() func() []byte {
	size := generator.Size(r.cfg.MsgSize)
	switch r.cfg.GeneratorType {
	case "json":
		g := generator.NewJSONGenerator()
		return func() []byte { return g.Next(size) }
	case "binary":
		g := generator.NewBinaryGenerator()
		return func() []byte { return g.Next(size) }
	case "sequential":
		g := generator.NewSequentialGenerator()
		return func() []byte { return g.Next(size) }
	case "market":
		universe := market.DefaultUniverse()
		g := generator.NewMarketTickGenerator(universe)
		idx := 0
		return func() []byte {
			b := g.NextTickEncoded(idx % len(universe))
			idx++
			return b
		}
	default: // "random"
		g := generator.NewRandomGenerator()
		return func() []byte { return g.Next(size) }
	}
}

// Recorder exposes the recorder for external metric injection (e.g. from client goroutines).
func (r *ScenarioRunner) Recorder() *metrics.Recorder { return r.recorder }
