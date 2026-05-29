package scenarios

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// RunScenarioA runs the 1:1 baseline scenario.
// One publisher sends to exactly one subscriber (full 100-instrument feed).
// Also benchmarks round-trip latency in a ping-pong subtest.
func RunScenarioA(ctx context.Context, cfg ScenarioConfig, logger *zap.Logger) (*collector.RunResult, error) {
	cfg.Scenario = "A"
	cfg.ReceiverCount = 1
	if cfg.Duration == 0 {
		cfg.Duration = 60 * time.Second
	}
	if cfg.WarmupDuration == 0 {
		cfg.WarmupDuration = 5 * time.Second
	}
	runner := NewRunner(cfg, logger)
	return runner.Run(ctx)
}
