package scenarios

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// RunScenarioC runs the realistic 1:1000 mixed-subscriber scenario.
// Mix: 700 retail (10 instruments), 200 algo (50 instruments), 100 full-feed.
// Key metric: P99 latency degradation from B→C (should be <2×).
func RunScenarioC(ctx context.Context, cfg ScenarioConfig, logger *zap.Logger) (*collector.RunResult, error) {
	cfg.Scenario = "C"
	cfg.ReceiverCount = 1000
	if cfg.Duration == 0 {
		cfg.Duration = 120 * time.Second
	}
	if cfg.WarmupDuration == 0 {
		cfg.WarmupDuration = 10 * time.Second
	}
	runner := NewRunner(cfg, logger)
	return runner.Run(ctx)
}
