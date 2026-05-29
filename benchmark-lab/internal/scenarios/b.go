package scenarios

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// RunScenarioB runs the 1:100 selective subscription scenario.
// 100 subscribers, each subscribing to a random 10-instrument subset.
// Key metric: tail latency per instrument, topic routing overhead.
func RunScenarioB(ctx context.Context, cfg ScenarioConfig, logger *zap.Logger) (*collector.RunResult, error) {
	cfg.Scenario = "B"
	cfg.ReceiverCount = 100
	if cfg.Duration == 0 {
		cfg.Duration = 60 * time.Second
	}
	if cfg.WarmupDuration == 0 {
		cfg.WarmupDuration = 5 * time.Second
	}
	runner := NewRunner(cfg, logger)
	return runner.Run(ctx)
}
