package scenarios

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// RunScenarioD runs the institutional-scale 1:10000 scenario.
// 10,000 subscribers, 1000 instruments.
// Requires: ulimit -n 25000
// Tests epoll vs goroutine-per-conn performance cliff.
func RunScenarioD(ctx context.Context, cfg ScenarioConfig, logger *zap.Logger) (*collector.RunResult, error) {
	cfg.Scenario = "D"
	cfg.ReceiverCount = 10_000
	if cfg.RateLimit == 0 {
		cfg.RateLimit = 50_000 // 50K ticks/sec across all instruments
	}
	if cfg.Duration == 0 {
		cfg.Duration = 180 * time.Second
	}
	if cfg.WarmupDuration == 0 {
		cfg.WarmupDuration = 15 * time.Second
	}
	runner := NewRunner(cfg, logger)
	return runner.Run(ctx)
}
