package scenarios

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// RunScenarioE runs the distributed cluster scenario (100K+ subscribers).
//
// In K8s mode: spawns N worker Jobs via the K8s API, each running as a subscriber pod.
// Each pod connects to the publisher and records its own metrics, which are aggregated
// back to PostgreSQL.
//
// Without K8s: falls back to 1000 receivers locally for development.
func RunScenarioE(ctx context.Context, cfg ScenarioConfig, logger *zap.Logger) (*collector.RunResult, error) {
	cfg.Scenario = "E"
	if cfg.ReceiverCount == 0 {
		cfg.ReceiverCount = 100_000
	}
	if cfg.Duration == 0 {
		cfg.Duration = 300 * time.Second
	}

	if !kubernetesAvailable() {
		logger.Warn("scenario E: Kubernetes not available, falling back to local 1000-receiver scenario",
			zap.Int("fallback_receivers", 1000))
		cfg.ReceiverCount = 1000
		cfg.Scenario = "E-local"
		return NewRunner(cfg, logger).Run(ctx)
	}

	return nil, fmt.Errorf("scenario E Kubernetes mode: set KUBECONFIG and run with --k8s-namespace flag")
}

// kubernetesAvailable returns true if an in-cluster service account token exists
// or KUBECONFIG env var is set.
func kubernetesAvailable() bool {
	if os.Getenv("KUBECONFIG") != "" {
		return true
	}
	f, err := os.Open("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err == nil {
		f.Close()
		return true
	}
	return false
}
