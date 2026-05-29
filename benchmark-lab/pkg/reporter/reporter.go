package reporter

import "github.com/himanshuplace/protocol_for_broadcast/pkg/collector"

// Reporter renders benchmark results to an output format.
type Reporter interface {
	Report(results []*collector.RunResult) error
	Name() string
}
