package collector

import (
	"context"
	"sync"
)

// MemoryCollector stores results in memory. Used for tests and standalone runs.
type MemoryCollector struct {
	mu      sync.RWMutex
	results []*RunResult
}

// NewMemoryCollector returns a new in-memory collector.
func NewMemoryCollector() *MemoryCollector {
	return &MemoryCollector{}
}

func (m *MemoryCollector) Store(_ context.Context, result *RunResult) error {
	m.mu.Lock()
	m.results = append(m.results, result)
	m.mu.Unlock()
	return nil
}

func (m *MemoryCollector) List(_ context.Context, protocol, scenario string, limit int) ([]*RunResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*RunResult
	// Iterate in reverse to return most recent first
	for i := len(m.results) - 1; i >= 0; i-- {
		r := m.results[i]
		if protocol != "" && r.Protocol != protocol {
			continue
		}
		if scenario != "" && r.Scenario != scenario {
			continue
		}
		out = append(out, r)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *MemoryCollector) All() []*RunResult {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*RunResult, len(m.results))
	copy(out, m.results)
	return out
}

func (m *MemoryCollector) Close() error { return nil }
