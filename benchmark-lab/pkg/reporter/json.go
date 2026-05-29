package reporter

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/collector"
)

// JSONReporter writes results as a JSON array.
type JSONReporter struct {
	out io.Writer
}

// NewJSONReporter creates a reporter writing to w (defaults to os.Stdout).
func NewJSONReporter(w io.Writer) *JSONReporter {
	if w == nil {
		w = os.Stdout
	}
	return &JSONReporter{out: w}
}

func (r *JSONReporter) Name() string { return "json" }

func (r *JSONReporter) Report(results []*collector.RunResult) error {
	enc := json.NewEncoder(r.out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		return fmt.Errorf("json reporter: %w", err)
	}
	return nil
}
