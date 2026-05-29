package generator

import (
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// SequentialGenerator fills the payload with a deterministic repeating byte pattern:
// byte[i] = byte(i % 256). This produces a predictable, compressible payload.
//
// Use cases:
//   - Testing protocol compression (compressible data shows whether compression helps)
//   - Reproducible debugging (same bytes every run for a given targetSize)
//   - Detecting corruption (receiver can verify the pattern)
type SequentialGenerator struct{}

// NewSequentialGenerator creates a SequentialGenerator.
// No state needed — the pattern is fully determined by position.
func NewSequentialGenerator() *SequentialGenerator {
	return &SequentialGenerator{}
}

// Next returns a wire-encoded frame with a deterministic sequential byte payload.
func (g *SequentialGenerator) Next(targetSize Size) []byte {
	payloadLen := int(targetSize) - wire.HeaderLen
	if payloadLen < 0 {
		payloadLen = 0
	}

	seq := nextSeq()
	now := time.Now().UnixNano()

	out := make([]byte, wire.HeaderLen+payloadLen)
	wire.EncodeInto(out, seq, now, out[wire.HeaderLen:])

	// Fill payload with sequential pattern
	for i := 0; i < payloadLen; i++ {
		out[wire.HeaderLen+i] = byte(i)
	}

	return out
}

// Name returns the generator identifier.
func (g *SequentialGenerator) Name() string { return "sequential" }

// Verify checks that a received payload (after the wire header) contains the
// expected sequential pattern. Used by receiver-side integrity checking.
// Returns the first mismatched byte index, or -1 if valid.
func (g *SequentialGenerator) Verify(payload []byte) int {
	for i, b := range payload {
		if b != byte(i) {
			return i
		}
	}
	return -1
}
