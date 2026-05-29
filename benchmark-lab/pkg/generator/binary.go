package generator

import (
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// BinaryGenerator produces zero-filled payloads. Used to benchmark protocol
// overhead in isolation, without random or sequential data computation cost.
// Also useful for testing whether protocols/libraries compress zero bytes transparently.
//
// Throughput note: Go's runtime zero-initializes all heap allocations, so
// make([]byte, N) is already zero-filled at no extra cost. This generator
// measures the minimal possible payload generation overhead.
type BinaryGenerator struct{}

// NewBinaryGenerator creates a BinaryGenerator.
func NewBinaryGenerator() *BinaryGenerator {
	return &BinaryGenerator{}
}

// Next returns a wire-encoded frame with a zero-filled payload.
func (g *BinaryGenerator) Next(targetSize Size) []byte {
	payloadLen := int(targetSize) - wire.HeaderLen
	if payloadLen < 0 {
		payloadLen = 0
	}
	seq := nextSeq()
	now := time.Now().UnixNano()
	// make() zero-fills automatically on all platforms (Go spec guarantee)
	out := make([]byte, wire.HeaderLen+payloadLen)
	wire.EncodeInto(out, seq, now, out[wire.HeaderLen:])
	return out
}

// Name returns the generator identifier.
func (g *BinaryGenerator) Name() string { return "binary" }
