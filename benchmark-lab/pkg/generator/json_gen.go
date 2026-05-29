package generator

import (
	"time"

	"github.com/bytedance/sonic"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// JSONGenerator produces JSON-encoded payloads using github.com/bytedance/sonic.
//
// Why sonic over encoding/json?
//   - AMD64 + AVX2: sonic JIT-compiles a SIMD JSON encoder at first use.
//     Marshal throughput: ~900MB/s vs encoding/json ~300MB/s (3× faster).
//   - AMD Ryzen 5000+/7000+ and Intel Haswell+: both support AVX2.
//   - sonic detects CPU capabilities at runtime via CPUID — no build tags needed.
//   - Falls back to standard encoding/json on ARM64, RISC-V, etc. automatically.
//
// Payload structure:
//   - The JSON object contains the wire frame fields (seq, sendNs) + padding string.
//   - The padding string is base64(random bytes) to reach the target total size.
//   - This makes the JSON payload compressible (base64 chars are 64-symbol alphabet)
//     but not trivially so (not all-zeros).
type JSONGenerator struct {
	rng *RandomGenerator
}

// jsonPayload is the structure serialized to JSON.
// Field names are lowercase + short to minimize JSON overhead.
// Field ordering is fixed (struct tags) for reproducible sizing.
type jsonPayload struct {
	Seq   uint64 `json:"s"`   // sequence number
	Ts    int64  `json:"t"`   // timestamp nanoseconds
	Pad   string `json:"p"`   // padding to reach target size
	Sym   string `json:"sym"` // symbol (populated in market mode)
	Price int64  `json:"px"`  // price in micros (populated in market mode)
}

// NewJSONGenerator creates a JSONGenerator.
func NewJSONGenerator() *JSONGenerator {
	return &JSONGenerator{rng: NewRandomGenerator()}
}

// Next produces a wire-encoded frame where the payload is a JSON object.
// The JSON is padded to fill targetSize bytes total (header + JSON payload).
// If the base JSON (without padding) already exceeds targetSize, no padding is added.
func (g *JSONGenerator) Next(targetSize Size) []byte {
	seq := nextSeq()
	now := time.Now().UnixNano()

	// Base JSON object (no padding yet)
	base := jsonPayload{Seq: seq, Ts: now, Sym: "BENCH", Price: 0}
	baseJSON, _ := sonic.Marshal(base)
	baseSize := wire.HeaderLen + len(baseJSON)

	// Calculate padding needed
	padNeeded := int(targetSize) - baseSize
	var padStr string
	if padNeeded > 0 {
		// Generate random padding characters (visible ASCII range)
		padBytes := make([]byte, padNeeded)
		g.rng.fillRandom(padBytes)
		// Map to printable ASCII: 0x20–0x7E (95 chars)
		for i, b := range padBytes {
			padBytes[i] = 0x20 + b%95
		}
		padStr = string(padBytes)
	}

	base.Pad = padStr
	jsonBytes, err := sonic.Marshal(base)
	if err != nil {
		// Fallback: return a minimal frame on marshal failure (should never happen)
		out := make([]byte, wire.HeaderLen)
		wire.EncodeInto(out, seq, now, nil)
		return out
	}

	out := make([]byte, wire.HeaderLen+len(jsonBytes))
	wire.EncodeInto(out, seq, now, jsonBytes)
	return out
}

// Name returns the generator identifier.
func (g *JSONGenerator) Name() string { return "json" }

// MarshalTick marshals a market tick to JSON using sonic.
// Used by the market tick generator when JSON encoding mode is selected.
func MarshalTick(v any) ([]byte, error) {
	return sonic.Marshal(v)
}

// UnmarshalTick deserializes a JSON payload into v using sonic.
// Used by receivers when JSON encoding mode is active.
func UnmarshalTick(data []byte, v any) error {
	return sonic.Unmarshal(data, v)
}
