package generator

import (
	"math/rand/v2"
	"time"
	"unsafe"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// RandomGenerator fills the payload with pseudo-random bytes.
// Uses math/rand/v2's PCG64 algorithm (not crypto/rand) for speed:
//   - PCG64 throughput: ~3GB/s on AMD64
//   - crypto/rand throughput: ~150MB/s (OS entropy limited)
//
// For benchmarking, statistical randomness > cryptographic randomness.
// Each goroutine gets its own RNG to avoid contention.
//
// Thread safety: safe because each goroutine creates its own RandomGenerator.
// For shared use, wrap with a sync.Pool.
type RandomGenerator struct {
	rng *rand.Rand
}

// NewRandomGenerator creates a new RandomGenerator with a time-seeded PCG64 source.
func NewRandomGenerator() *RandomGenerator {
	// PCG64: Permuted Congruential Generator. Excellent statistical properties,
	// passes all PractRand tests, faster than MT19937.
	src := rand.NewPCG(uint64(time.Now().UnixNano()), 0xDEADBEEFCAFEBABE)
	return &RandomGenerator{rng: rand.New(src)}
}

// NewRandomGeneratorWithSeed creates a deterministic RandomGenerator.
// Use for reproducible benchmarks.
func NewRandomGeneratorWithSeed(seed uint64) *RandomGenerator {
	src := rand.NewPCG(seed, 0)
	return &RandomGenerator{rng: rand.New(src)}
}

// Next returns a wire-encoded frame where the payload is pseudo-random bytes.
// The wire frame header (24 bytes) contains the sequence number and send timestamp.
// Payload size = targetSize - wire.HeaderLen bytes; if targetSize <= wire.HeaderLen,
// the payload is empty.
func (g *RandomGenerator) Next(targetSize Size) []byte {
	payloadLen := int(targetSize) - wire.HeaderLen
	if payloadLen < 0 {
		payloadLen = 0
	}

	seq := nextSeq()
	now := time.Now().UnixNano()

	out := make([]byte, wire.HeaderLen+payloadLen)
	wire.EncodeInto(out, seq, now, out[wire.HeaderLen:])

	// Fill payload with random bytes using 8-byte-at-a-time Uint64 writes.
	// This is the fastest way to fill a byte slice: 4 bytes per cycle on AMD64 with AVX2.
	// Note: we fill AFTER EncodeInto so the header is correct and only payload is random.
	payload := out[wire.HeaderLen:]
	g.fillRandom(payload)

	return out
}

// fillRandom fills b with pseudo-random bytes using 8-byte-aligned writes.
// Significantly faster than calling rng.Uint64() per byte.
func (g *RandomGenerator) fillRandom(b []byte) {
	n := len(b)
	// Fill 8 bytes at a time
	words := n / 8
	for i := 0; i < words; i++ {
		v := g.rng.Uint64()
		// Unsafe write: avoids bounds check per byte; safe because we know the slice size
		*(*uint64)(unsafe.Pointer(&b[i*8])) = v
	}
	// Fill remaining bytes
	if rem := n % 8; rem > 0 {
		v := g.rng.Uint64()
		for j := 0; j < rem; j++ {
			b[words*8+j] = byte(v >> (uint(j) * 8))
		}
	}
}

// Name returns the generator identifier.
func (g *RandomGenerator) Name() string { return "random" }

// NextInto fills dst with a wire-encoded frame and returns dst.
// Zero-allocation variant: caller provides the buffer.
// dst must be at least wire.EncodedLen(int(targetSize)-wire.HeaderLen) bytes.
func (g *RandomGenerator) NextInto(dst []byte, targetSize Size) []byte {
	payloadLen := int(targetSize) - wire.HeaderLen
	if payloadLen < 0 {
		payloadLen = 0
	}
	need := wire.HeaderLen + payloadLen
	if cap(dst) < need {
		dst = make([]byte, need)
	}
	dst = dst[:need]

	seq := nextSeq()
	now := time.Now().UnixNano()
	wire.EncodeInto(dst, seq, now, dst[wire.HeaderLen:])
	g.fillRandom(dst[wire.HeaderLen:])
	return dst
}
