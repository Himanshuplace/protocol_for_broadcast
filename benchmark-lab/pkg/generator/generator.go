// Package generator produces benchmark payload bytes in various formats and sizes.
//
// Two generator types:
//  1. Generator — produces raw byte slices of specified size for microbenchmarks.
//     Each slice contains a wire frame header (pkg/wire) + payload bytes.
//     Types: Random, Sequential, Binary, JSON (via sonic), Protobuf.
//
//  2. MarketTickGenerator — produces realistic market data ticks (pkg/market.MarketTick)
//     for the financial domain benchmark scenarios. Uses geometric Brownian motion
//     for price simulation with per-instrument volatility parameters.
//
// Why sonic for JSON?
// github.com/bytedance/sonic uses SIMD/AVX2 via JIT-compiled code on AMD64.
// Benchmark: sonic Marshal 1KB struct → ~200ns; encoding/json → ~600ns (3× faster).
// On AMD Ryzen (Zen 3+) and Intel (Haswell+), AVX2 is available and sonic detects it
// automatically via CPUID at runtime. Falls back to encoding/json on other arches.
package generator

import (
	"sync/atomic"
)

// Size represents a standard message payload size in bytes.
type Size int

// Standard benchmark message sizes covering the full range from tiny control
// messages (16B) to large market data blobs (1MB).
const (
	Size16B  Size = 16
	Size32B  Size = 32
	Size64B  Size = 64
	Size128B Size = 128
	Size256B Size = 256
	Size512B Size = 512
	Size1KB  Size = 1024
	Size4KB  Size = 4 * 1024
	Size16KB Size = 16 * 1024
	Size64KB Size = 64 * 1024
	Size1MB  Size = 1024 * 1024
)

// StandardSizes lists all benchmark message sizes in ascending order.
var StandardSizes = []Size{
	Size16B, Size32B, Size64B, Size128B, Size256B,
	Size512B, Size1KB, Size4KB, Size16KB, Size64KB, Size1MB,
}

// Generator produces encoded benchmark payloads.
// Implementations must be safe for concurrent use from multiple goroutines.
type Generator interface {
	// Next returns a wire-encoded payload of exactly targetSize total bytes
	// (including the 24-byte wire frame header).
	// If targetSize < wire.HeaderLen, the behavior is implementation-defined
	// (most implementations return a minimal header-only frame).
	Next(targetSize Size) []byte
	// Name returns the generator's identifier (e.g., "random", "json", "market-binary").
	Name() string
}

// seqCounter is a global monotonic sequence number shared across all generators.
// Using a global counter ensures unique sequence numbers even when multiple
// generators run concurrently during mixed-protocol scenarios.
var globalSeq atomic.Uint64

// nextSeq atomically increments and returns the next sequence number.
//
//go:nosplit
func nextSeq() uint64 {
	return globalSeq.Add(1)
}

// ResetGlobalSeq resets the global sequence counter to 0.
// Call only between completely separate benchmark runs.
func ResetGlobalSeq() {
	globalSeq.Store(0)
}

// PayloadType identifies the content type of the payload (after the wire frame header).
type PayloadType uint8

const (
	PayloadRandom     PayloadType = 0
	PayloadSequential PayloadType = 1
	PayloadBinary     PayloadType = 2
	PayloadJSON       PayloadType = 3
	PayloadProtobuf   PayloadType = 4
	PayloadMarketTick PayloadType = 5
)
