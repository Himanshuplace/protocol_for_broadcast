package market

import (
	"encoding/binary"
	"time"
)

// TickSize is the fixed binary size of a MarketTick.
// 64 bytes = exactly one CPU cache line on x86_64 and ARM64.
// This means a single cache miss fetches one complete tick — no straddling.
const TickSize = 64

// MarketTick is the fundamental unit of market data broadcast.
//
// Binary layout (little-endian, 64 bytes total):
//
//	[0:8]   uint64  SeqNum     global publisher sequence number
//	[8:16]  int64   Timestamp  exchange timestamp in nanoseconds
//	[16:24] int64   RecvNs     server receive timestamp (for internal latency)
//	[24:28] uint32  InstrHash  FNV32a hash of Symbol (topic routing key)
//	[28:36] int64   BidPrice   best bid in micros
//	[36:44] int64   AskPrice   best ask in micros
//	[44:52] int64   LastPrice  last trade price in micros
//	[52:56] uint32  Volume     contracts/shares in last trade
//	[56:58] uint16  BidSize    quantity at best bid
//	[58:60] uint16  AskSize    quantity at best ask
//	[60]    uint8   Flags      status flags (Halt|Auction|Close)
//	[61]    uint8   Class      AssetClass enum
//	[62:64] [2]byte _          reserved padding
//
// All prices are integer micros to avoid floating-point in the hot path.
// Example: AAPL bid $182.350500 → BidPrice = 182350500
//
// Zero-allocation encoding: Encode/Decode work on caller-supplied [64]byte arrays.
type MarketTick struct {
	SeqNum    uint64
	Timestamp int64  // exchange/generator nanosecond timestamp
	RecvNs    int64  // set by server on receipt; 0 in outbound ticks
	InstrHash uint32
	BidPrice  int64
	AskPrice  int64
	LastPrice int64
	Volume    uint32
	BidSize   uint16
	AskSize   uint16
	Flags     TickFlags
	Class     AssetClass
	_         [2]byte // reserved
}

// TickFlags is a bitmask of market status indicators.
type TickFlags uint8

const (
	FlagHalt    TickFlags = 1 << 0 // trading halted
	FlagAuction TickFlags = 1 << 1 // opening/closing auction
	FlagClose   TickFlags = 1 << 2 // closing tick
	FlagFast    TickFlags = 1 << 3 // fast market (high volatility)
)

// Encode serializes t into a 64-byte little-endian binary representation.
// Zero allocation — writes directly into the caller-provided array.
// Use a [TickSize]byte on the stack or from a sync.Pool.
func (t *MarketTick) Encode(dst *[TickSize]byte) {
	binary.LittleEndian.PutUint64(dst[0:8], t.SeqNum)
	binary.LittleEndian.PutUint64(dst[8:16], uint64(t.Timestamp))
	binary.LittleEndian.PutUint64(dst[16:24], uint64(t.RecvNs))
	binary.LittleEndian.PutUint32(dst[24:28], t.InstrHash)
	binary.LittleEndian.PutUint64(dst[28:36], uint64(t.BidPrice))
	binary.LittleEndian.PutUint64(dst[36:44], uint64(t.AskPrice))
	binary.LittleEndian.PutUint64(dst[44:52], uint64(t.LastPrice))
	binary.LittleEndian.PutUint32(dst[52:56], t.Volume)
	binary.LittleEndian.PutUint16(dst[56:58], t.BidSize)
	binary.LittleEndian.PutUint16(dst[58:60], t.AskSize)
	dst[60] = uint8(t.Flags)
	dst[61] = uint8(t.Class)
	dst[62] = 0
	dst[63] = 0
}

// EncodeSlice serializes t into a byte slice.
// dst must be at least TickSize bytes. Returns dst[:TickSize].
func (t *MarketTick) EncodeSlice(dst []byte) []byte {
	_ = dst[TickSize-1] // bounds check elimination
	var arr [TickSize]byte
	t.Encode(&arr)
	copy(dst[:TickSize], arr[:])
	return dst[:TickSize]
}

// Decode deserializes a tick from a 64-byte little-endian source.
func (t *MarketTick) Decode(src *[TickSize]byte) {
	t.SeqNum = binary.LittleEndian.Uint64(src[0:8])
	t.Timestamp = int64(binary.LittleEndian.Uint64(src[8:16]))
	t.RecvNs = int64(binary.LittleEndian.Uint64(src[16:24]))
	t.InstrHash = binary.LittleEndian.Uint32(src[24:28])
	t.BidPrice = int64(binary.LittleEndian.Uint64(src[28:36]))
	t.AskPrice = int64(binary.LittleEndian.Uint64(src[36:44]))
	t.LastPrice = int64(binary.LittleEndian.Uint64(src[44:52]))
	t.Volume = binary.LittleEndian.Uint32(src[52:56])
	t.BidSize = binary.LittleEndian.Uint16(src[56:58])
	t.AskSize = binary.LittleEndian.Uint16(src[58:60])
	t.Flags = TickFlags(src[60])
	t.Class = AssetClass(src[61])
}

// DecodeSlice deserializes a tick from a byte slice of at least TickSize bytes.
func (t *MarketTick) DecodeSlice(src []byte) {
	_ = src[TickSize-1] // bounds check elimination
	var arr [TickSize]byte
	copy(arr[:], src[:TickSize])
	t.Decode(&arr)
}

// Spread returns the bid-ask spread in micros.
//
//go:nosplit
func (t *MarketTick) Spread() int64 { return t.AskPrice - t.BidPrice }

// MidPriceMicros returns the mid price (average of bid and ask) in micros.
//
//go:nosplit
func (t *MarketTick) MidPriceMicros() int64 { return (t.BidPrice + t.AskPrice) / 2 }

// Latency returns the one-way latency from Timestamp to recvNs.
// On loopback, this is the kernel scheduling + userspace processing delay.
// On distributed setups, requires NTP/PTP clock synchronization.
//
//go:nosplit
func (t *MarketTick) Latency(recvNs int64) time.Duration {
	return time.Duration(recvNs - t.Timestamp)
}

// IsHalted reports whether trading is halted for this instrument.
//
//go:nosplit
func (t *MarketTick) IsHalted() bool { return t.Flags&FlagHalt != 0 }
