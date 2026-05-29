// Package wire defines the binary framing protocol used by all transport implementations.
//
// Every benchmark message — regardless of the network protocol carrying it — is prefixed
// with a 24-byte header that enables cross-protocol latency measurement, sequence-based
// loss/reorder detection, and message correlation between sender and receiver processes.
//
// Wire layout (little-endian):
//
//	[0:4]   uint32  Magic   = 0xBEEFCAFE  (frame boundary marker)
//	[4:12]  uint64  SeqNum               (monotonic per-sender counter)
//	[12:20] int64   SendNs               (time.Now().UnixNano() at send time)
//	[20:24] uint32  PayloadLen           (byte count of the payload that follows)
//	[24:]   []byte  Payload              (application data of PayloadLen bytes)
//
// Design rationale:
//   - Little-endian: x86_64 and ARM64 are both little-endian; no byte-swap instructions needed.
//   - 24-byte fixed header: fits in one third of a cache line; header parse is branchless.
//   - No checksum in the header: magic bytes catch accidental misalignment; CRC is expensive
//     in the hot path and most transports have their own integrity layer (TCP, TLS, QUIC).
//     Use Checksum() separately when you need integrity verification (e.g., UDP with loss).
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"unsafe"
)

const (
	// Magic is the 4-byte frame marker: 0xBEEFCAFE.
	// Chosen to be visually distinctive and statistically unlikely in random data.
	Magic = uint32(0xBEEFCAFE)

	// HeaderLen is the fixed size of the wire frame header in bytes.
	HeaderLen = 24

	// MaxPayload is the maximum payload size (64 MB). UDP practical limit is ~65KB.
	MaxPayload = 64 * 1024 * 1024
)

var (
	// ErrInvalidMagic is returned when the magic bytes don't match.
	ErrInvalidMagic = errors.New("wire: invalid magic bytes")
	// ErrBufferTooShort is returned when the buffer is smaller than HeaderLen.
	ErrBufferTooShort = errors.New("wire: buffer too short for header")
	// ErrPayloadOverflow is returned when the declared payload length exceeds available bytes.
	ErrPayloadOverflow = errors.New("wire: payload length exceeds buffer")
	// ErrPayloadTooLarge is returned when payload length exceeds MaxPayload.
	ErrPayloadTooLarge = errors.New("wire: payload too large")
)

// Frame is a decoded wire frame.
// Payload is a zero-copy slice into the source buffer — it is only valid until the
// next call on that buffer. Copy if you need to retain it.
type Frame struct {
	SeqNum     uint64
	SendNs     int64  // sender's time.Now().UnixNano()
	PayloadLen uint32
	Payload    []byte // zero-copy reference into source buffer
}

// crcTable is precomputed once at init. Castagnoli CRC32 is hardware-accelerated
// on both Intel (SSE4.2 CRC32 instruction) and AMD (same ISA extension).
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// encPool recycles encode buffers to reduce GC pressure in the broadcast hot path.
// The pool stores *[]byte pointers; the buffer is grown as needed.
var encPool = &sync.Pool{
	New: func() any {
		// Pre-allocate for typical 1KB payloads. Will grow if needed.
		b := make([]byte, HeaderLen+1024)
		return &b
	},
}

// GetEncBuf retrieves a buffer from the pool. Call PutEncBuf when done.
// The returned buffer has length=0 and capacity >= HeaderLen+1024.
func GetEncBuf() *[]byte {
	return encPool.Get().(*[]byte)
}

// PutEncBuf returns a buffer to the pool. Do not use the buffer after calling this.
func PutEncBuf(b *[]byte) {
	*b = (*b)[:0] // reset length, retain capacity
	encPool.Put(b)
}

// Encode returns a newly-allocated wire-encoded frame.
// For zero-allocation encoding in the hot path, use EncodeInto instead.
func Encode(seq uint64, sendNs int64, payload []byte) []byte {
	out := make([]byte, HeaderLen+len(payload))
	EncodeInto(out, seq, sendNs, payload)
	return out
}

// EncodeInto writes a wire frame into dst.
// dst must have length >= HeaderLen + len(payload).
// This is the zero-allocation path — caller manages the buffer lifecycle (use encPool).
//
// On AMD64 with GOAMD64=v3, binary.LittleEndian.PutUint64 compiles to a single
// 64-bit MOV instruction with no byte-swap overhead.
func EncodeInto(dst []byte, seq uint64, sendNs int64, payload []byte) {
	// Bounds check elimination hint: compiler sees the slice is large enough.
	_ = dst[HeaderLen+len(payload)-1]
	binary.LittleEndian.PutUint32(dst[0:4], Magic)
	binary.LittleEndian.PutUint64(dst[4:12], seq)
	binary.LittleEndian.PutUint64(dst[12:20], uint64(sendNs))
	binary.LittleEndian.PutUint32(dst[20:24], uint32(len(payload)))
	copy(dst[24:], payload)
}

// EncodeAppend appends a wire-encoded frame to dst and returns the extended slice.
// Useful when building multi-message batches into a single buffer.
func EncodeAppend(dst []byte, seq uint64, sendNs int64, payload []byte) []byte {
	need := HeaderLen + len(payload)
	if cap(dst)-len(dst) < need {
		grown := make([]byte, len(dst), len(dst)+need+1024)
		copy(grown, dst)
		dst = grown
	}
	start := len(dst)
	dst = dst[:start+need]
	EncodeInto(dst[start:], seq, sendNs, payload)
	return dst
}

// EncodedLen returns the total wire-encoded length for a payload of the given size.
//
//go:nosplit
func EncodedLen(payloadLen int) int { return HeaderLen + payloadLen }

// Decode parses a wire frame from b without allocating.
// Frame.Payload is a zero-copy slice into b — valid only while b is live.
// Returns the decoded frame and the total bytes consumed (HeaderLen + PayloadLen).
func Decode(b []byte) (Frame, int, error) {
	if len(b) < HeaderLen {
		return Frame{}, 0, fmt.Errorf("%w: need %d bytes, got %d",
			ErrBufferTooShort, HeaderLen, len(b))
	}

	// Read magic first — fail fast before touching other fields.
	m := binary.LittleEndian.Uint32(b[0:4])
	if m != Magic {
		return Frame{}, 0, fmt.Errorf("%w: got 0x%08X, want 0x%08X",
			ErrInvalidMagic, m, Magic)
	}

	seq := binary.LittleEndian.Uint64(b[4:12])
	sendNs := int64(binary.LittleEndian.Uint64(b[12:20]))
	plen := binary.LittleEndian.Uint32(b[20:24])

	if int(plen) > MaxPayload {
		return Frame{}, 0, fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, plen)
	}

	total := HeaderLen + int(plen)
	if total > len(b) {
		return Frame{}, 0, fmt.Errorf("%w: need %d bytes, got %d",
			ErrPayloadOverflow, total, len(b))
	}

	return Frame{
		SeqNum:     seq,
		SendNs:     sendNs,
		PayloadLen: plen,
		Payload:    b[HeaderLen:total],
	}, total, nil
}

// DecodeHeader parses only the 24-byte header without touching the payload.
// Useful for routing decisions that don't need the payload content.
func DecodeHeader(b []byte) (seq uint64, sendNs int64, plen uint32, err error) {
	if len(b) < HeaderLen {
		return 0, 0, 0, fmt.Errorf("%w: need %d bytes, got %d",
			ErrBufferTooShort, HeaderLen, len(b))
	}
	if m := binary.LittleEndian.Uint32(b[0:4]); m != Magic {
		return 0, 0, 0, fmt.Errorf("%w: got 0x%08X", ErrInvalidMagic, m)
	}
	return binary.LittleEndian.Uint64(b[4:12]),
		int64(binary.LittleEndian.Uint64(b[12:20])),
		binary.LittleEndian.Uint32(b[20:24]),
		nil
}

// Checksum computes the Castagnoli CRC32 of the frame header fields.
// Hardware-accelerated on Intel/AMD with SSE4.2 (all x86_64 CPUs since ~2008).
// Use this for UDP frames where the transport provides no integrity check.
func Checksum(seq uint64, sendNs int64, payloadLen uint32, payload []byte) uint32 {
	var hdr [20]byte
	binary.LittleEndian.PutUint64(hdr[0:8], seq)
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(sendNs))
	binary.LittleEndian.PutUint32(hdr[16:20], payloadLen)
	h := crc32.Update(0, crcTable, hdr[:])
	return crc32.Update(h, crcTable, payload)
}

// HeaderSize returns the wire header size. Exposed as a function so callers don't
// need to import the constant directly — helps with interface stability.
//
//go:nosplit
func HeaderSize() int { return HeaderLen }

// IsZeroCopy reports whether Decode returns a zero-copy slice.
// Always true — this is a documentation helper for callers.
//
//go:nosplit
func IsZeroCopy() bool { return true }

// UnsafeString converts a byte slice to string without allocation.
// Only safe when the string is not retained after b is modified.
// Used internally in routing hot paths.
//
//go:nosplit
func UnsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
