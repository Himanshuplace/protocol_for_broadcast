package wire

import (
	"testing"
	"time"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	payloads := [][]byte{
		[]byte("hello benchmark"),
		make([]byte, 0),        // empty payload
		make([]byte, 1),        // single byte
		make([]byte, 1024),     // 1KB
		make([]byte, 64*1024),  // 64KB
	}
	for _, p := range payloads {
		seq := uint64(42)
		now := time.Now().UnixNano()
		encoded := Encode(seq, now, p)
		if len(encoded) != EncodedLen(len(p)) {
			t.Fatalf("len mismatch: want %d, got %d", EncodedLen(len(p)), len(encoded))
		}
		f, n, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if n != len(encoded) {
			t.Errorf("bytes consumed: want %d, got %d", len(encoded), n)
		}
		if f.SeqNum != seq {
			t.Errorf("SeqNum: want %d, got %d", seq, f.SeqNum)
		}
		if f.SendNs != now {
			t.Errorf("SendNs: want %d, got %d", now, f.SendNs)
		}
		if int(f.PayloadLen) != len(p) {
			t.Errorf("PayloadLen: want %d, got %d", len(p), f.PayloadLen)
		}
		if string(f.Payload) != string(p) {
			t.Errorf("Payload mismatch for len=%d", len(p))
		}
	}
}

func TestDecode_InvalidMagic(t *testing.T) {
	b := make([]byte, HeaderLen+4)
	b[0] = 0xDE // wrong magic
	b[1] = 0xAD
	b[2] = 0xBE
	b[3] = 0xEF
	_, _, err := Decode(b)
	if err == nil {
		t.Fatal("expected error for invalid magic")
	}
}

func TestDecode_BufferTooShort(t *testing.T) {
	_, _, err := Decode(make([]byte, HeaderLen-1))
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
}

func TestDecode_PayloadOverflow(t *testing.T) {
	b := make([]byte, HeaderLen+4)
	EncodeInto(b[:HeaderLen], 1, 123, []byte{})
	// claim 100 bytes of payload but buffer only has 4
	b[20] = 100
	b[21] = 0
	b[22] = 0
	b[23] = 0
	_, _, err := Decode(b)
	if err == nil {
		t.Fatal("expected overflow error")
	}
}

func TestEncodeInto_ZeroAlloc(t *testing.T) {
	payload := make([]byte, 1024)
	buf := make([]byte, EncodedLen(len(payload)))
	allocs := testing.AllocsPerRun(1000, func() {
		EncodeInto(buf, 1, 12345678, payload)
	})
	if allocs > 0 {
		t.Errorf("EncodeInto: expected 0 allocs, got %.0f", allocs)
	}
}

func TestDecodeHeader(t *testing.T) {
	payload := []byte("test payload")
	now := time.Now().UnixNano()
	encoded := Encode(999, now, payload)
	seq, sendNs, plen, err := DecodeHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeHeader: %v", err)
	}
	if seq != 999 {
		t.Errorf("seq: want 999, got %d", seq)
	}
	if sendNs != now {
		t.Errorf("sendNs mismatch")
	}
	if int(plen) != len(payload) {
		t.Errorf("plen: want %d, got %d", len(payload), plen)
	}
}

func TestChecksum_Deterministic(t *testing.T) {
	payload := []byte("market tick data")
	c1 := Checksum(1, 1000, uint32(len(payload)), payload)
	c2 := Checksum(1, 1000, uint32(len(payload)), payload)
	if c1 != c2 {
		t.Errorf("checksum not deterministic: %d != %d", c1, c2)
	}
	// Different sequence number must produce different checksum
	c3 := Checksum(2, 1000, uint32(len(payload)), payload)
	if c1 == c3 {
		t.Errorf("different seqnums produced same checksum")
	}
}

func TestEncodeAppend(t *testing.T) {
	var buf []byte
	for i := uint64(0); i < 5; i++ {
		buf = EncodeAppend(buf, i, int64(i*1000), []byte{byte(i)})
	}
	offset := 0
	for i := uint64(0); i < 5; i++ {
		f, n, err := Decode(buf[offset:])
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if f.SeqNum != i {
			t.Errorf("frame %d: SeqNum want %d got %d", i, i, f.SeqNum)
		}
		offset += n
	}
	if offset != len(buf) {
		t.Errorf("did not consume all bytes: offset=%d len=%d", offset, len(buf))
	}
}

// --- Benchmarks ---

func BenchmarkEncode_64B(b *testing.B) {
	benchmarkEncode(b, 64)
}

func BenchmarkEncode_1KB(b *testing.B) {
	benchmarkEncode(b, 1024)
}

func BenchmarkEncode_64KB(b *testing.B) {
	benchmarkEncode(b, 65536)
}

func benchmarkEncode(b *testing.B, size int) {
	payload := make([]byte, size)
	b.SetBytes(int64(EncodedLen(size)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Encode(uint64(i), 123456789, payload)
	}
}

func BenchmarkEncodeInto_64B(b *testing.B) {
	benchmarkEncodeInto(b, 64)
}

func BenchmarkEncodeInto_1KB(b *testing.B) {
	benchmarkEncodeInto(b, 1024)
}

func benchmarkEncodeInto(b *testing.B, size int) {
	payload := make([]byte, size)
	buf := make([]byte, EncodedLen(size))
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeInto(buf, uint64(i), 123456789, payload)
	}
}

func BenchmarkDecode_1KB(b *testing.B) {
	payload := make([]byte, 1024)
	encoded := Encode(1, 123456789, payload)
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = Decode(encoded)
	}
}

func BenchmarkDecodeHeader_1KB(b *testing.B) {
	encoded := Encode(1, 123456789, make([]byte, 1024))
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = DecodeHeader(encoded)
	}
}
