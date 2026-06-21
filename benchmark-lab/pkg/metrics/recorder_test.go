package metrics

import (
	"strconv"
	"testing"
	"time"
)

// TestRecordRecvFrom_NoFanoutInflation is the regression test for the
// multi-receiver inflation bug: when N subscribers each receive the same
// broadcast sequence, the recorder must NOT count the N-1 fan-out copies as
// duplicates, and throughput must be reported per subscriber (not N×).
func TestRecordRecvFrom_NoFanoutInflation(t *testing.T) {
	const (
		conns = 5
		msgs  = 100
	)
	rec := NewRecorder(RecorderConfig{Label: "test", Protocol: "udp", Scenario: "A"})
	rec.Start()
	defer rec.Stop()

	send := time.Now().UnixNano()
	recv := send + int64(time.Millisecond)
	// Every connection receives every broadcast seq exactly once — a healthy
	// broadcast with no real loss, duplication, or reordering.
	for seq := uint64(1); seq <= msgs; seq++ {
		for c := 0; c < conns; c++ {
			rec.RecordRecvFrom("conn-"+strconv.Itoa(c), seq, send, 100, recv)
		}
	}

	snap := rec.Snapshot()

	if snap.Seq.Duplicated != 0 {
		t.Errorf("fan-out copies miscounted as duplicates: got %d, want 0", snap.Seq.Duplicated)
	}
	if snap.Seq.Reordered != 0 {
		t.Errorf("unexpected reorders: got %d, want 0", snap.Seq.Reordered)
	}
	if snap.MsgRecv != msgs {
		t.Errorf("throughput not per-subscriber: MsgRecv=%d, want %d (got N× inflation?)", snap.MsgRecv, msgs)
	}
}

// TestRecordRecv_SingleStreamUnaffected guards backward compatibility: the
// standalone single-connection path (used by cmd/*-client) still detects a real
// duplicate and reports the raw receive count.
func TestRecordRecv_SingleStreamUnaffected(t *testing.T) {
	rec := NewRecorder(RecorderConfig{Label: "test", Protocol: "tcp", Scenario: "A"})
	rec.Start()
	defer rec.Stop()

	send := time.Now().UnixNano()
	recv := send + int64(time.Millisecond)
	rec.RecordRecv(1, send, 100, recv)
	rec.RecordRecv(2, send, 100, recv)
	rec.RecordRecv(2, send, 100, recv) // a genuine duplicate

	snap := rec.Snapshot()
	if snap.MsgRecv != 3 {
		t.Errorf("MsgRecv=%d, want 3", snap.MsgRecv)
	}
	if snap.Seq.Duplicated != 1 {
		t.Errorf("real duplicate not detected: got %d, want 1", snap.Seq.Duplicated)
	}
}
