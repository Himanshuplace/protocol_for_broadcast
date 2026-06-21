// k6 WebSocket subscriber — independent cross-check of the Go broadcast harness.
//
// Topology:
//   cmd/websocket-server  (publishes wire-framed market data)  --->  k6 VUs (subscribe)
//
// What this validates (and what it does NOT):
//   ✓ Throughput   — messages/sec a standard third-party client can sustain
//   ✓ Delivery     — total messages received per subscriber
//   ✓ Ordering     — sequence gaps (loss/reorder) decoded from the wire header
//   ✗ Precise latency — k6's Date.now() is MILLISECOND granular. On loopback the
//     real latency is microseconds, so k6 reports ~0-1ms. Treat k6 latency as a
//     sanity bound only; the Go HDR histogram is the authoritative µs instrument.
//
// Run:
//   k6 run -e TARGET=ws://localhost:9000/ws -e VUS=20 -e DURATION=30 k6/websocket.js
//
// Stream to Grafana (Prometheus remote-write):
//   K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
//   k6 run -o experimental-prometheus-rw -e TARGET=ws://localhost:9000/ws k6/websocket.js

import { WebSocket } from 'k6/experimental/websockets';
import { setTimeout } from 'k6/timers';
import { Counter, Trend, Gauge } from 'k6/metrics';

const TARGET = __ENV.TARGET || 'ws://localhost:9000/ws';
const VUS = Number(__ENV.VUS || 10);
const DURATION = Number(__ENV.DURATION || 30); // seconds
const MAGIC = 0xbeefcafe;

const msgsReceived = new Counter('ws_msgs_received');
const bytesReceived = new Counter('ws_bytes_received');
const latencyMs = new Trend('ws_latency_ms', true);
const seqGaps = new Counter('ws_seq_gaps');
const badMagic = new Counter('ws_bad_magic');
const recvRate = new Gauge('ws_recv_rate_per_vu');

export const options = {
  scenarios: {
    subscribers: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: `${DURATION + 15}s`,
    },
  },
  thresholds: {
    // No correctness loss expected over a reliable WebSocket (TCP-backed).
    ws_seq_gaps: ['count==0'],
    ws_bad_magic: ['count==0'],
  },
};

export default function () {
  const ws = new WebSocket(TARGET);
  ws.binaryType = 'arraybuffer';

  let received = 0;
  let lastSeq = -1n;
  const startMs = Date.now();

  ws.onopen = () => {
    // The server broadcasts to every connection automatically; no subscribe
    // message is needed. Just stay connected for DURATION, then close.
    setTimeout(() => ws.close(), DURATION * 1000);
  };

  ws.onmessage = (e) => {
    const buf = e.data;
    if (!(buf instanceof ArrayBuffer) || buf.byteLength < 24) return;
    const dv = new DataView(buf);

    const magic = dv.getUint32(0, true);
    if (magic !== MAGIC) {
      badMagic.add(1);
      return;
    }

    const seq = dv.getBigUint64(4, true);
    const sendNs = dv.getBigInt64(12, true);

    // Sequence gap detection (per VU). seq is monotonic per server.
    if (lastSeq >= 0n && seq > lastSeq + 1n) {
      seqGaps.add(Number(seq - lastSeq - 1n));
    }
    lastSeq = seq;

    // Coarse latency: ms resolution only (see header note).
    const sendMs = Number(sendNs / 1000000n);
    const lat = Date.now() - sendMs;
    if (lat >= 0 && lat < 60000) latencyMs.add(lat);

    received++;
    msgsReceived.add(1);
    bytesReceived.add(buf.byteLength);
  };

  ws.onclose = () => {
    const elapsedS = (Date.now() - startMs) / 1000;
    if (elapsedS > 0) recvRate.add(received / elapsedS);
  };

  ws.onerror = (e) => {
    console.error(`ws error: ${e.error || e}`);
  };
}
