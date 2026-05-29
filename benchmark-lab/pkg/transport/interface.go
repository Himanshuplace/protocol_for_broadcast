// Package transport defines the core interfaces and shared types for all protocol implementations.
//
// Every protocol (UDP, TCP, HTTP/1-3, WebSocket, SSE, WebTransport, WebRTC) implements
// the Transport interface. This enables the benchmark runner to be protocol-agnostic:
// it calls Start(), Broadcast(), Connections(), Stats() without knowing which protocol
// is underneath.
//
// For market data benchmarks, TopicTransport extends Transport with pub/sub semantics
// so the runner can test selective instrument-level fanout across protocols.
package transport

import "time"

// ConnID is an opaque peer identifier. Representation varies by protocol:
//   - UDP: "ip:port" (e.g., "127.0.0.1:50123")
//   - TCP/WebSocket: UUID v4 assigned at connect time
//   - HTTP/2: stream ID as decimal string
//   - QUIC: connection ID hex string
//   - WebRTC: PeerConnection session UUID
type ConnID string

// Stats is a point-in-time performance snapshot for one transport instance.
// All fields are value-typed — callers never hold a reference to internal mutable state.
// Latency fields are in nanoseconds throughout for maximum precision.
type Stats struct {
	Protocol string
	// Connection counts
	Connections int
	// Message counts
	Sent       uint64
	Received   uint64
	Lost       uint64
	Duplicated uint64
	Reordered  uint64
	// Byte counts
	BytesSent uint64
	BytesRecv uint64
	// Latency in nanoseconds (from wire frame SendNs to receive time)
	MinLatencyNs  int64
	AvgLatencyNs  int64
	P50LatencyNs  int64
	P95LatencyNs  int64
	P99LatencyNs  int64
	P999LatencyNs int64
	MaxLatencyNs  int64
	// Resource usage (sampled every 100ms)
	CPUPercent float64
	MemBytes   uint64
	Goroutines int
	FDs        int
	// Connection lifecycle
	HandshakeNs int64 // average connection establishment time
	ReconnectNs int64 // average reconnect time after disconnect
	// Snapshot metadata
	Uptime     time.Duration
	SnapshotAt time.Time
}

// Transport is the single interface every protocol implementation must satisfy.
// All methods are safe to call concurrently from multiple goroutines.
type Transport interface {
	// Start initializes and begins listening/accepting connections.
	Start() error
	// Stop gracefully shuts down the transport, closing all connections.
	Stop() error
	// Broadcast sends data to all currently connected clients.
	// Implementations choose their own fanout strategy (naive loop, worker pool, epoll).
	Broadcast(data []byte) error
	// Send delivers data to one specific client identified by ConnID.
	// Returns ErrClientNotFound if the client is no longer connected.
	Send(id ConnID, data []byte) error
	// Connections returns the current number of connected peers.
	Connections() int
	// Stats returns a point-in-time snapshot. Does not block the data path.
	Stats() Stats
}

// TopicTransport extends Transport with publish/subscribe semantics for market data.
// Protocols implement this natively (HTTP/2 streams per instrument, QUIC streams per topic)
// or via an application-layer Router in pkg/market.
type TopicTransport interface {
	Transport
	// Subscribe registers a client for one or more instrument hashes.
	// instrHashes are FNV32a hashes of instrument symbols (see market.Instrument.SymbolHash).
	Subscribe(id ConnID, instrHashes []uint32) error
	// Unsubscribe removes a client from a set of instruments.
	Unsubscribe(id ConnID, instrHashes []uint32) error
	// Publish sends data to all subscribers of the given instrument hash.
	// Returns the number of clients routed to.
	Publish(instrHash uint32, data []byte) (int, error)
}

// RecvHandler is invoked by server-side transports when inbound data arrives.
// The benchmark runner injects a handler that records latency and feeds sequence trackers.
// recvAt is time.Now() captured immediately upon receipt — minimize code between
// socket read and this call to preserve latency accuracy.
type RecvHandler func(id ConnID, data []byte, recvAt time.Time)

// ConnectHandler is called when a new peer connects. Used for subscription setup
// in TopicTransport implementations.
type ConnectHandler func(id ConnID)

// DisconnectHandler is called when a peer disconnects. Used to clean up subscriptions
// and update the sequence tracker (trigger Flush for loss accounting).
type DisconnectHandler func(id ConnID, cause error)

// TransportConfig carries startup parameters for any transport implementation.
// Protocol-specific parameters are in the corresponding implementation packages.
type TransportConfig struct {
	ListenAddr     string // e.g., "0.0.0.0:8080"
	TLSCertDir     string // directory with cert.pem + key.pem
	MaxConnections int    // 0 = unlimited
	ReadBufSize    int    // per-connection read buffer (0 = default 65536)
	WriteBufSize   int    // per-connection write buffer (0 = default 65536)
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	// Handlers injected by the benchmark runner
	OnRecv       RecvHandler
	OnConnect    ConnectHandler
	OnDisconnect DisconnectHandler
}

// TransportFactory is a constructor function registered by each protocol implementation.
// The benchmark runner uses this registry to instantiate transports by name.
type TransportFactory func(cfg TransportConfig) (Transport, error)

// Sentinel errors shared by all transport implementations.
const (
	ErrNotStarted    = transportError("transport: not started")
	ErrAlreadyStarted = transportError("transport: already started")
	ErrClientNotFound = transportError("transport: client not found")
	ErrBroadcastFailed = transportError("transport: broadcast failed")
)

type transportError string

func (e transportError) Error() string { return string(e) }
