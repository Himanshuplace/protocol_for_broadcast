package transport

import (
	"sync"
	"time"
)

// BaseTransport provides common lifecycle fields embedded by every protocol implementation.
// Embedding BaseTransport gives the implementation IsStarted(), Protocol(), Uptime(),
// MarkStarted(), and MarkStopped() for free — avoiding duplicated boilerplate.
type BaseTransport struct {
	mu        sync.RWMutex
	protocol  string
	startTime time.Time
	started   bool
}

// SetProtocol sets the protocol name reported in Stats().
// Call this in the implementation's constructor, before Start().
func (b *BaseTransport) SetProtocol(name string) {
	b.mu.Lock()
	b.protocol = name
	b.mu.Unlock()
}

// Protocol returns the protocol name.
func (b *BaseTransport) Protocol() string {
	b.mu.RLock()
	v := b.protocol
	b.mu.RUnlock()
	return v
}

// MarkStarted records the start time. Call at the beginning of Start().
func (b *BaseTransport) MarkStarted() {
	b.mu.Lock()
	b.startTime = time.Now()
	b.started = true
	b.mu.Unlock()
}

// MarkStopped resets the started flag. Call at the end of Stop().
func (b *BaseTransport) MarkStopped() {
	b.mu.Lock()
	b.started = false
	b.mu.Unlock()
}

// IsStarted returns true if Start() has been called and Stop() has not.
func (b *BaseTransport) IsStarted() bool {
	b.mu.RLock()
	v := b.started
	b.mu.RUnlock()
	return v
}

// Uptime returns the duration since Start() was called.
// Returns 0 if the transport has not been started.
func (b *BaseTransport) Uptime() time.Duration {
	b.mu.RLock()
	if !b.started {
		b.mu.RUnlock()
		return 0
	}
	d := time.Since(b.startTime)
	b.mu.RUnlock()
	return d
}

// BaseStats returns a Stats struct populated with the base fields.
// Protocol implementations call this and add their own fields.
func (b *BaseTransport) BaseStats() Stats {
	b.mu.RLock()
	protocol := b.protocol
	uptime := time.Duration(0)
	if b.started {
		uptime = time.Since(b.startTime)
	}
	b.mu.RUnlock()
	return Stats{
		Protocol:   protocol,
		Uptime:     uptime,
		SnapshotAt: time.Now(),
	}
}
