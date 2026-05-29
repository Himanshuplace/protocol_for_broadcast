package metrics

import (
	"runtime"
	"sync"
	"time"
)

// ResourcePoint is a single CPU/memory/goroutine/FD sample captured at one instant.
type ResourcePoint struct {
	Ts         time.Time
	CPUPercent float64 // process CPU utilization (0–100 × NumCPU)
	MemBytes   uint64  // process RSS memory in bytes
	Goroutines int     // live goroutine count
	FDs        int     // open file descriptors (-1 if unavailable on this OS)
	NetRxBytes uint64  // network bytes received since last sample (delta)
	NetTxBytes uint64  // network bytes sent since last sample (delta)
}

// ResourceSnapshot holds aggregated stats over the sampling window.
type ResourceSnapshot struct {
	Samples    int
	CPUAvg     float64
	CPUP99     float64
	MemAvg     uint64
	MemMax     uint64
	GoroutineAvg int
	GoroutineMax int
	FDAvg      int
	FDMax      int
}

// ResourceSampler continuously samples process resource usage every interval.
// Uses runtime.ReadMemStats for memory (cross-platform) and platform-specific
// /proc parsing for CPU on Linux. Falls back to runtime.NumCPU heuristics elsewhere.
//
// Background goroutine lifecycle:
//   - Start() launches the sampling goroutine
//   - Stop() signals shutdown and waits for the goroutine to exit
type ResourceSampler struct {
	interval time.Duration
	mu       sync.RWMutex
	samples  []ResourcePoint
	done     chan struct{}
	stopped  chan struct{}

	// CPU accounting state
	lastCPUTime cpuTime
	lastWall    time.Time
}

// NewResourceSampler creates a sampler with the given interval.
// Recommended: 100ms for production benchmarks (low overhead, sufficient resolution).
func NewResourceSampler(interval time.Duration) *ResourceSampler {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	return &ResourceSampler{
		interval: interval,
		done:     make(chan struct{}),
		stopped:  make(chan struct{}),
		samples:  make([]ResourcePoint, 0, 1024),
	}
}

// Start begins the background sampling goroutine.
func (s *ResourceSampler) Start() {
	s.lastWall = time.Now()
	s.lastCPUTime = readCPUTime()
	go s.loop()
}

// Stop signals the sampler to stop and waits for the goroutine to exit.
func (s *ResourceSampler) Stop() {
	close(s.done)
	<-s.stopped
}

func (s *ResourceSampler) loop() {
	defer close(s.stopped)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p := s.sample()
			s.mu.Lock()
			s.samples = append(s.samples, p)
			s.mu.Unlock()
		case <-s.done:
			return
		}
	}
}

func (s *ResourceSampler) sample() ResourcePoint {
	now := time.Now()

	// Memory via runtime.ReadMemStats (cross-platform, ~1µs overhead)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// CPU: compute Δ(cpu_time) / Δ(wall_time) since last sample
	curCPU := readCPUTime()
	curWall := now
	wallDelta := curWall.Sub(s.lastWall).Seconds()
	cpuPct := 0.0
	if wallDelta > 0 {
		cpuDelta := curCPU.total() - s.lastCPUTime.total()
		cpuPct = (cpuDelta / wallDelta) * 100.0
	}
	s.lastCPUTime = curCPU
	s.lastWall = curWall

	return ResourcePoint{
		Ts:         now,
		CPUPercent: cpuPct,
		MemBytes:   ms.Sys, // total memory obtained from OS
		Goroutines: runtime.NumGoroutine(),
		FDs:        countFDs(),
	}
}

// Snapshot returns aggregated statistics over all collected samples.
// Safe to call while sampling is running.
func (s *ResourceSampler) Snapshot() ResourceSnapshot {
	s.mu.RLock()
	samples := make([]ResourcePoint, len(s.samples))
	copy(samples, s.samples)
	s.mu.RUnlock()

	if len(samples) == 0 {
		return ResourceSnapshot{}
	}

	var snap ResourceSnapshot
	snap.Samples = len(samples)

	// Sort CPU samples for P99 — use a simple selection approach
	cpuValues := make([]float64, len(samples))
	totalCPU := 0.0
	totalMem := uint64(0)
	totalGoroutines := 0
	totalFDs := 0

	for i, p := range samples {
		cpuValues[i] = p.CPUPercent
		totalCPU += p.CPUPercent
		totalMem += p.MemBytes
		totalGoroutines += p.Goroutines
		totalFDs += p.FDs
		if p.MemBytes > snap.MemMax {
			snap.MemMax = p.MemBytes
		}
		if p.Goroutines > snap.GoroutineMax {
			snap.GoroutineMax = p.Goroutines
		}
		if p.FDs > snap.FDMax {
			snap.FDMax = p.FDs
		}
	}

	n := len(samples)
	snap.CPUAvg = totalCPU / float64(n)
	snap.MemAvg = totalMem / uint64(n)
	snap.GoroutineAvg = totalGoroutines / n
	snap.FDAvg = totalFDs / n

	// Approximate P99 by sorting the CPU slice
	sortFloat64(cpuValues)
	p99Idx := int(float64(n)*0.99 + 0.5)
	if p99Idx >= n {
		p99Idx = n - 1
	}
	snap.CPUP99 = cpuValues[p99Idx]

	return snap
}

// Reset clears all samples. Call between warmup and measurement phases.
func (s *ResourceSampler) Reset() {
	s.mu.Lock()
	s.samples = s.samples[:0]
	s.lastWall = time.Now()
	s.lastCPUTime = readCPUTime()
	s.mu.Unlock()
}

// sortFloat64 is a simple insertion sort for small slices (N < 10K is typical).
func sortFloat64(a []float64) {
	for i := 1; i < len(a); i++ {
		v := a[i]
		j := i - 1
		for j >= 0 && a[j] > v {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = v
	}
}

// cpuTime holds user+system CPU time in seconds.
type cpuTime struct {
	user   float64
	system float64
}

func (c cpuTime) total() float64 { return c.user + c.system }
