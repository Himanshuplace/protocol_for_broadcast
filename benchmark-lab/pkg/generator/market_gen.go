package generator

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/himanshuplace/protocol_for_broadcast/pkg/market"
	"github.com/himanshuplace/protocol_for_broadcast/pkg/wire"
)

// MarketTickGenerator simulates an exchange-grade market data feed.
//
// Price model: Discrete Geometric Brownian Motion (GBM)
//
//	S(t+dt) = S(t) * exp((μ - σ²/2)*dt + σ*Z*sqrt(dt))
//
// where:
//   - S(t) = current price
//   - μ = drift (set to 0 for benchmarking — we don't care about long-run price levels)
//   - σ = annualized volatility per instrument (e.g., 0.20 for AAPL)
//   - Z = standard normal random variable (Box-Muller transform from PCG64 PRNG)
//   - dt = 1/TickRateHz (time between ticks for this instrument)
//
// This produces realistic, non-trivial byte sequences with the statistical
// properties of real market data — not the uniform random bytes from RandomGenerator.
//
// Per-instrument price state is stored as atomic.Int64 (micros) to allow
// concurrent read by Stats() while the generator loop writes new prices.
// No mutex needed because int64 reads/writes are atomic on AMD64.
type MarketTickGenerator struct {
	universe []market.Instrument
	prices   []atomicPrice // current mid price per instrument (index aligned with universe)
	seq      atomic.Uint64
	rng      *RandomGenerator
}

// atomicPrice holds an instrument's current price with cache-line isolation.
// The [56]byte pad ensures each atomicPrice occupies its own cache line (64 bytes),
// preventing false sharing between adjacent instruments during concurrent access.
type atomicPrice struct {
	value atomic.Int64
	_     [56]byte // cache-line padding: 8B atomic + 56B pad = 64B
}

// NewMarketTickGenerator creates a generator for the given instrument universe.
// Initial prices are taken from Instrument.MidPrice.
func NewMarketTickGenerator(universe []market.Instrument) *MarketTickGenerator {
	g := &MarketTickGenerator{
		universe: universe,
		prices:   make([]atomicPrice, len(universe)),
		rng:      NewRandomGenerator(),
	}
	for i, inst := range universe {
		g.prices[i].value.Store(inst.MidPrice)
	}
	return g
}

// NextTick generates one price tick for the instrument at instrIdx.
// Writes the result into out (zero-allocation).
// seq is assigned from the generator's monotonic counter.
func (g *MarketTickGenerator) NextTick(instrIdx int, out *market.MarketTick) {
	inst := &g.universe[instrIdx]
	dt := 1.0 / inst.TickRateHz // time step in seconds

	// Generate Z ~ N(0,1) using Box-Muller transform
	// Two independent U(0,1) values from PCG64
	u1 := g.rng.rng.Float64()
	u2 := g.rng.rng.Float64()
	if u1 <= 0 {
		u1 = 1e-10 // avoid log(0)
	}
	z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)

	// GBM price update (pure diffusion, μ=0)
	sigma := inst.Volatility
	// dt is in seconds; volatility is annualized; scale to per-tick
	perTickVol := sigma * math.Sqrt(dt/252.0/6.5/3600.0) // approx per-tick vol
	lnReturn := -0.5*perTickVol*perTickVol + perTickVol*z

	// Apply price update atomically
	// CAS loop: read current, compute new, store if unchanged
	for {
		old := g.prices[instrIdx].value.Load()
		// newPrice = old * exp(lnReturn), computed in integer micros
		newMid := int64(float64(old) * math.Exp(lnReturn))
		if newMid < 1 {
			newMid = 1 // floor at 1 micro (price can't go negative)
		}
		if g.prices[instrIdx].value.CompareAndSwap(old, newMid) {
			// Compute bid/ask from mid and spread
			spreadMicros := int64(float64(newMid) * float64(inst.SpreadBps) / 10000.0)
			if spreadMicros < 1 {
				spreadMicros = 1
			}
			bid := newMid - spreadMicros/2
			ask := newMid + spreadMicros/2

			out.SeqNum = g.seq.Add(1)
			out.Timestamp = time.Now().UnixNano()
			out.RecvNs = 0
			out.InstrHash = inst.SymbolHash()
			out.BidPrice = bid
			out.AskPrice = ask
			out.LastPrice = newMid
			out.Volume = uint32(g.rng.rng.IntN(1000) + 100)
			out.BidSize = uint16(g.rng.rng.IntN(500) + 100)
			out.AskSize = uint16(g.rng.rng.IntN(500) + 100)
			out.Flags = 0
			out.Class = inst.Class
			break
		}
	}
}

// NextTickEncoded generates a tick for instrIdx and encodes it into a wire frame.
// Returns the encoded bytes (64-byte tick + 24-byte wire header = 88 bytes).
// Zero-allocation on the tick encoding; one allocation for the output slice.
func (g *MarketTickGenerator) NextTickEncoded(instrIdx int) []byte {
	var tick market.MarketTick
	g.NextTick(instrIdx, &tick)

	var tickBuf [market.TickSize]byte
	tick.Encode(&tickBuf)

	// Wrap in wire frame for sequence tracking and latency measurement
	// Wire header uses tick.SeqNum and tick.Timestamp for correlation
	return wire.Encode(tick.SeqNum, tick.Timestamp, tickBuf[:])
}

// RunFeed starts per-instrument goroutines that generate ticks at each instrument's
// TickRateHz and send them to the out channel.
// Call cancel() to stop all goroutines.
// The channel consumer is responsible for draining out before checking ctx.Done().
//
// Each instrument has its own goroutine and ticker to match the configured tick rate.
// This accurately models the bursty, per-instrument arrival pattern of a real feed.
func (g *MarketTickGenerator) RunFeed(ctx context.Context, out chan<- *market.MarketTick) {
	for i := range g.universe {
		i := i // capture loop variable
		inst := &g.universe[i]

		// Compute tick interval for this instrument
		interval := time.Duration(float64(time.Second) / inst.TickRateHz)
		if interval < time.Microsecond {
			interval = time.Microsecond // floor to 1µs to avoid spin loop
		}

		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			// Pre-allocate MarketTick to reuse across ticks for this goroutine.
			// The tick is sent on the channel; receiver must copy it if needed beyond the call.
			tick := &market.MarketTick{}
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.NextTick(i, tick)
					// Non-blocking send: drop if channel is full (simulates packet loss at publisher)
					select {
					case out <- tick:
						tick = &market.MarketTick{} // allocate new tick for next round
					default:
						// channel full: drop this tick
					}
				}
			}
		}()
	}
}

// CurrentPrice returns the current mid price for instrument instrIdx.
//
//go:nosplit
func (g *MarketTickGenerator) CurrentPrice(instrIdx int) int64 {
	return g.prices[instrIdx].value.Load()
}

// TotalTicksGenerated returns the global tick sequence counter.
//
//go:nosplit
func (g *MarketTickGenerator) TotalTicksGenerated() uint64 {
	return g.seq.Load()
}
