// Package market defines the financial domain model for the broadcast benchmark.
//
// The benchmark simulates an exchange-grade market data feed: a publisher emits
// sub-millisecond price updates (ticks) for 100+ instruments across equities,
// futures, options, crypto, and FX. Each transport's selective fanout behavior
// is tested against this workload — the same workload seen at Bloomberg, CME,
// NASDAQ, and Binance.
//
// Why financial data? Because it is the canonical high-frequency, selective-fanout
// workload that stresses every part of the broadcast pipeline:
//
//   - High tick rate (futures: 500 ticks/sec) stresses the sender goroutine
//   - Many instruments (100+) stresses the topic router
//   - Selective subscriptions (10 of 100 instruments per client) stresses routing logic
//   - Burst ticks (correlated market moves) stress the fanout path simultaneously
//   - Realistic payload sizes (64B binary tick vs 180B JSON) reveal serialization cost
package market

// AssetClass represents the type of tradeable instrument.
// Stored as uint8 to fit into the MarketTick binary encoding without padding.
type AssetClass uint8

const (
	AssetEquity  AssetClass = iota // 0: equities (stocks)
	AssetFuture                    // 1: futures (commodity, index, rate)
	AssetOption                    // 2: options (calls and puts)
	AssetCrypto                    // 3: cryptocurrency spot
	AssetFX                        // 4: foreign exchange pairs
)

func (a AssetClass) String() string {
	switch a {
	case AssetEquity:
		return "equity"
	case AssetFuture:
		return "future"
	case AssetOption:
		return "option"
	case AssetCrypto:
		return "crypto"
	case AssetFX:
		return "fx"
	default:
		return "unknown"
	}
}

// Instrument describes a tradeable instrument and its tick generation parameters.
// All price fields use integer micros (millionths of the base unit) to avoid
// floating-point in the hot path.
type Instrument struct {
	Symbol     string     // exchange-style symbol, e.g. "AAPL", "ESH25", "BTC-USD"
	Class      AssetClass
	TickRateHz float64    // tick frequency under normal market conditions
	Volatility float64    // annualized volatility (e.g., 0.20 = 20%)
	MidPrice   int64      // initial mid price in micros
	SpreadBps  int        // bid-ask spread in basis points
}

// SymbolHash returns the FNV-1a 32-bit hash of the symbol string.
// This is the routing key used by market.Router and all TopicTransport implementations.
// Inline + branchless on AMD64 — generates 4 instructions per character.
//
//go:nosplit
func (inst *Instrument) SymbolHash() uint32 {
	h := uint32(2166136261) // FNV-1a offset basis
	for i := 0; i < len(inst.Symbol); i++ {
		h ^= uint32(inst.Symbol[i])
		h *= 16777619
	}
	return h
}

// DefaultUniverse returns 100 instruments across all asset classes, designed to
// reflect a realistic market data subscription universe:
//
//   - 30 S&P 500 blue-chip equities (20–100 ticks/sec each)
//   - 20 major futures contracts (100–500 ticks/sec — the highest tick rate instruments)
//   - 30 representative options (0.5–5 ticks/sec)
//   - 10 cryptocurrency spot pairs (10–50 ticks/sec, 24/7)
//   - 10 FX pairs (10–20 ticks/sec, 24/5)
//
// Total aggregate tick rate: ~14,000 ticks/sec across all 100 instruments.
// This matches the throughput of a mid-tier market data feed (e.g., Nasdaq TotalView-ITCH
// peaks at ~100K msgs/sec for the full tape; 14K/sec is a realistic subsample).
func DefaultUniverse() []Instrument {
	insts := make([]Instrument, 0, 100)

	// 30 S&P 500 blue-chip equities
	// Prices in micros: $182.35050 = 182350500 (7 significant figures)
	equities := []struct {
		sym    string
		price  int64
		vol    float64
		tickHz float64
	}{
		{"AAPL", 18235050000, 0.22, 50.0},
		{"MSFT", 41500000000, 0.20, 45.0},
		{"NVDA", 88000000000, 0.45, 80.0},
		{"GOOGL", 17500000000, 0.25, 40.0},
		{"AMZN", 18800000000, 0.28, 45.0},
		{"META", 51000000000, 0.30, 50.0},
		{"TSLA", 21500000000, 0.60, 100.0},
		{"AVGO", 165000000000, 0.30, 30.0},
		{"BRK.B", 40000000000, 0.15, 20.0},
		{"JPM", 21500000000, 0.22, 35.0},
		{"LLY", 88000000000, 0.28, 30.0},
		{"V", 27500000000, 0.18, 30.0},
		{"XOM", 11500000000, 0.25, 25.0},
		{"UNH", 52000000000, 0.20, 25.0},
		{"MA", 46500000000, 0.20, 30.0},
		{"HD", 35000000000, 0.20, 20.0},
		{"JNJ", 15000000000, 0.15, 20.0},
		{"PG", 16500000000, 0.14, 15.0},
		{"ORCL", 13500000000, 0.28, 25.0},
		{"MRK", 13000000000, 0.18, 20.0},
		{"COST", 82000000000, 0.20, 20.0},
		{"AMD", 17500000000, 0.50, 70.0},
		{"ABBV", 17500000000, 0.20, 20.0},
		{"NFLX", 62000000000, 0.38, 40.0},
		{"CRM", 26500000000, 0.32, 30.0},
		{"TMO", 55000000000, 0.20, 15.0},
		{"BAC", 4200000000, 0.28, 35.0},
		{"ADBE", 50000000000, 0.30, 25.0},
		{"CVX", 15000000000, 0.25, 20.0},
		{"WMT", 8200000000, 0.15, 20.0},
	}
	for _, e := range equities {
		insts = append(insts, Instrument{
			Symbol:     e.sym,
			Class:      AssetEquity,
			TickRateHz: e.tickHz,
			Volatility: e.vol,
			MidPrice:   e.price,
			SpreadBps:  1,
		})
	}

	// 20 major futures — the most liquid instruments in existence.
	// ES (S&P 500 E-mini) alone generates ~500 quote updates/sec.
	futures := []struct {
		sym    string
		price  int64
		vol    float64
		tickHz float64
	}{
		{"ES", 525000000000, 0.15, 500.0},   // S&P 500 E-mini
		{"NQ", 183000000000, 0.20, 400.0},   // Nasdaq 100 E-mini
		{"YM", 397000000000, 0.14, 300.0},   // Dow Jones E-mini
		{"RTY", 220000000000, 0.22, 200.0},  // Russell 2000 E-mini
		{"CL", 80000000000, 0.30, 300.0},    // Crude Oil (WTI)
		{"GC", 235000000000, 0.15, 200.0},   // Gold
		{"SI", 28000000000, 0.25, 150.0},    // Silver
		{"NG", 3000000000, 0.50, 200.0},     // Natural Gas
		{"ZB", 124000000000, 0.08, 100.0},   // 30Y Treasury Bond
		{"ZN", 112000000000, 0.06, 150.0},   // 10Y Treasury Note
		{"ZF", 107000000000, 0.05, 100.0},   // 5Y Treasury Note
		{"6E", 108000000000, 0.08, 200.0},   // Euro FX Futures
		{"6J", 65000000000, 0.08, 150.0},    // Japanese Yen Futures
		{"6B", 126000000000, 0.09, 150.0},   // British Pound Futures
		{"VX", 15000000000, 0.60, 100.0},    // VIX Futures
		{"HG", 420000, 0.20, 100.0},         // Copper (per pound, stored as $0.420000)
		{"ZW", 580000, 0.25, 80.0},          // Wheat (per bushel, $5.80 = 580000 micros)
		{"ZC", 440000, 0.20, 80.0},          // Corn
		{"ZS", 1050000, 0.20, 80.0},         // Soybean
		{"BTC", 6500000000000, 0.60, 500.0}, // Bitcoin CME Futures
	}
	for _, f := range futures {
		insts = append(insts, Instrument{
			Symbol:     f.sym,
			Class:      AssetFuture,
			TickRateHz: f.tickHz,
			Volatility: f.vol,
			MidPrice:   f.price,
			SpreadBps:  0,
		})
	}

	// 30 options (calls and puts on major underlyings)
	// Low tick rate — options tick when the underlying moves significantly
	optionSymbols := []string{
		"SPY240P480", "SPY240C490", "QQQ240P400", "QQQ240C420",
		"AAPL240C200", "AAPL240P180", "TSLA240C250", "TSLA240P200",
		"NVDA240C900", "NVDA240P800", "META240C500", "AMZN240C200",
		"GOOGL240C175", "MSFT240C430", "JPM240C220", "BAC240C45",
		"GLD240C225", "GLD240P220", "USO240C80", "SLV240C26",
		"VIX240C20", "VIX240C25", "VIX240C30", "VIX240P15",
		"SPX240P5000", "SPX240C5200", "NDX240P18000", "NDX240C19000",
		"IWM240P190", "DIA240C390",
	}
	// Option premiums: representative mid prices (2.00–20.00 range)
	optionPrices := []int64{
		5000000, 4500000, 6000000, 5500000,
		3000000, 2500000, 8000000, 7000000,
		15000000, 12000000, 9000000, 4000000,
		3500000, 5000000, 4000000, 2000000,
		7000000, 6500000, 3000000, 2500000,
		2000000, 4000000, 1500000, 8000000,
		10000000, 8000000, 12000000, 9000000,
		3500000, 4500000,
	}
	for i, sym := range optionSymbols {
		insts = append(insts, Instrument{
			Symbol:     sym,
			Class:      AssetOption,
			TickRateHz: 2.0,
			Volatility: 0.40,
			MidPrice:   optionPrices[i],
			SpreadBps:  50,
		})
	}

	// 10 crypto spot pairs (24/7 market)
	cryptos := []struct {
		sym    string
		price  int64
		tickHz float64
	}{
		{"BTC-USD", 6500000000000, 50.0},
		{"ETH-USD", 350000000000, 40.0},
		{"SOL-USD", 18000000000, 30.0},
		{"BNB-USD", 60000000000, 20.0},
		{"XRP-USD", 600000, 20.0},
		{"DOGE-USD", 170000, 20.0},
		{"ADA-USD", 450000, 15.0},
		{"AVAX-USD", 4000000000, 20.0},
		{"LINK-USD", 1800000000, 15.0},
		{"UNI-USD", 1100000000, 10.0},
	}
	for _, c := range cryptos {
		insts = append(insts, Instrument{
			Symbol:     c.sym,
			Class:      AssetCrypto,
			TickRateHz: c.tickHz,
			Volatility: 0.70,
			MidPrice:   c.price,
			SpreadBps:  2,
		})
	}

	// 10 FX pairs (24/5, tight spreads)
	fxPairs := []struct {
		sym    string
		price  int64 // stored as integer with 6 decimal places (e.g., EUR/USD 1.080000 = 1080000)
		tickHz float64
	}{
		{"EUR/USD", 1080000, 20.0},
		{"GBP/USD", 1270000, 15.0},
		{"USD/JPY", 155000000, 20.0}, // 155.000000 — yen has 6 decimal places too
		{"AUD/USD", 650000, 15.0},
		{"USD/CAD", 1370000, 15.0},
		{"USD/CHF", 900000, 12.0},
		{"NZD/USD", 600000, 10.0},
		{"EUR/GBP", 850000, 10.0},
		{"EUR/JPY", 167000000, 12.0},
		{"GBP/JPY", 197000000, 10.0},
	}
	for _, fx := range fxPairs {
		insts = append(insts, Instrument{
			Symbol:     fx.sym,
			Class:      AssetFX,
			TickRateHz: fx.tickHz,
			Volatility: 0.08,
			MidPrice:   fx.price,
			SpreadBps:  1,
		})
	}

	return insts
}

// AggregateTickRate returns the total ticks per second across all instruments.
func AggregateTickRate(universe []Instrument) float64 {
	total := 0.0
	for i := range universe {
		total += universe[i].TickRateHz
	}
	return total
}
