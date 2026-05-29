package network

// Profile describes a network impairment condition applied via tc/netem.
type Profile struct {
	Name         string
	LossPct      float64
	DelayMs      int
	JitterMs     int
	DuplicatePct float64
	ReorderPct   float64
	CorruptPct   float64
	Rate         string // bandwidth limit, e.g. "100mbit"; empty = unlimited
}

// Profiles is the canonical set of named network conditions.
var Profiles = map[string]Profile{
	"clean": {Name: "clean"},
	"loss1": {Name: "loss1", LossPct: 1.0},
	"loss5": {Name: "loss5", LossPct: 5.0},
	"loss10": {Name: "loss10", LossPct: 10.0},
	"loss20": {Name: "loss20", LossPct: 20.0},
	"latency20": {Name: "latency20", DelayMs: 20, JitterMs: 2},
	"latency50": {Name: "latency50", DelayMs: 50, JitterMs: 5},
	"latency100": {Name: "latency100", DelayMs: 100, JitterMs: 10},
	"reorder": {Name: "reorder", DelayMs: 10, ReorderPct: 25.0},
	"duplicate": {Name: "duplicate", DuplicatePct: 1.0},
	"jitter": {Name: "jitter", DelayMs: 20, JitterMs: 15},
	"wan": {Name: "wan", DelayMs: 50, JitterMs: 10, LossPct: 0.5},
	"mobile4g": {Name: "mobile4g", DelayMs: 40, JitterMs: 20, LossPct: 1.0},
	"mobile3g": {Name: "mobile3g", DelayMs: 100, JitterMs: 30, LossPct: 2.0, Rate: "1mbit"},
	"lossburst": {Name: "lossburst", LossPct: 1.0, ReorderPct: 5.0},
	"satellite": {Name: "satellite", DelayMs: 600, JitterMs: 50, LossPct: 0.2},
}
