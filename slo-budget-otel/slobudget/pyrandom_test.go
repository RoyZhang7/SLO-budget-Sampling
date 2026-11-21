package slobudget

import (
	"math"
	"strconv"
	"testing"
)

// Reference values printed by CPython 3.14: random.Random(seed).random() x 4.
var pyRandomRef = map[uint64][]string{
	1001:                 {"0x1.97e2a2bb6dfc7p-1", "0x1.e0441f0ae5510p-5", "0x1.853e25ccac9f2p-1", "0x1.b9b44dfc757bfp-1"},
	1002:                 {"0x1.0ab9bfe61ff24p-1", "0x1.af5c36731c5e4p-2", "0x1.c47ee724d0158p-3", "0x1.0aee2b4706ac8p-3"},
	1003:                 {"0x1.fd4711c787cecp-2", "0x1.70a1569c58fccp-2", "0x1.71b2141da42f8p-1", "0x1.c9714cc506590p-2"},
	0:                    {"0x1.b0580f98a7dbep-1", "0x1.84129978f9c1ap-1", "0x1.aeaa51052e978p-2", "0x1.092178fb945a6p-2"},
	1<<40 + 7:            {"0x1.3a3761afd4291p-1", "0x1.a13cb57c0715ap-1", "0x1.e3d88c68edf49p-1", "0x1.240a3fed28776p-1"},
	math.MaxUint64:       {"0x1.659799fd7f980p-6", "0x1.5a35a94f333d8p-2", "0x1.b21c0274c09a4p-3", "0x1.3af1cb8614c45p-1"},
}

func hexFloat(t *testing.T, s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPyRandomMatchesCPython(t *testing.T) {
	for seed, ref := range pyRandomRef {
		r := NewPyRandom(seed)
		for i, want := range ref {
			if got := r.Random(); got != hexFloat(t, want) {
				t.Fatalf("seed %d draw %d: got %x want %s", seed, i, math.Float64bits(got), want)
			}
		}
	}
	// the twist is exercised well past the first 624 words
	r := NewPyRandom(1001)
	for i := 0; i < 10000; i++ {
		r.Random()
	}
	if got := r.Random(); got != hexFloat(t, "0x1.343750a75e6fep-1") {
		t.Fatalf("draw 10001: got %x", math.Float64bits(got))
	}
}

func TestPyFloorDivMatchesCPython(t *testing.T) {
	cases := []struct{ a, b, want float64 }{
		{10.0, 5.0, 2}, {14.999999999999998, 5.0, 2}, {15.0, 5.0, 3}, {0.0, 5.0, 0}, {4.9, 5.0, 0},
		{14.999999999999998, 5.0, 2}, {1e-9, 5.0, 0}, {37.5, 5.0, 7},
	}
	for _, c := range cases {
		if got := PyFloorDiv(c.a, c.b); got != c.want {
			t.Fatalf("%v // %v: got %v want %v", c.a, c.b, got, c.want)
		}
	}
	// a value a hair below a boundary whose quotient rounds up to the boundary: CPython gives the lower bucket
	a := math.Nextafter(15.0, 0)
	if a/5.0 == 3.0 && PyFloorDiv(a, 5.0) != 2 {
		t.Fatalf("boundary case: got %v", PyFloorDiv(a, 5.0))
	}
}
