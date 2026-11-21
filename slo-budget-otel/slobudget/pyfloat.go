package slobudget

import "math"

// PyFloorDiv is CPython's float floor division (a // b), used for period
// indexes so that a span ending a hair before a period boundary lands in the
// same period as in the Python replay.
func PyFloorDiv(a, b float64) float64 {
	mod := math.Mod(a, b)
	div := (a - mod) / b
	if mod != 0 && (b < 0) != (mod < 0) {
		mod += b
		div -= 1.0
	}
	var floordiv float64
	if div != 0 {
		floordiv = math.Floor(div)
		if div-floordiv > 0.5 {
			floordiv += 1.0
		}
	} else {
		floordiv = math.Copysign(0.0, a/b)
	}
	return floordiv
}

// PySum is CPython's built-in sum() over floats (3.12+): the first item exact,
// then Neumaier compensated summation, the compensation added at the end when
// it is non-zero and finite.  The simulator's sums (weights, sqrt budgets,
// callee budgets, expected counts) all go through sum(), so the port must too
// for bit-identical T and p.
func PySum(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	f := xs[0]
	c := 0.0
	for _, x := range xs[1:] {
		t := f + x
		if math.Abs(f) >= math.Abs(x) {
			c += (f - t) + x
		} else {
			c += (x - t) + f
		}
		f = t
	}
	if c != 0 && !math.IsInf(c, 0) && !math.IsNaN(c) {
		f += c
	}
	return f
}

// PyRound is CPython's round(x) for a float: half to even.
func PyRound(x float64) float64 { return math.RoundToEven(x) }
