package slobudget

// PyRandom reproduces CPython's random.Random for the two calls the decider
// uses: seeding from an integer and random() in [0, 1).  Same Mersenne
// Twister (MT19937) state, same init_by_array seeding of the integer's 32-bit
// words, same 53-bit double construction, so a Go replay seeded like the
// Python simulator draws the identical coin sequence.
type PyRandom struct {
	mt  [mtN]uint32
	mti int
}

const (
	mtN         = 624
	mtM         = 397
	mtMatrixA   = 0x9908b0df
	mtUpperMask = 0x80000000
	mtLowerMask = 0x7fffffff
)

// NewPyRandom seeds like random.Random(seed) for a non-negative integer seed.
func NewPyRandom(seed uint64) *PyRandom {
	key := []uint32{uint32(seed)}
	if hi := uint32(seed >> 32); hi != 0 {
		key = append(key, hi)
	}
	r := &PyRandom{}
	r.initByArray(key)
	return r
}

func (r *PyRandom) initGenrand(s uint32) {
	r.mt[0] = s
	for i := 1; i < mtN; i++ {
		r.mt[i] = 1812433253*(r.mt[i-1]^(r.mt[i-1]>>30)) + uint32(i)
	}
	r.mti = mtN
}

func (r *PyRandom) initByArray(key []uint32) {
	r.initGenrand(19650218)
	i, j := 1, 0
	k := mtN
	if len(key) > k {
		k = len(key)
	}
	for ; k > 0; k-- {
		r.mt[i] = (r.mt[i] ^ ((r.mt[i-1] ^ (r.mt[i-1] >> 30)) * 1664525)) + key[j] + uint32(j)
		i++
		j++
		if i >= mtN {
			r.mt[0] = r.mt[mtN-1]
			i = 1
		}
		if j >= len(key) {
			j = 0
		}
	}
	for k = mtN - 1; k > 0; k-- {
		r.mt[i] = (r.mt[i] ^ ((r.mt[i-1] ^ (r.mt[i-1] >> 30)) * 1566083941)) - uint32(i)
		i++
		if i >= mtN {
			r.mt[0] = r.mt[mtN-1]
			i = 1
		}
	}
	r.mt[0] = 0x80000000
}

// Uint32 is genrand_int32.
func (r *PyRandom) Uint32() uint32 {
	if r.mti >= mtN {
		var kk int
		for kk = 0; kk < mtN-mtM; kk++ {
			y := (r.mt[kk] & mtUpperMask) | (r.mt[kk+1] & mtLowerMask)
			r.mt[kk] = r.mt[kk+mtM] ^ (y >> 1) ^ (mtMatrixA * (y & 1))
		}
		for ; kk < mtN-1; kk++ {
			y := (r.mt[kk] & mtUpperMask) | (r.mt[kk+1] & mtLowerMask)
			r.mt[kk] = r.mt[kk+(mtM-mtN)] ^ (y >> 1) ^ (mtMatrixA * (y & 1))
		}
		y := (r.mt[mtN-1] & mtUpperMask) | (r.mt[0] & mtLowerMask)
		r.mt[mtN-1] = r.mt[mtM-1] ^ (y >> 1) ^ (mtMatrixA * (y & 1))
		r.mti = 0
	}
	y := r.mt[r.mti]
	r.mti++
	y ^= y >> 11
	y ^= (y << 7) & 0x9d2c5680
	y ^= (y << 15) & 0xefc60000
	y ^= y >> 18
	return y
}

// Random is random.random(): a 53-bit double in [0, 1) from two draws.
func (r *PyRandom) Random() float64 {
	a := r.Uint32() >> 5
	b := r.Uint32() >> 6
	return (float64(a)*67108864.0 + float64(b)) * (1.0 / 9007199254740992.0)
}
