package hll

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_divideBy8RoundUp(t *testing.T) {
	assert.Equal(t, 0, divideBy8RoundUp(0))
	assert.Equal(t, 1, divideBy8RoundUp(1))
	assert.Equal(t, 1, divideBy8RoundUp(7))
	assert.Equal(t, 1, divideBy8RoundUp(8))
	assert.Equal(t, 2, divideBy8RoundUp(9))
	assert.Equal(t, 8, divideBy8RoundUp(64))
	assert.Equal(t, 9, divideBy8RoundUp(65))
}

func Test_readWriteBits(t *testing.T) {

	numSamples := 1000

	for nBits := 1; nBits < 64; nBits++ {
		mask := uint64((1 << uint(nBits)) - 1)

		// test from i = 0 to i = 1000...makes sure handling of lower bits is correct.
		t.Run(fmt.Sprintf("Ascending-%d", nBits), func(t *testing.T) {
			bytes := make([]byte, divideBy8RoundUp(nBits*numSamples))
			for i := 0; i < numSamples; i++ {
				writeBits(bytes, i*nBits, uint64(i), nBits)
			}

			for i := 0; i < numSamples; i++ {
				assert.Equal(t, uint64(i)&mask, readBits(bytes, i*nBits, nBits), "i == %d", i)
			}
		})

		// test from i = MAX to i = MAX - 1000...makes sure handling of upper bits is correct
		t.Run(fmt.Sprintf("Descending-%d", nBits), func(t *testing.T) {
			bytes := make([]byte, divideBy8RoundUp(nBits*numSamples))
			for i := 0; i < numSamples; i++ {
				writeBits(bytes, i*nBits, math.MaxUint64-uint64(i), nBits)
			}

			for i := 0; i < numSamples; i++ {
				assert.Equal(t, (math.MaxInt64-uint64(i))&mask, readBits(bytes, i*nBits, nBits), "i == %d", i)
			}
		})
	}
}
