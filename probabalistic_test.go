package hll

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_smallRangeSmokeTest(t *testing.T) {
	m := 1 << uint(sparseTestSettings.Log2m)

	// only one register set
	{
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, 0, 1))
		assertSparse(t, hll)

		// Trivially true that small correction conditions hold: one register
		// set implies zeroes exist, and estimator trivially smaller than 5m/2.
		// Small range correction: m * log(m/V)
		expected := uint64(math.Ceil(float64(m) * math.Log(float64(m)/float64(m-1) /*# of zeroes*/)))
		assert.Equal(t, expected, hll.Cardinality())
	}
	// at sparse capacity
	{
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		for i := 0; i < int(hll.settings.sparseThreshold); i++ {
			hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, 1))
		}
		assertSparse(t, hll)

		// Small range correction: m * log(m/V)
		expected := uint64(math.Ceil(float64(m) * math.Log(float64(m)/float64(m-int(hll.settings.sparseThreshold)) /*# of zeroes*/)))
		assert.Equal(t, expected, hll.Cardinality())
	}
	// all but one register set
	{
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		for i := 0; i < m-1; i++ {
			hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, 1))
		}
		assertDense(t, hll)

		// Small range correction: m * log(m/V)
		expected := uint64(math.Ceil(float64(m) * math.Log(float64(m)/float64(1) /*# of zeroes*/)))
		assert.Equal(t, expected, hll.Cardinality())
	}
}

func Test_normalRangeSmokeTest(t *testing.T) {
	m := 1 << uint(sparseTestSettings.Log2m)
	// regwidth = 5, so hash space is
	// log2m + (2^5 - 1 - 1), so L = log2m + 30
	l := sparseTestSettings.Log2m + 30

	// all registers at 'medium' value
	{
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		registerValue := 7 /*chosen to ensure neither correction kicks in*/
		for i := 0; i < m; i++ {
			hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, registerValue))
		}
		assertDense(t, hll)

		// Simplified estimator when all registers take same value: alpha / (m/2^val)
		twoToRegValue := 1 << uint(registerValue)
		estimator := alphaMSquared(sparseTestSettings.Log2m) / (float64(m) / float64(twoToRegValue))

		// Assert conditions for uncorrected range
		twoToL := 1 << uint(l)
		assert.True(t, estimator <= float64(twoToL)/30)
		assert.True(t, estimator > (float64(5)*float64(m)/float64(2)))

		expected := uint64(math.Ceil(estimator))
		assert.Equal(t, expected, hll.Cardinality())
	}
}

func Test_largeRangeSmokeTest(t *testing.T) {
	m := 1 << uint(sparseTestSettings.Log2m)
	// regwidth = 5, so hash space is
	// log2m + (2^5 - 1 - 1), so L = log2m + 30
	l := sparseTestSettings.Log2m + 30

	// all registers at large value
	{
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		// NOTE : java test uses 31 here, but that is too large and results in
		//        NaN for cardinality calculation (PG agrees)
		registerValue := 28 /*chosen to ensure large correction kicks in*/
		for i := 0; i < m; i++ {
			hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, registerValue))
		}
		assertDense(t, hll)

		// Simplified estimator when all registers take same value: alpha / (m/2^val)
		twoToRegValue := 1 << uint(registerValue)
		estimator := alphaMSquared(sparseTestSettings.Log2m) / (float64(m) / float64(twoToRegValue))

		// Assert conditions for uncorrected range
		assert.True(t, estimator > math.Pow(2, float64(l))/30)

		// Large range correction: -2^32 * log(1 - E/2^32)
		expected := uint64(math.Ceil(-1.0 * math.Pow(2, float64(l)) * math.Log(1.0-estimator/math.Pow(2, float64(l)))))
		assert.Equal(t, expected, hll.Cardinality())
	}
}

func Test_LargeEstimatorCutoff(t *testing.T) {

	for log2m := minimumLog2mParam; log2m <= maximumLog2mParam; log2m++ {
		for regwidth := minimumRegwidthParam; regwidth <= maximumRegwidthParam; regwidth++ {
			cutoff := largeEstimatorCutoff(twoToL(log2m, regwidth))

			// See blog post (http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/)
			// and original paper (Fig. 3) for information on 2^L and "large range correction" cutoff.
			expected := math.Pow(2, math.Pow(2, float64(regwidth))-2+float64(log2m)) / 30.0
			assert.Equal(t, expected, cutoff)
		}
	}
}
