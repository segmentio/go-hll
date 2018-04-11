package hll

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var explicitTestSettings = Settings{
	Log2m:             11,
	Regwidth:          5,
	ExplicitThreshold: 128,
	SparseEnabled:     true,
}

func Test_Add_Explicit(t *testing.T) {

	hll := newHll(t, explicitTestSettings)

	for i := 1; i < explicitTestSettings.ExplicitThreshold; i++ {
		assert.Equal(t, uint64(i-1), hll.Cardinality())
		hll.AddRaw(uint64(i))
		assert.Equal(t, uint64(i), hll.Cardinality())
	}

	// can re-add values w/out changing cardinality
	for i := 1; i < explicitTestSettings.ExplicitThreshold; i++ {
		hll.AddRaw(uint64(i))
		assert.Equal(t, uint64(explicitTestSettings.ExplicitThreshold-1), hll.Cardinality())
	}
}

func Test_Add_Explicit_Max(t *testing.T) {

	hll := newHll(t, explicitTestSettings)
	hll.AddRaw(math.MaxUint64)
	assert.Equal(t, uint64(1), hll.Cardinality())
}

func Test_Union_Explicit(t *testing.T) {
	{ // Unioning two distinct sets should work
		hllA, _ := NewHll(explicitTestSettings)
		hllB, _ := NewHll(explicitTestSettings)
		hllA.AddRaw(1)
		hllA.AddRaw(2)
		hllB.AddRaw(3)
		hllA.Union(hllB)
		assert.Equal(t, uint64(3), hllA.Cardinality(), "hll: %v", hllA)
	}
	{ // Unioning two sets whose union doesn't exceed the cardinality cap should not promote
		hllA, _ := NewHll(explicitTestSettings)
		hllB, _ := NewHll(explicitTestSettings)
		hllA.AddRaw(1)
		hllA.AddRaw(2)
		hllB.AddRaw(1)
		hllA.Union(hllB)
		assert.Equal(t, uint64(2), hllA.Cardinality())
	}
	{ // unioning two sets whose union exceeds the cardinality cap should promote
		hllA, _ := NewHll(explicitTestSettings)
		hllB, _ := NewHll(explicitTestSettings)

		// fill up A to explicitThreshold
		for i := 1; i <= explicitTestSettings.ExplicitThreshold; i++ {
			hllA.AddRaw(uint64(i))
		}
		assert.IsType(t, explicitStorage{}, hllA.storage)

		// add a single element to B to cause an upgrade on union
		hllB.AddRaw(uint64(explicitTestSettings.ExplicitThreshold + 1))
		hllA.Union(hllB)
		assert.IsType(t, sparseStorage{}, hllA.storage)
	}
}

func Test_Clear_Explicit(t *testing.T) {
	hll, err := NewHll(explicitTestSettings)
	require.NoError(t, err)

	hll.AddRaw(1)
	assert.Equal(t, uint64(1), hll.Cardinality())
	hll.Clear()
	assert.Equal(t, uint64(0), hll.Cardinality())
}

func Test_ToFromBytes_Explicit(t *testing.T) {
	padding := 3

	{ // Should work on a partially filled set
		hll, err := NewHll(explicitTestSettings)
		require.NoError(t, err)

		for i := 1; i < explicitTestSettings.ExplicitThreshold; i++ {
			hll.AddRaw(uint64(i))

			bytes := hll.ToBytes()

			// assert output has correct byte length
			assert.Equal(t, padding+(8*i /*elements*/), len(bytes))

			inHLL, err := FromBytes(bytes)
			assert.NoError(t, err)
			assertElementsEqualExplicit(t, hll, inHLL)
		}
	}
	{ // Should work on a full set
		hll, err := NewHll(explicitTestSettings)
		require.NoError(t, err)

		for i := 1; i <= explicitTestSettings.ExplicitThreshold; i++ {
			hll.AddRaw(uint64(i))
		}

		bytes := hll.ToBytes()

		// assert output has correct byte length
		assert.Equal(t, padding+int(8*explicitTestSettings.ExplicitThreshold /*elements*/), len(bytes))

		inHLL, err := FromBytes(bytes)
		assert.NoError(t, err)
		assertElementsEqualExplicit(t, hll, inHLL)
	}
}

func Test_RandomValues_Explicit(t *testing.T) {
	settings := explicitTestSettings
	settings.ExplicitThreshold = 4096
	hll, err := NewHll(settings)
	require.NoError(t, err)

	seed := 1 // makes for reproducible tests.
	canonical := make(map[uint64]struct{})
	rand := rand.NewSource(int64(seed))
	for i := 0; i < settings.ExplicitThreshold; i++ {
		value := uint64(rand.Int63())
		canonical[value] = struct{}{}
		hll.AddRaw(value)
	}

	assert.Equal(t, uint64(len(canonical)), hll.Cardinality())
}

func assertElementsEqualExplicit(t *testing.T, hll1 Hll, hll2 Hll) {
	assertExplicit(t, hll1)
	assertExplicit(t, hll2)
	assert.Equal(t, hll1.storage, hll2.storage)
}
