package hll

import (
	"fmt"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sparseTestSettings = Settings{
	Log2m:             11,
	Regwidth:          5,
	ExplicitThreshold: 0,
	SparseEnabled:     true,
}

func Test_Add_Sparse(t *testing.T) {
	{ // insert an element with register value 1 (minimum set value)
		registerIndex := 0
		registerValue := 1
		rawValue := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue)

		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(rawValue)

		assertOneRegisterSet(t, hll, registerIndex, byte(registerValue))
	}
	{ // insert an element with register value 31 (maximum set value)
		registerIndex := 0
		registerValue := 31
		rawValue := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue)

		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(rawValue)

		assertOneRegisterSet(t, hll, registerIndex, byte(registerValue))
	}
	{ // insert an element that could overflow the register (past 31)
		registerIndex := 0
		registerValue := 36
		rawValue := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue)

		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(rawValue)

		assertOneRegisterSet(t, hll, registerIndex, byte(31) /*register max*/)
	}
	{ // insert duplicate elements, observe no change
		registerIndex := 0
		registerValue := 1
		rawValue := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue)

		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(rawValue)
		hll.AddRaw(rawValue)

		assertOneRegisterSet(t, hll, registerIndex, byte(registerValue))
	}
	{ // insert elements that increase a register's value
		registerIndex := 0
		registerValue := 1
		rawValue := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue)

		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(rawValue)

		registerValue2 := 2
		rawValue2 := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue2)
		hll.AddRaw(rawValue2)

		assertOneRegisterSet(t, hll, registerIndex, byte(registerValue2))
	}
	{ // insert elements that have lower register values, observe no change
		registerIndex := 0
		registerValue := 2
		rawValue := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue)

		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		hll.AddRaw(rawValue)

		registerValue2 := 1
		rawValue2 := constructHllValue(sparseTestSettings.Log2m, registerIndex, registerValue2)
		hll.AddRaw(rawValue2)

		assertOneRegisterSet(t, hll, registerIndex, byte(registerValue))
	}
}

func Test_Union_Sparse(t *testing.T) {

	{ // two disjoint multisets should union properly
		hllA, _ := NewHll(sparseTestSettings)
		hllA.AddRaw(constructHllValue(sparseTestSettings.Log2m, 1, 1))
		hllB, _ := NewHll(sparseTestSettings)
		hllB.AddRaw(constructHllValue(sparseTestSettings.Log2m, 2, 1))

		hllA.Union(hllB)

		assertSparse(t, hllA)
		assert.Equal(t, uint64(3), hllA.Cardinality())
		assertRegisterPresent(t, hllA, 1, 1)
		assertRegisterPresent(t, hllA, 2, 1)

		assert.Equal(t, uint64(2), hllB.Cardinality())
	}
	{ // two exactly overlapping multisets should union properly
		hllA, _ := NewHll(sparseTestSettings)
		hllA.AddRaw(constructHllValue(sparseTestSettings.Log2m, 1, 10))
		hllB, _ := NewHll(sparseTestSettings)
		hllB.AddRaw(constructHllValue(sparseTestSettings.Log2m, 1, 13))

		hllA.Union(hllB)

		assertSparse(t, hllA)
		assert.Equal(t, uint64(2), hllA.Cardinality())
		assertOneRegisterSet(t, hllA, 1, 13)
	}
	{ // overlapping multisets should union properly
		hllA, _ := NewHll(sparseTestSettings)
		hllB, _ := NewHll(sparseTestSettings)
		// register index = 3
		rawValueA := constructHllValue(sparseTestSettings.Log2m, 3, 11)

		// register index = 4
		rawValueB := constructHllValue(sparseTestSettings.Log2m, 4, 13)
		rawValueBPrime := constructHllValue(sparseTestSettings.Log2m, 4, 21)

		// register index = 5
		rawValueC := constructHllValue(sparseTestSettings.Log2m, 5, 14)

		hllA.AddRaw(rawValueA)
		hllA.AddRaw(rawValueB)

		hllB.AddRaw(rawValueBPrime)
		hllB.AddRaw(rawValueC)

		hllA.Union(hllB)
		// union should have three registers set, with partition B set to the
		// max of the two registers
		assertRegisterPresent(t, hllA, 3, 11)
		assertRegisterPresent(t, hllA, 4, 21 /*max(21,13)*/)
		assertRegisterPresent(t, hllA, 5, 14)
	}
	{ // too-large unions should promote
		hllA, _ := NewHll(sparseTestSettings)
		hllB, _ := NewHll(sparseTestSettings)

		// fill up sets to maxCapacity
		for i := 0; i < int(hllA.settings.sparseThreshold); i++ {
			hllA.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, 1))
			hllB.AddRaw(constructHllValue(sparseTestSettings.Log2m, i+int(hllA.settings.sparseThreshold), 1))
		}

		hllA.Union(hllB)
		assertDense(t, hllA)
	}
}

func Test_Clear_Sparse(t *testing.T) {
	hll, _ := NewHll(sparseTestSettings)
	hll.AddRaw(1)
	assertSparse(t, hll)
	hll.Clear()
	assertEmpty(t, hll)
	assert.Equal(t, uint64(0), hll.Cardinality())
}

func Test_ToFromBytes_Sparse(t *testing.T) {

	padding := 3

	{ // Should work on an empty element
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		bytes := hll.ToBytes()

		// assert output length is correct
		assert.Equal(t, len(bytes), padding)

		inHll, err := FromBytes(bytes)
		assert.NoError(t, err)
		assert.Nil(t, hll.storage)
		assert.Equal(t, uint64(0), inHll.Cardinality())
		assertEmpty(t, hll)
	}
	{ // Should work on a partially filled element
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, i+9))
		}

		bytes := hll.ToBytes()

		// assert output length is correct
		assert.Equal(t, padding+6, len(bytes))

		inHll, err := FromBytes(bytes)
		assert.NoError(t, err)
		assertSparse(t, hll)

		// assert register values correct
		assertElementsEqualSparse(t, hll, inHll)
	}
	{ // Should work on a full set
		hll, err := NewHll(sparseTestSettings)
		require.NoError(t, err)

		for i := 0; i < int(hll.settings.sparseThreshold); i++ {
			hll.AddRaw(constructHllValue(sparseTestSettings.Log2m, i, (i%9)+1))
		}

		bytes := hll.ToBytes()

		// assert output length is correct
		assert.Equal(t, padding+(int(hll.settings.sparseThreshold)*2), len(bytes))

		inHll, err := FromBytes(bytes)
		assert.NoError(t, err)
		assertSparse(t, hll)

		// assert register values correct
		assertElementsEqualSparse(t, hll, inHll)
	}
}

func Test_RandomValues_Sparse(t *testing.T) {

	seed := 1 // makes for reproducible tests.
	r := rand.NewSource(int64(seed))

	for run := 0; run < 100; run++ {
		t.Run(fmt.Sprint("run ", run), func(t *testing.T) {
			hll, err := NewHll(sparseTestSettings)
			require.NoError(t, err)

			registers := make(map[int]byte)

			for i := 0; i < int(hll.settings.sparseThreshold); i++ {
				value := uint64(r.Int63())

				reg := getRegisterIndex(value, hll.settings.log2m)
				regVal := getRegisterValue(value, hll.settings.log2m)
				if registers[reg] < regVal {
					registers[reg] = regVal
				}

				hll.AddRaw(value)
			}

			for reg, val := range registers {
				assertRegisterPresent(t, hll, reg, val)
			}
		})
	}
}

func assertRegisterPresent(t *testing.T, hll Hll, register int, value byte) {
	if assert.IsType(t, sparseStorage{}, hll.storage) {
		assert.Equal(t, value, hll.storage.(sparseStorage)[int32(register)])
	}
}

func assertOneRegisterSet(t *testing.T, hll Hll, register int, value byte) {
	if assert.IsType(t, sparseStorage{}, hll.storage) {
		assert.Equal(t, value, hll.storage.(sparseStorage)[int32(register)])
		assert.Equal(t, len(hll.storage.(sparseStorage)), 1)
	}
}

func constructHllValue(log2m int, register int, value int) uint64 {
	substreamValue := uint64(1) << uint(value-1)
	return (substreamValue << uint(log2m)) | uint64(register)
}

func assertElementsEqualSparse(t *testing.T, hll1 Hll, hll2 Hll) {
	if assertSparse(t, hll1) && assertSparse(t, hll2) {
		assert.Equal(t, hll1.storage, hll2.storage)
	}
}

func getRegisterIndex(value uint64, log2m int) int {
	mBitsMask := (1 << uint(log2m)) - 1
	return int(value & uint64(mBitsMask))
}

func getRegisterValue(value uint64, log2m int) byte {

	substreamValue := value >> uint(log2m)

	// The paper does not cover p(0x0), so the special value 0 is used.
	// 0 is the original initialization value of the registers, so by
	// doing this the HLL simply ignores it. This is acceptable
	// because the probability is 1/(2^(2^registerSizeInBits)).
	if substreamValue == 0 {
		return 0
	}

	// NOTE : trailing zeros == the 0-based index of the least significant 1 bit.
	pW := byte(1 + bits.TrailingZeros64(substreamValue))

	max := byte((1 << uint(log2m)) - 1)
	if pW > max {
		return max
	}

	return pW
}
