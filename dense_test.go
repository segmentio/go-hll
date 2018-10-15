package hll

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

var denseTestSettings = Settings{
	Log2m:             11,
	Regwidth:          5,
	ExplicitThreshold: 0,
	SparseEnabled:     false,
}

func Test_ClearDense(t *testing.T) {
	hll, err := NewHll(Settings{Log2m: 4, Regwidth: 5})
	assert.NoError(t, err)

	hll.storage = newDenseStorage(hll.settings) // force upgrade for the test.
	for i := 0; i < 100; i++ {
		hll.AddRaw(uint64(i))
	}

	assert.True(t, hll.Cardinality() > 0)
	hll.Clear()
	assert.Equal(t, uint64(0), hll.Cardinality())
}

func Test_ToFromBytes_Dense(t *testing.T) {

	expectedByteCount := 3 /*header*/ + divideBy8RoundUp(int(denseTestSettings.Regwidth)*(1<<uint(denseTestSettings.Log2m)))

	{ // Should work on an empty element
		hll, err := NewHll(denseTestSettings)
		assert.NoError(t, err)

		hll.storage = newDenseStorage(hll.settings)
		bytes := hll.ToBytes()

		// assert output length is correct
		assert.Equal(t, expectedByteCount, len(bytes))

		inHll, err := FromBytes(bytes)
		assert.NoError(t, err)

		// assert register values correct
		assertElementsEqualDense(t, hll, inHll)
	}
	{ // Should work on a partially filled element
		hll, err := NewHll(denseTestSettings)
		assert.NoError(t, err)

		for i := 0; i < 3; i++ {
			hll.AddRaw(constructHllValue(int(hll.settings.log2m), i, i+9))
		}
		bytes := hll.ToBytes()

		// assert output length is correct
		assert.Equal(t, expectedByteCount, len(bytes))

		inHll, err := FromBytes(bytes)
		assert.NoError(t, err)

		// assert register values correct
		assertElementsEqualDense(t, hll, inHll)
	}
	{ // Should work on a full set
		hll, err := NewHll(denseTestSettings)
		assert.NoError(t, err)

		for i := 0; i < (1 << uint(hll.settings.log2m)); i++ {
			hll.AddRaw(constructHllValue(int(hll.settings.log2m), i, (i%9)+1))
		}
		bytes := hll.ToBytes()

		// assert output length is correct
		assert.Equal(t, expectedByteCount, len(bytes))

		inHll, err := FromBytes(bytes)
		assert.NoError(t, err)

		// assert register values correct
		assertElementsEqualDense(t, hll, inHll)
	}
}

func Test_ToFromBytes_Dense_Trailing(t *testing.T) {
	// ensure coverage on the code where the number of bits in the HLL is not
	// evenly divisible by 64
	hll, _ := NewHll(Settings{Log2m: 4, Regwidth: 3})
	for i := 0; i < 16; i++ {
		hll.AddRaw(constructHllValue(int(hll.settings.log2m), i, i+1))
	}
	bytes := hll.ToBytes()
	require.True(t, len(bytes)%8 != 0)
	hll2, _ := FromBytes(bytes)
	assert.Equal(t, hll.storage, hll2.storage)
}

func Test_DenseRegisters(t *testing.T) {

	tests := []struct {
		regwidth  int
		values    []uint64
		registers map[int]int
	}{
		{
			// register width 4 (the minimum size)
			regwidth: 4,
			values: []uint64{
				0x000000000000001,  /*'j'=1*/
				0x0000000000000012, /*'j'=2*/
				0x0000000000000023, /*'j'=3*/
				0x0000000000000044, /*'j'=4*/
				0x0000000000000085, /*'j'=5*/
				0x0000000000010006, /*'j'=6*/
				0x0000000000020007, /*'j'=7*/
				0x0000000000040008, /*'j'=8*/
				0x0000000000080009, /*'j'=9*/
				// sanity checks to ensure that no other bits above the lowest-set
				// bit matters
				// NOTE:  same as case 'j = 6' above
				0x000000000003000A, /*'j'=10*/
				0x000000000011000B, /*'j'=11*/
			},
			registers: map[int]int{
				1: 0,
				2: 1,
				3: 2,
				4: 3,
				5: 4,
				// upper-bounds of the register
				// NOTE:  bear in mind that BitVector itself does ensure that
				//        overflow of a register is prevented
				6:  13,
				7:  14,
				8:  15,
				9:  15, /*overflow*/
				10: 13,
				11: 13,
			},
		},
		{
			regwidth: 5,
			values: []uint64{
				0x000000000000001,  /*'j'=1*/
				0x0000000000000012, /*'j'=2*/
				0x0000000000000023, /*'j'=3*/
				0x0000000000000044, /*'j'=4*/
				0x0000000000000085, /*'j'=5*/
				// upper-bounds of the register
				// NOTE:  bear in mind that BitVector itself does ensure that
				//        overflow of a register is prevented
				0x0000000100000006, /*'j'=6*/
				0x0000000200000007, /*'j'=7*/
				0x0000000400000008, /*'j'=8*/
				0x0000000800000009, /*'j'=9*/
			},
			registers: map[int]int{
				1: 0,
				2: 1,
				3: 2,
				4: 3,
				5: 4,
				6: 29,
				7: 30,
				8: 31,
				9: 31, /*overflow*/
			},
		},
	}

	log2m := 4

	for _, tt := range tests {
		t.Run(fmt.Sprint("Regwidth_", tt.regwidth), func(t *testing.T) {
			hll, err := NewHll(Settings{Log2m: log2m, Regwidth: tt.regwidth})
			assert.NoError(t, err)

			for _, value := range tt.values {
				hll.AddRaw(value)
			}

			for regnum, value := range tt.registers {
				assert.Equal(t, byte(value), hll.storage.(denseStorage).get(regnum, tt.regwidth))
			}
		})
	}
}

// Test_DenseGet ensures that borders of 64 bit words are properly handled when
// settings don't align nicely to a 64 bit word.
func Test_DenseGet(t *testing.T) {
	settings, err := Settings{Regwidth: 7, Log2m: 7}.toInternal()
	require.NoError(t, err)
	ds := newDenseStorage(settings)
	for i := 0; i < 1<<uint(settings.log2m); i++ {
		ds.setIfGreater(settings, i, byte(i))
	}
	for i := 0; i < 1<<uint(settings.log2m); i++ {
		require.Equal(t, byte(i), ds.get(i, settings.regwidth), "loop: %d", i)
	}
}

func assertElementsEqualDense(t *testing.T, hll1 Hll, hll2 Hll) {
	if assertDense(t, hll1) && assertDense(t, hll2) {
		assert.Equal(t, hll1.storage, hll2.storage)
	}
}
