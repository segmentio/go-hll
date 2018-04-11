package hll

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ZeroValue_NoDefaultSettings(t *testing.T) {

	tests := []struct {
		label string
		op    func(hll Hll)
	}{
		{
			label: "AddRaw",
			op:    func(hll Hll) { hll.AddRaw(1) },
		},
		{
			label: "Settings",
			op:    func(hll Hll) { hll.Settings() },
		},
		{
			label: "Cardinality",
			op:    func(hll Hll) { hll.Cardinality() },
		},
		{
			label: "StrictUnion",
			op:    func(hll Hll) { _ = hll.StrictUnion(Hll{}) },
		},
		{
			label: "Union",
			op:    func(hll Hll) { hll.Union(Hll{}) },
		},
		{
			label: "ToBytes",
			op:    func(hll Hll) { hll.ToBytes() },
		},
		{
			label: "Clear",
			op:    func(hll Hll) { hll.Clear() },
		},
		{
			label: "Settings",
			op:    func(hll Hll) { hll.Settings() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			defer func() {
				r := recover()
				require.NotNil(t, r, "method should have errored out")
				require.Contains(t, r, "without default settings")
			}()
			tt.op(Hll{} /*zero value*/)
		})
	}
}

func Test_ZeroValue_WithDefaultSettings(t *testing.T) {

	defaults := Settings{
		Log2m:             31,
		Regwidth:          6,
		ExplicitThreshold: AutoExplicitThreshold,
		SparseEnabled:     true,
	}
	Defaults(defaults)

	tests := []struct {
		label  string
		op     func(hll Hll) interface{}
		result interface{}
	}{
		{
			label: "AddRaw",
			op: func(hll Hll) interface{} {
				hll.AddRaw(1)
				return hll.Cardinality()
			},
			result: uint64(1),
		},

		{
			label:  "Cardinality",
			op:     func(hll Hll) interface{} { return hll.Cardinality() },
			result: uint64(0),
		},
		{
			label: "StrictUnion",
			op: func(hll Hll) interface{} {
				_ = hll.StrictUnion(Hll{})
				return hll.Cardinality()
			},
			result: uint64(0),
		},
		{
			label: "Union",
			op: func(hll Hll) interface{} {
				hll.Union(Hll{})
				return hll.Cardinality()
			},
			result: uint64(0),
		},
		{
			label:  "ToBytes",
			op:     func(hll Hll) interface{} { return hll.ToBytes() },
			result: []byte{0x11, 0xbf, 0x7f},
		},
		{
			label: "Clear",
			op: func(hll Hll) interface{} {
				hll.Clear()
				return hll.Cardinality()
			},
			result: uint64(0),
		},
		{
			label: "Settings",
			op: func(hll Hll) interface{} {
				return hll.Settings()
			},
			result: defaults,
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			assert.Equal(t, tt.result, tt.op(Hll{} /*zero value*/))
		})
	}
}

func Test_Undefined(t *testing.T) {

	tests := []struct {
		label string
		op    func(hll Hll)
	}{
		{
			label: "AddRaw",
			op:    func(hll Hll) { hll.AddRaw(1) },
		},

		{
			label: "Cardinality",
			op:    func(hll Hll) { hll.Cardinality() },
		},
		{
			label: "StrictUnion",
			op:    func(hll Hll) { _ = hll.StrictUnion(Hll{}) },
		},
		{
			label: "Union",
			op:    func(hll Hll) { hll.Union(Hll{}) },
		},
		{
			label: "ToBytes",
			op:    func(hll Hll) { hll.ToBytes() },
		},
		{
			label: "Clear",
			op:    func(hll Hll) { hll.Clear() },
		},
		{
			label: "Settings",
			op:    func(hll Hll) { hll.Clear() },
		},
	}

	resetDefaults()

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			defer func() {
				r := recover()
				require.NotNil(t, r, "method should have errored out")
				require.Contains(t, r, "attempted operation on empty Hll without default settings")
			}()
			tt.op(Hll{})
		})
	}
}

// Test_UpgradePaths ensures that the Hll upgrades storage as elements are added
// to the Hll per the specification and the configuration settings.
func Test_UpgradePaths(t *testing.T) {

	// make the test repeatable...this allows us to assert on cardinalities
	// below.  checking the cardinality is important because it ensures that the
	// conversion was accurate.  however, without using the same seed for every
	// test, the probabalistic cardinality could vary.
	rand.Seed(1234567890)

	tests := []struct {
		label        string
		settings     Settings
		prepareFuncs []func(*Hll)
		verifyFuncs  []func(*testing.T, Hll)
	}{
		{
			label: "all types enabled",
			settings: Settings{
				Log2m:             8,
				Regwidth:          4,
				ExplicitThreshold: AutoExplicitThreshold,
				SparseEnabled:     true,
			},
			prepareFuncs: []func(*Hll){
				func(hll *Hll) {
					for {
						hll.AddRaw(rand.Uint64())

						s := hll.storage.(explicitStorage)
						if len(s) == hll.settings.explicitThreshold {
							break
						}
					}
				},
				func(hll *Hll) {
					hll.AddRaw(rand.Uint64())
				},
				func(hll *Hll) {
					for {
						if _, ok := hll.storage.(sparseStorage); !ok {
							break
						}
						hll.AddRaw(rand.Uint64())
					}
				},
			},
			verifyFuncs: []func(*testing.T, Hll){
				func(t *testing.T, hll Hll) {
					assertExplicit(t, hll)
					assert.Equal(t, uint64(hll.settings.explicitThreshold), hll.Cardinality())
				},
				func(t *testing.T, hll Hll) {
					assertSparse(t, hll)
					assert.Equal(t, uint64(18), hll.Cardinality())
				},
				func(t *testing.T, hll Hll) {
					assertDense(t, hll)
					assert.Equal(t, uint64(75), hll.Cardinality())
				},
			},
		},
		{
			label: "explicit threshold/sparse disabled",
			settings: Settings{
				Log2m:             10,
				Regwidth:          4,
				ExplicitThreshold: 100,
				SparseEnabled:     false,
			},
			prepareFuncs: []func(*Hll){
				func(hll *Hll) {
					for {
						hll.AddRaw(rand.Uint64())
						s := hll.storage.(explicitStorage)
						if len(s) == 100 {
							break
						}
					}
				},
				func(hll *Hll) {
					hll.AddRaw(rand.Uint64())
				},
			},
			verifyFuncs: []func(*testing.T, Hll){
				func(t *testing.T, hll Hll) {
					assertExplicit(t, hll)
					assert.Equal(t, uint64(100), hll.Cardinality())
				},
				func(t *testing.T, hll Hll) {
					assertDense(t, hll)
					assert.Equal(t, uint64(101), hll.Cardinality())
				},
			},
		},
		{
			label: "explicit threshold/sparse enabled",
			settings: Settings{
				Log2m:             10,
				Regwidth:          4,
				ExplicitThreshold: 200,
				SparseEnabled:     true,
			},
			prepareFuncs: []func(*Hll){
				func(hll *Hll) {
					for {
						hll.AddRaw(rand.Uint64())
						s := hll.storage.(explicitStorage)
						if len(s) == 200 {
							break
						}
					}
				},
				func(hll *Hll) {
					hll.AddRaw(rand.Uint64())
				},
			},
			verifyFuncs: []func(*testing.T, Hll){
				func(t *testing.T, hll Hll) {
					assertExplicit(t, hll)
					assert.Equal(t, uint64(200), hll.Cardinality())
				},
				func(t *testing.T, hll Hll) {
					assertSparse(t, hll)
					assert.Equal(t, uint64(200), hll.Cardinality())
				},
			},
		},
		{
			label: "explicit disabled/sparse enabled",
			settings: Settings{
				Log2m:             10,
				Regwidth:          4,
				ExplicitThreshold: 0,
				SparseEnabled:     true,
			},
			prepareFuncs: []func(*Hll){
				func(hll *Hll) {
					hll.AddRaw(rand.Uint64())
				},
			},
			verifyFuncs: []func(*testing.T, Hll){
				func(t *testing.T, hll Hll) {
					assertSparse(t, hll)
					assert.NotZero(t, hll.Cardinality())
				},
			},
		},
		{
			label: "explicit disabled/sparse disabled",
			settings: Settings{
				Log2m:             10,
				Regwidth:          4,
				ExplicitThreshold: 0,
				SparseEnabled:     false,
			},
			prepareFuncs: []func(*Hll){
				func(hll *Hll) {
					hll.AddRaw(rand.Uint64())
				},
			},
			verifyFuncs: []func(*testing.T, Hll){
				func(t *testing.T, hll Hll) {
					assertDense(t, hll)
					assert.NotZero(t, hll.Cardinality())
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {

			hll, err := NewHll(tt.settings)
			require.NoError(t, err)

			assertEmpty(t, hll)

			for i := range tt.prepareFuncs {
				tt.prepareFuncs[i](&hll)
				tt.verifyFuncs[i](t, hll)
			}
		})
	}
}

// Test_MismatchedStorageUnions exercises the different possible cases when
// unioning Hlls with different storage types.
func Test_MismatchedStorageUnions(t *testing.T) {

	// make the test repeatable...this allows us to assert on cardinalities
	// below.  checking the cardinality is important because it ensures that the
	// conversion was accurate.  however, without using the same seed for every
	// test, the probabalistic cardinality could vary.
	rand.Seed(1234567890)

	expThresh := 5
	settings := Settings{
		Log2m:             11,
		Regwidth:          5,
		ExplicitThreshold: expThresh,
		SparseEnabled:     true,
	}

	Defaults(settings)

	defer resetDefaults()

	// a generator for Hlls with n unique values added to them.
	used := make(map[uint64]struct{})
	randGen := func() uint64 {
		for {
			next := rand.Uint64()
			if _, ok := used[next]; !ok {
				used[next] = struct{}{}
				return next
			}
		}
	}
	newHllFunc := func(n int) (hll Hll) {
		for i := 0; i < n; i++ {
			hll.AddRaw(rand.Uint64())
			used[randGen()] = struct{}{}
		}
		return
	}

	tests := []struct {
		label       string
		hll1        Hll
		hll2        Hll
		cardinality uint64
		verifyFunc  func(*testing.T, Hll) bool
	}{
		{
			label:       "empty with empty",
			hll1:        Hll{},
			hll2:        Hll{},
			cardinality: 0,
			verifyFunc:  assertEmpty,
		},
		{
			label:       "empty with explicit",
			hll1:        Hll{},
			hll2:        newHllFunc(1),
			cardinality: 1,
			verifyFunc:  assertExplicit,
		},
		{
			label:       "explicit with empty",
			hll1:        newHllFunc(1),
			hll2:        Hll{},
			cardinality: 1,
			verifyFunc:  assertExplicit,
		},
		{
			label:       "empty with sparse",
			hll1:        Hll{},
			hll2:        newHllFunc(expThresh + 1),
			cardinality: 7,
			verifyFunc:  assertSparse,
		},
		{
			label:       "sparse with empty",
			hll1:        newHllFunc(expThresh + 1),
			hll2:        Hll{},
			cardinality: 7,
			verifyFunc:  assertSparse,
		},
		{
			label:       "empty with dense",
			hll1:        Hll{},
			hll2:        newHllFunc(1000),
			cardinality: 1030,
			verifyFunc:  assertDense,
		},
		{
			label:       "dense with empty",
			hll1:        newHllFunc(1000),
			hll2:        Hll{},
			cardinality: 1021,
			verifyFunc:  assertDense,
		},
		{
			label:       "explicit with explicit",
			hll1:        newHllFunc(2),
			hll2:        newHllFunc(2),
			cardinality: 4,
			verifyFunc:  assertExplicit,
		},
		{
			label:       "explicit with explicit/overflow",
			hll1:        newHllFunc(3),
			hll2:        newHllFunc(3),
			cardinality: 7,
			verifyFunc:  assertSparse,
		},
		{
			label:       "explicit with sparse",
			hll1:        newHllFunc(2),
			hll2:        newHllFunc(expThresh + 1),
			cardinality: 9,
			verifyFunc:  assertSparse,
		},
		{
			label:       "sparse with explicit",
			hll1:        newHllFunc(expThresh + 1),
			hll2:        newHllFunc(2),
			cardinality: 9,
			verifyFunc:  assertSparse,
		},
		{
			label:       "explicit with dense",
			hll1:        newHllFunc(2),
			hll2:        newHllFunc(1000),
			cardinality: 994,
			verifyFunc:  assertDense,
		},
		{
			label:       "dense with explicit",
			hll1:        newHllFunc(1000),
			hll2:        newHllFunc(2),
			cardinality: 984,
			verifyFunc:  assertDense,
		},
		{
			label:       "sparse with sparse",
			hll1:        newHllFunc(expThresh + 1),
			hll2:        newHllFunc(expThresh + 1),
			cardinality: 13,
			verifyFunc:  assertSparse,
		},
		{
			label: "sparse with sparse/overflow",
			hll1: func() Hll {
				hll := newHllFunc(400)
				assertSparse(t, hll)
				return hll
			}(),
			hll2: func() Hll {
				hll := newHllFunc(400)
				assertSparse(t, hll)
				return hll
			}(),
			cardinality: 797,
			verifyFunc:  assertDense,
		},
		{
			label:       "sparse with dense",
			hll1:        newHllFunc(expThresh + 1),
			hll2:        newHllFunc(1000),
			cardinality: 990,
			verifyFunc:  assertDense,
		},
		{
			label:       "dense with sparse",
			hll1:        newHllFunc(1000),
			hll2:        newHllFunc(expThresh + 1),
			cardinality: 992,
			verifyFunc:  assertDense,
		},
		{
			label:       "dense with dense",
			hll1:        newHllFunc(1000),
			hll2:        newHllFunc(1000),
			cardinality: 2004,
			verifyFunc:  assertDense,
		},
		{
			label: "explicit with sparse/sparse disabled",
			hll1: func() Hll {
				s := settings
				s.SparseEnabled = false
				hll, _ := NewHll(s)
				hll.AddRaw(randGen())
				return hll
			}(),
			hll2:        newHllFunc(expThresh + 1),
			cardinality: 8,
			verifyFunc:  assertDense,
		},
		{
			label: "empty with sparse/sparse disabled",
			hll1: func() Hll {
				s := settings
				s.SparseEnabled = false
				hll, _ := NewHll(s)
				return hll
			}(),
			hll2:        newHllFunc(expThresh + 1),
			cardinality: 7,
			verifyFunc:  assertDense,
		},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {

			cardinality2 := tt.hll2.Cardinality()

			var storage2 storage
			if tt.hll2.storage != nil {
				storage2 = tt.hll2.storage.copy()
			}

			err := tt.hll1.StrictUnion(tt.hll2)
			require.NoError(t, err)
			require.Equal(t, tt.cardinality, tt.hll1.Cardinality())
			tt.verifyFunc(t, tt.hll1)

			// mutate hll1
			tt.hll1.AddRaw(randGen())

			// and ensure that hll2 has not been modified by union or successive
			// modification
			require.Equal(t, cardinality2, tt.hll2.Cardinality())
			require.Equal(t, storage2, tt.hll2.storage)
		})
	}
}

func newHll(t *testing.T, settings Settings) Hll {
	hll, err := NewHll(settings)
	require.NoError(t, err)
	return hll
}

func assertEmpty(t *testing.T, hll Hll) bool {
	return assert.Nil(t, hll.storage, "expected empty hll")
}

func assertExplicit(t *testing.T, hll Hll) bool {
	return assert.Equal(t, reflect.TypeOf(explicitStorage{}), reflect.TypeOf(hll.storage), "expected explicit storage")
}

func assertSparse(t *testing.T, hll Hll) bool {
	return assert.Equal(t, reflect.TypeOf(sparseStorage{}), reflect.TypeOf(hll.storage), "expected sparse storage")
}

func assertDense(t *testing.T, hll Hll) bool {
	return assert.Equal(t, reflect.TypeOf(denseStorage{}), reflect.TypeOf(hll.storage), "expected dense storage")
}
