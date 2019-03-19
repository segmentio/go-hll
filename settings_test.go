package hll

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SettingsValidate(t *testing.T) {

	tests := []struct {
		field              string
		minValue, maxValue int
	}{
		{
			field:    "Log2m",
			minValue: minimumLog2mParam,
			maxValue: maximumLog2mParam,
		},
		{
			field:    "Regwidth",
			minValue: minimumRegwidthParam,
			maxValue: maximumRegwidthParam,
		},
		{
			field:    "ExplicitThreshold",
			minValue: minimumExpthreshParam,
			maxValue: maximumExplicitThreshold,
		},
		// NOTE : SparseEnabled is not tested b/c it's not possible to have an invalid value.
	}

	defaults := Settings{
		Log2m:    11,
		Regwidth: 5,
	}
	// sanity check...ensure defaults are valid since we will use it as a base for all the tests.
	require.NoError(t, defaults.validate())

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			settings := defaults // copy known good settings

			ps := reflect.ValueOf(&settings)
			field := ps.Elem().FieldByName(tt.field)

			field.SetInt(int64(tt.minValue - 1))
			err := settings.validate()
			assert.Error(t, err, "one less than minimum value")
			assert.Contains(t, err.Error(), tt.field)
			assert.Contains(t, err.Error(), "Requires at least")

			field.SetInt(int64(tt.minValue))
			assert.NoError(t, settings.validate(), "minimum value")

			field.SetInt(int64(tt.maxValue))
			assert.NoError(t, settings.validate(), "maximum value")

			field.SetInt(int64(tt.maxValue + 1))
			err = settings.validate()
			assert.Error(t, err, "one more than maximum value")
			assert.Contains(t, err.Error(), tt.field)
			assert.Contains(t, err.Error(), "Allows at most")
		})
	}
}

func Test_Settings_calculateExplicitThreshold(t *testing.T) {
	assert.Equal(t, 160, calculateExplicitThreshold(11, 5))
	assert.Equal(t, 384, calculateExplicitThreshold(12, 6))
}

func Test_Settings_toExternal(t *testing.T) {

	originalSettings := []Settings{
		{
			Log2m:             5,
			Regwidth:          4,
			ExplicitThreshold: AutoExplicitThreshold,
			SparseEnabled:     true,
		},
		{
			Log2m:             8,
			Regwidth:          5,
			ExplicitThreshold: 0,
			SparseEnabled:     false,
		},
		{
			Log2m:             11,
			Regwidth:          6,
			ExplicitThreshold: 256,
			SparseEnabled:     true,
		},
	}

	for _, settings := range originalSettings {
		internalSettings, err := settings.toInternal()
		require.NoError(t, err)
		assert.Equal(t, settings, internalSettings.toExternal())
	}
}

func Test_Defaults(t *testing.T) {
	s := Settings{
		Log2m:    11,
		Regwidth: 5,
	}

	// reset the defaults on the way out of this function
	defer resetDefaults()

	err := Defaults(s)
	require.NoError(t, err)

	// this is allowed b/c the settings are the same.
	err = Defaults(s)
	require.NoError(t, err)

	// this is not allowed!
	s.Regwidth = 4
	err = Defaults(s)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already been installed")

	// this is also not allowed b/c settings are bad.
	s.Regwidth = 0
	err = Defaults(s)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Regwidth is too small")
}

func resetDefaults() {
	defaultSettingsLock.Lock()
	defaultSettings = nil
	defaultSettingsLock.Unlock()
}

func BenchmarkSettingsToInternal(b *testing.B) {
	s := Settings{
		Log2m:    11,
		Regwidth: 5,
	}

	for i := 0; i < b.N; i++ {
		s.toInternal()
	}
}
