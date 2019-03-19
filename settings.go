package hll

import (
	"fmt"
	"math"
	"sync"

	"github.com/pkg/errors"
)

const (
	// minimum and maximum values for the log-base-2 of the number of registers
	// in the HLL
	minimumLog2mParam = 4
	maximumLog2mParam = 31

	// minimum and maximum values for the register width of the HLL
	minimumRegwidthParam = 1
	maximumRegwidthParam = 8

	// minimum and maximum values for the 'expthresh' parameter of the
	// constructor that is meant to match the PostgreSQL
	// implementation's constructor and parameter names
	minimumExpthreshParam    = -1
	maximumExpthreshParam    = 18
	maximumExplicitThreshold = 1 << (maximumExpthreshParam - 1) /*per storage spec*/

	// AutoExplicitThreshold indicates that the threshold at which an Hll goes
	// from using an explicit to a probabalistic representation should be
	// calculated based on the configuration.  Using the calculated threshold is
	// generally preferable.  One exception would be working with a pre-existing
	// data set that uses a particular explicit threshold setting in which case
	// it may be desirable to use the same explicit threshold.
	AutoExplicitThreshold = -1
)

// Settings are used to configure the Hll and how it transitions between the
// backing storage types.
type Settings struct {
	// Log2m determines the number of registers in the Hll.  The minimum value
	// is 4 and the maximum value is 31.  The number of registers in the Hll
	// will be calculated as 2^Log2m.
	Log2m int

	// Regwidth is the number of bits dedicated to each register value.  The
	// minimum value is 1 and the maximum value is 8.
	Regwidth int

	// ExplicitThreshold is the cardinality at which the Hll will go from
	// storing explicit values to using a probabilistic model.  A value of 0
	// disables explicit storage entirely.  The value AutoExplicitThreshold can
	// be used to signal the library to calculate an appropriate threshold
	// (recommended).  The maximum allowed value is 131,072.
	ExplicitThreshold int

	// SparseEnabled controls whether the Hll will use the sparse
	// representation.  The thresholds for conversion are automatically
	// calculated by the library when this field is set to true (recommended).
	SparseEnabled bool
}

var defaultSettings *settings
var defaultSettingsLock sync.RWMutex

var settingsCache map[Settings]*settings
var settingsCacheLock sync.RWMutex

func init() {
	settingsCache = make(map[Settings]*settings)
}

// Defaults installs settings that will be used by the zero value Hll.  It
// recommended to call this function once at initialization time and never
// again.  It will return an error if the provided settings are invalid or if a
// different set of defaults has already been installed.
func Defaults(settings Settings) error {

	s, err := settings.toInternal()
	if err != nil {
		return err
	}

	defaultSettingsLock.Lock()
	defer defaultSettingsLock.Unlock()

	if defaultSettings != nil && s != defaultSettings {
		return errors.New("different default settings have already been installed")
	}

	defaultSettings = s

	return nil
}

// getDefaults will return the default settings or nil if they haven't been
// configured.
func getDefaults() *settings {
	defaultSettingsLock.RLock()
	defer defaultSettingsLock.RUnlock()
	return defaultSettings
}

type settings struct {
	log2m, regwidth                    int
	explicitAuto, sparseEnabled        bool
	explicitThreshold, sparseThreshold int

	// pwMaxMask is a mask that prevents overflow of HyperLogLog registers.
	pwMaxMask uint64

	// mBitsMask is a precomputed mask where the bottom-most regwidth bits are
	// set.
	mBitsMask uint64

	// alpha * m^2 (the constant in the "'raw' HyperLogLog estimator")
	alphaMSquared float64

	// smallEstimatorCutoff is the cutoff value of the estimator for using the
	// "small" range cardinality correction formula
	smallEstimatorCutoff float64

	// largeEstimatorCutoff is the cutoff value of the estimator for using the
	// "large" range cardinality correction formula
	largeEstimatorCutoff float64

	twoToL float64
}

// toInternal translates Settings to settings, validating them in the process.
// This function will also compute and populate auto-generated thresholds and
// constant values used by the Hll calculations and cache the result.
func (s Settings) toInternal() (*settings, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	settingsCacheLock.RLock()
	cachedSettings := settingsCache[s]
	settingsCacheLock.RUnlock()

	if cachedSettings != nil {
		return cachedSettings, nil
	}

	log2m := s.Log2m
	regwidth := s.Regwidth

	var explicitThreshold int
	explicitAuto := s.ExplicitThreshold == AutoExplicitThreshold
	if explicitAuto {
		explicitThreshold = calculateExplicitThreshold(log2m, regwidth)
	} else {
		explicitThreshold = s.ExplicitThreshold
	}

	sparseThreshold := 0
	if s.SparseEnabled {
		sparseThreshold = calculateSparseThreshold(log2m, regwidth)
	}

	twoToL := twoToL(log2m, regwidth)

	settings := settings{
		log2m:                log2m,
		regwidth:             regwidth,
		explicitAuto:         explicitAuto,
		explicitThreshold:    explicitThreshold,
		sparseEnabled:        s.SparseEnabled,
		sparseThreshold:      sparseThreshold,
		pwMaxMask:            pwMaxMask(regwidth),
		mBitsMask:            uint64((1 << uint(log2m)) - 1),
		alphaMSquared:        alphaMSquared(log2m),
		smallEstimatorCutoff: smallEstimatorCutoff(1 << uint(log2m)),
		largeEstimatorCutoff: largeEstimatorCutoff(twoToL),
		twoToL:               twoToL,
	}

	// install the settings.  note that if another equal set of settings had
	// been installed between our critical sections, the result is idempotent.
	settingsCacheLock.Lock()
	settingsCache[s] = &settings
	settingsCacheLock.Unlock()

	return &settings, nil
}

// validate ensures that all of the settings in s are within bounds.  It will
// throw an error if any of them are not.
func (s *Settings) validate() error {

	if s.Log2m < minimumLog2mParam {
		return fmt.Errorf("Log2m is too small.  Requires at least %d but got %d", minimumLog2mParam, s.Log2m)
	} else if s.Log2m > maximumLog2mParam {
		return fmt.Errorf("Log2m is too large.  Allows at most %d but got %d", maximumLog2mParam, s.Log2m)
	}

	if s.Regwidth < minimumRegwidthParam {
		return fmt.Errorf("Regwidth is too small.  Requires at least %d but got %d", minimumRegwidthParam, s.Regwidth)
	} else if s.Regwidth > maximumRegwidthParam {
		return fmt.Errorf("Regwidth is too large.  Allows at most %d but got %d", maximumRegwidthParam, s.Regwidth)
	}

	if s.ExplicitThreshold < minimumExpthreshParam {
		return fmt.Errorf("ExplicitThreshold is too small.  Requires at least %d but got %d", minimumExpthreshParam, s.ExplicitThreshold)
	} else if s.ExplicitThreshold > maximumExplicitThreshold {
		return fmt.Errorf("ExplicitThreshold is too large.  Allows at most %d but got %d", maximumExpthreshParam, s.ExplicitThreshold)
	}

	return nil
}

// toExternal translates the internal settings back to their exported version.
func (s *settings) toExternal() Settings {
	settings := Settings{
		Log2m:         s.log2m,
		Regwidth:      s.regwidth,
		SparseEnabled: s.sparseEnabled,
	}

	if s.explicitAuto {
		settings.ExplicitThreshold = AutoExplicitThreshold
	} else {
		settings.ExplicitThreshold = s.explicitThreshold
	}

	return settings
}

// calculateExplicitThreshold determines a good cutoff to switch between
// explicit and probabilistic storage.
func calculateExplicitThreshold(log2m, regwidth int) int {

	// NOTE:  This math matches the size calculation in the PostgreSQL impl.
	m := 1 << uint(log2m)
	fullRepresentationSize := divideBy8RoundUp(regwidth * m) /*round up to next whole byte*/
	numLongs := fullRepresentationSize / 8

	if numLongs > maximumExplicitThreshold {
		return maximumExplicitThreshold
	}

	return numLongs
}

// calculateSparseThreshold determines a good cutoff to switch between sparse
// and dense probabilistic storage.
func calculateSparseThreshold(log2m, regwidth int) int {

	m := 1 << uint(log2m)
	shortWordLength := log2m + regwidth

	largestPow2LessThanCutoff := math.Log2(float64(m*regwidth) / float64(shortWordLength))
	sparseThreshold := 1 << uint(largestPow2LessThanCutoff)

	return sparseThreshold
}

// pwMaxMask calculates the mask that is used to prevent overflow of HyperLogLog
// registers.
func pwMaxMask(regwidth int) uint64 {
	maxRegisterValue := (1 << uint(regwidth)) - 1
	return ^((1 << uint(maxRegisterValue-1)) - 1)
}

// alphaMSquared calculates the 'alpha-m-squared' constant (gamma times
// registerCount squared where gamma is based on the value of registerCount)
// used by the HyperLogLog algorithm.
func alphaMSquared(log2m int) float64 {

	m := float64(int(1) << uint(log2m))

	switch log2m {
	case 4:
		return 0.673 * m * m
	case 5:
		return 0.697 * m * m
	case 6:
		return 0.709 * m * m
	default:
		return (0.7213 / (1.0 + 1.079/m)) * m * m
	}
}

// smallEstimatorCutoff calculates the "small range correction" formula, in the
// HyperLogLog algorith based on the total number of registers (m)
func smallEstimatorCutoff(m int) float64 {
	return (float64(m) * 5) / 2
}

// largeEstimatorCutoff calculates The cutoff for using the "large range
// correction" formula, from the HyperLogLog algorithm, adapted for 64 bit
// hashes.  See http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll.
func largeEstimatorCutoff(twoToL float64) float64 {
	return twoToL / 30.0
}

// twoToL calculates 2 raised to L where L is the "large range correction
// boundary" described at http://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll.
func twoToL(log2m int, regwidth int) float64 {

	maxRegisterValue := (1 << uint(regwidth)) - 1

	// Since 1 is added to p(w) in the insertion algorithm, only
	// (maxRegisterValue - 1) bits are inspected hence the hash
	// space is one power of two smaller.
	pwBits := maxRegisterValue - 1
	totalBits := pwBits + log2m

	// NOTE : this can get larger than fits in a 64 bit integer.
	return math.Pow(2, float64(totalBits))
}
