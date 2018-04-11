package hll

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
)

// storageType is an enum whose values match the type values in the hll storage
// spec.  In the the spec, the "dense" value is referred to as "full".  We use
// the name dense because we fined it to be more descriptive.
type storageType int

const (
	undefined storageType = iota
	empty
	explicit
	sparse
	dense
)

// ErrInsufficientBytes is returned by FromBytes in cases where the provided
// byte slice is truncated.
var ErrInsufficientBytes = errors.New("insufficient bytes to deserialize Hll")

// ErrIncompatible is returned by StrictUnion in cases where the two Hlls have
// incompatible settings that prevent the operation from occurring.
var ErrIncompatible = errors.New("cannot StrictUnion Hlls with different regwidth or log2m settings")

// Hll is a probabilistic set of hashed elements.  It supports add and union
// operations in addition to estimating the cardinality.  The zero value is an
// empty set, provided that Defaults has been invoked with default settings.
// Otherwise, operations on the zero value will cause a panic as it would be a
// coding error to attempt operations without first configuring the library.
type Hll struct {
	settings *settings
	storage  storage
}

// NewHll creates a new Hll with the provided settings.  It will return an error
// if the settings are invalid.  Since an application usually deals with
// homogeneous Hlls, it's preferable to install default settings and use the
// zero value. This function is provided in case an application must juggle
// different configurations.
func NewHll(s Settings) (Hll, error) {

	settings, err := s.toInternal()
	if err != nil {
		return Hll{}, err
	}

	return Hll{settings: settings}, nil
}

// FromBytes deserializes the provided byte slice into an Hll.  It will return
// an error if the version is anything other than 1, if the leading bytes
// specify an invalid configuration, or if the byte slice is truncated.
func FromBytes(bytes []byte) (Hll, error) {

	if len(bytes) < 3 {
		return Hll{}, ErrInsufficientBytes
	}

	version, storageType := int(bytes[0]>>4), storageType(bytes[0]&0xf)
	if version != 1 {
		return Hll{}, fmt.Errorf("unsupported Hll version: %d", version)
	}

	// NOTE : this means undefined cannot be instantiated!  this is compatible
	//        with the Java impl even though the PG impl would allow it.
	if storageType < empty || storageType > dense {
		return Hll{}, fmt.Errorf("invalid Hll type: %d", storageType)
	}

	regwidth, log2m := (bytes[1]>>5)+1, bytes[1]&0x1f

	sparseEnabled, explicitThreshold := unpackCutoffByte(bytes[2])

	settings := Settings{
		Log2m:             int(log2m),
		Regwidth:          int(regwidth),
		SparseEnabled:     sparseEnabled,
		ExplicitThreshold: explicitThreshold,
	}

	internalSettings, err := settings.toInternal()

	// NOTE : in this error case, the Hll is undefined and will not
	//        auto-initialize to an empty hll if an exported function is called.
	if err != nil || storageType == undefined {
		return Hll{}, err
	}

	h := Hll{settings: internalSettings}

	switch storageType {
	case explicit:
		h.storage = make(explicitStorage)
	case sparse:
		h.storage = make(sparseStorage)
	case dense:
		h.storage = newDenseStorage(h.settings)
	}

	// trim off the header bytes and populate the storage.
	storageByes := bytes[3:]
	if h.storage != nil {
		err = h.storage.fromBytes(h.settings, storageByes)
	}

	if err != nil {
		return Hll{}, err
	}

	return h, nil
}

// Settings returns the Settings for this Hll.
func (h *Hll) Settings() Settings {
	h.initOrPanic()
	return h.settings.toExternal()
}

// AddRaw adds the observed value into the Hll.  The value is expected to have
// been hashed with a good hash function such as Murmur3 or xxHash.  If the
// value does not have sufficient entropy, then the resulting cardinality
// estimations will not be accurate.
//
// There is an edge case where the raw value of 0 is not added to the Hll.  In
// the sparse or dense representation, a zero value would not affect the
// cardinality calculations because there are no set bits to observe.  In order
// to be consistent, the explicit representation will therefore ignore a 0
// value.
func (h *Hll) AddRaw(value uint64) {

	h.initOrPanic()

	// by contract...ignore zero.
	if value == 0 {
		return
	}

	// bootstrap case...if this is an empty HLL, it needs storage so we can add
	// to it.
	if h.storage == nil {
		if h.settings.explicitThreshold > 0 {
			h.storage = make(explicitStorage)
		} else if h.settings.sparseEnabled {
			h.storage = make(sparseStorage)
		} else {
			h.storage = newDenseStorage(h.settings)
		}
	}

	switch s := h.storage.(type) {
	case explicitStorage:
		s[value] = struct{}{}
	case registers:
		// following documentation courtesy of the java implementation:
		//
		// p(w): position of the least significant set bit (one-indexed)
		// By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register
		// value)
		//
		// By construction of pwMaxMask,
		//      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
		// thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
		// thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
		substreamValue := uint64(value >> uint(h.settings.log2m))
		if substreamValue == 0 {
			// The paper does not cover p(0x0), so the special value 0 is used.
			// 0 is the original initialization value of the registers, so by
			// doing this the multiset simply ignores it. This is acceptable
			// because the probability is 1/(2^(2^registerSizeInBits)).
			return
		}

		// NOTE : trailing zeros == the 0-based index of the least significant 1
		//        bit.
		pW := (byte)(1 + bits.TrailingZeros64(substreamValue|h.settings.pwMaxMask))
		// NOTE:  no +1 as in paper since 0-based indexing
		i := int(value & h.settings.mBitsMask)

		s.setIfGreater(h.settings, i, pW)
	}

	if h.storage.overCapacity(h.settings) {
		h.upgrade()
	}
}

// Cardinality estimates the number of values that have been added to this Hll.
func (h *Hll) Cardinality() uint64 {

	h.initOrPanic()

	switch s := h.storage.(type) {
	case explicitStorage:
		return uint64(len(s))
	case registers:
		sum, numberOfZeroes /*"V" in the paper*/ := s.indicator(h.settings)

		// apply the estimate and correction to the indicator function
		estimator := h.settings.alphaMSquared / sum

		if (numberOfZeroes != 0) && (estimator < h.settings.smallEstimatorCutoff) {
			// following documentation courtesy of the java implementation:
			// The "small range correction" formula from the HyperLogLog
			// algorithm. Only appropriate if both the estimator is smaller than
			// (5/2) * m and there are still registers that have the zero value.
			m := 1 << uint(h.settings.log2m)
			smallEstimator := float64(m) * math.Log(float64(m)/float64(numberOfZeroes))
			return uint64(math.Ceil(smallEstimator))
		}

		if estimator <= h.settings.largeEstimatorCutoff {
			return uint64(math.Ceil(estimator))
		}

		// following documentation courtesy of the java implementation:
		// The "large range correction" formula from the HyperLogLog algorithm,
		// adapted for 64 bit hashes. Only appropriate for estimators whose
		// value exceeds the calculated cutoff.
		largeEstimator := -1 * h.settings.twoToL * math.Log(1.0-(estimator/h.settings.twoToL))
		return uint64(math.Ceil(largeEstimator))

	default:
		// nil case.
		return 0
	}
}

// Union will calculate the union of this Hll and the other Hll and store the
// results into the receiver.
//
// Unlike StrictUnion, it allows unions between Hlls with different settings to
// be combined, though doing so is not recommended because it will result in a
// loss of accuracy.
//
// As long as your application uses a single group of settings, it is safe to
// use this function.  If there is a possibility that you may union two Hlls
// with incompatible settings, then it's safer to use StrictUnion and check for
// errors.
func (h *Hll) Union(other Hll) {
	if err := h.union(other, false); err != nil {
		// since the above union call passes false to strict, the only way an
		// error could pop up would be due to a bug in code.  handling
		// explicitly nonetheless b/c it was flagged by gosec.
		panic(err)
	}
}

// StrictUnion will calculate the union of this Hll and the other Hll and store
// the results into the receiver.  It will return an error if the two Hlls are
// not compatible where compatibility is defined as having the same register
// width and log2m.  explicit and sparse thresholds don't factor into
// compatibility.
func (h *Hll) StrictUnion(other Hll) error {
	return h.union(other, true)
}

func (h *Hll) union(other Hll, strict bool) error {

	// this is kind of an ugly method...this is where the abstraction of storage
	// breaks down because something needs to know how to convert between and
	// union the different storage types.

	h.initOrPanic()
	other.initOrPanic()

	sameSettings := h.settings.regwidth == other.settings.regwidth && h.settings.log2m == other.settings.log2m

	if strict && !sameSettings {
		return ErrIncompatible
	}

	// other is empty...there's nothing to do.
	if other.storage == nil {
		return nil
	}

	// if this one is empty, deep copy the other's storage.
	if h.storage == nil {
		// there's an edge case if sparse is disabled but the other is sparse.
		// in that case, we need to go straight to dense and copy over reg
		// values.
		if sparse, ok := other.storage.(sparseStorage); ok {
			if h.settings.sparseEnabled {
				h.storage = other.storage.copy()
			} else {
				// edge case...it's possible that the other hll is sparse but
				// that this one does not have sparse enabled.
				h.storage = sparseToDense(h.settings, sparse)
			}
		} else {
			h.storage = other.storage.copy()
		}
		return nil
	}

	// otherwise, the union operation depends on which types we're union-ing.
	switch otherStorage := other.storage.(type) {
	case explicitStorage:
		// regardless of the type of the hll we're union-ing into, add the
		// other's identifiers into this one.
		h.addFromExplicit(otherStorage)
	case sparseStorage:
		switch thisStorage := h.storage.(type) {
		case explicitStorage:
			// if this is explicit, then make a deep copy of the sparse storage
			// and then add all the values from the explicit set.  if sparse is
			// not enabled, then we need to go straight to dense storage and
			// copy the sparse registers prior to adding the explicit values.
			if h.settings.sparseEnabled {
				h.storage = otherStorage.copy()
			} else {
				h.storage = sparseToDense(h.settings, otherStorage)
			}
			h.addFromExplicit(thisStorage)
		case registers:
			// if the hll being copied into is sparse or dense, then iterate
			// over the sparse storage and copy over
			// larger register values.
			for k, v := range otherStorage {
				// ensure that the value fits within the sparse storage's
				// register.  it's possible that the value may be greater than
				// the max register value in the case of a non-strict union
				// where the other has wider registers.
				v = v & byte(h.settings.mBitsMask)
				thisStorage.setIfGreater(h.settings, int(k), v)
			}
		}
	case denseStorage:
		switch thisStorage := h.storage.(type) {
		case explicitStorage:
			// if this hll is explicit, then make a deep copy of the dense
			// storage and then add all the values from the explicit set.
			h.storage = otherStorage.copy()
			h.addFromExplicit(thisStorage)
		case sparseStorage:
			// if this hll is sparse, then upgrade it to a dense hll and then do
			// a dense union.
			h.upgrade()
			denseUnion(h.storage.(denseStorage), otherStorage, h.settings, other.settings)
		case denseStorage:
			denseUnion(thisStorage, otherStorage, h.settings, other.settings)
		}
	}

	// once union is complete, upgrade the storage type if we've gone over
	// capacity.
	if h.storage.overCapacity(h.settings) {
		h.upgrade()
	}

	return nil
}

// ToBytes returns a byte slice with the serialized Hll value per the storage
// spec https://github.com/aggregateknowledge/hll-storage-spec/blob/master/STORAGE.md.
func (h *Hll) ToBytes() []byte {

	h.initOrPanic()

	var storageType storageType

	switch h.storage.(type) {
	case explicitStorage:
		storageType = explicit
	case sparseStorage:
		storageType = sparse
	case denseStorage:
		storageType = dense
	case nil:
		storageType = empty
	}

	bytesNeeded := 0

	if h.storage != nil {
		bytesNeeded = h.storage.sizeInBytes(h.settings)
	}

	bytes := make([]byte, 3 /*header bytes*/ +bytesNeeded)

	bytes[0] = (1 << 4) | byte(storageType)
	bytes[1] = byte(((h.settings.regwidth - 1) << 5) | h.settings.log2m)
	bytes[2] = packCutoffByte(h.settings)

	if h.storage != nil {
		h.storage.writeBytes(h.settings, bytes[3:])
	}

	return bytes
}

// Clear resets this Hll.  Unlike other implementations that leave the backing
// storage in place, this resets the Hll to the empty, zero value.
func (h *Hll) Clear() {

	h.initOrPanic()

	h.storage = nil
}

// initOrPanic is used to lazily initialize a zero value to an empty Hll (in the
// presence of default settings) or to panic if the operation is being evaluated
// against an undefined Hll.  If there are no default settings, the zero value
// will also cause a panic.
func (h *Hll) initOrPanic() {

	// h is initialized if it has non-nil settings.  that will either happen by
	// lazy initialization or via explicit instantiation with NewHll
	if h.settings != nil {
		return
	}

	defaults := getDefaults()
	if defaults == nil {
		panic("attempted operation on empty Hll without default settings")
	}

	h.settings = defaults
}

// upgrade will bump up the storage to the next tier depending on the configured
// settings.  It's assumed that the current storage has already been verified to
// be over capacity.
//
// See https://github.com/aggregateknowledge/hll-storage-spec/blob/master/STORAGE.md#schema-version-1
func (h *Hll) upgrade() {

	// upgrade paths supported:
	// explicit -> either probabilistic type.  add for each element in the set.
	// sparse -> dense.  copy register values.
	//
	// since this is an internal method, assume that there are no invalid
	// upgrade paths being requested.
	switch s := h.storage.(type) {
	case explicitStorage:
		if h.settings.sparseEnabled {
			h.storage = make(sparseStorage)
		} else {
			h.storage = newDenseStorage(h.settings)
		}

		for value := range s {
			h.AddRaw(value)
		}
	case sparseStorage:
		ds := newDenseStorage(h.settings)
		h.storage = ds
		for regnum, value := range s {
			ds.setIfGreater(h.settings, int(regnum), value)
		}
	}
}

// addFromExplicit loops over all values in the provided storage and adds them
// to this Hll.
func (h *Hll) addFromExplicit(explicit explicitStorage) {
	for k := range explicit {
		h.AddRaw(k)
	}
}

// sparseToDense converts the provided sparse storage to dense.
func sparseToDense(settings *settings, sparse sparseStorage) denseStorage {
	dense := newDenseStorage(settings)
	for k, v := range sparse {
		dense.setIfGreater(settings, int(k), v)
	}
	return dense
}

// denseUnion handles union-ing two denseStorage instances.  In case the two
// settings have compatible regwidth and log2m settings, the efficient
// single-pass dense union will be used.  If they differ, then register values
// will be compared one-by-one, taking the largest value for each.
func denseUnion(thisStorage, otherStorage denseStorage, thisSettings, otherSettings *settings) {
	// if the settings are compatible, use the optimized union function.
	// otherwise, loop over each register and call get on other and setIfGreater
	// on this.
	if thisSettings.log2m == otherSettings.log2m && thisSettings.regwidth == otherSettings.regwidth {
		thisStorage.union(thisSettings, otherStorage)
	} else {
		for i := 0; i < 1<<uint(thisSettings.log2m); i++ {
			// mask the other's register value with our mBits setting to ensure
			// an accurate comparison.
			regVal := otherStorage.get(i, otherSettings.log2m) & byte(thisSettings.mBitsMask)
			thisStorage.setIfGreater(thisSettings, i, regVal)
		}
	}
}

// packCutoffByte is a helper function to serialize the byte that contains
// explicit and sparse settings.
func packCutoffByte(settings *settings) byte {

	var threshold byte
	if settings.explicitAuto {
		// per the spec, set all 6 bits.
		threshold = 63
	} else if settings.explicitThreshold == 0 {
		threshold = 0
	} else {
		// pack as an exponent of 2 per the spec.  note that this can be a
		// destructive transformation if the threshold is not a power of 2.  in
		// that case, this behaves the same as the java library where it rounds
		// down.
		threshold = byte(bits.Len32(uint32(settings.explicitThreshold))) - 1
	}

	cutoff := threshold
	if settings.sparseEnabled {
		cutoff |= 1 << 6
	}

	return cutoff
}

// unpackCutoffByte is a helper function to deserialize the byte that contains
// explicit and sparse settings.
func unpackCutoffByte(b byte) (bool, int) {

	sparseEnabled := b>>6 == 1
	expThreshold := b & 0x3f

	if expThreshold == 0 {
		return sparseEnabled, 0
	}

	if expThreshold == 63 {
		return sparseEnabled, -1
	}

	return sparseEnabled, 1 << (expThreshold - 1)
}
