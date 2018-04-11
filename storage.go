package hll

// storage is an interface that sets up the interaction between the Hll and the backing data.  this interface will be
// implemented for each valid, non-empty storageType.
type storage interface {

	// overCapacity returns true when this storage has grown beyond the target limits in the settings.  The Hll should
	// then upgrade it to another storage type.  Upgrade details are left to the Hll because it involves knowing how to
	// convert from one storage type to another, which is beyond the scope of this interface.
	overCapacity(settings *settings) bool

	// sizeInBytes returns the number of bytes required to serialize this storage.  This method is used by the Hll to
	// determine how large of a byte array to allocate for serialization.
	sizeInBytes(settings *settings) int

	// writeBytes serializes the storage into the provided byte slice.  The storage can assume that the slice has at
	// least as many bytes as indicated by sizeInBytes.
	writeBytes(settings *settings, bytes []byte)

	// fromBytes deserializes the provided byte slice into this storage object.  It will return an error in case the
	// byte slice contains invalid information or is truncated.
	fromBytes(settings *settings, bytes []byte) error

	// copy returns a deep copy of this storage.
	copy() storage
}

// registers is an add-on interface to storage that is implemented by the probabalistic types.
type registers interface {

	// setIfGreater sets the register value of register regnum to the provided value if and only if it's greater than
	// the current value.
	setIfGreater(settings *settings, regnum int, value byte)

	// indicator computes the "indicator function" (Z in the HLL paper).  It additionally returns the number of
	// registers whose value is zero (V in the paper).  The returned values are used to drive cardinality calculations.
	//
	// For reference, Z = indicator(2^(-M[j])) for all j from 0 -> num registers where M[j] is the register value.
	indicator(settings *settings) (float64, int)
}
