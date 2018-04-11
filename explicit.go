package hll

import (
	"encoding/binary"
	"sort"
)

// explicitStorage is the observed set of raw values.
type explicitStorage map[uint64]struct{}

// overCapacity returns true when the size of the map has exceeded the
// configured explicit threshold.
func (s explicitStorage) overCapacity(settings *settings) bool {
	return len(s) > settings.explicitThreshold
}

func (s explicitStorage) sizeInBytes(settings *settings) int {
	return 8 * len(s)
}

// writeBytes writes the observed set of raw values as a series of 8 byte big
// ending values. Per the storage spec, they are sorted as signed 64 bit
// integers in ascending order.
func (s explicitStorage) writeBytes(settings *settings, bytes []byte) {

	// NOTE : the postgres hll implementation will reject a serialized value that is not in order.
	sortedValues := make([]int64, 0, len(s))
	for value := range s {
		sortedValues = append(sortedValues, int64(value))
	}

	sort.Slice(sortedValues, func(i, j int) bool { return sortedValues[i] < sortedValues[j] })

	for i, value := range sortedValues {
		pos := i * 8
		binary.BigEndian.PutUint64(bytes[pos:pos+8], uint64(value))
	}
}

// fromBytes reads big endian 8 byte values from the byte slice.  It will return
// an error if the provided byte slice is not evenly divisible by 8.
func (s explicitStorage) fromBytes(settings *settings, bytes []byte) error {

	// if the length doesn't divide evenly into 8 byte words, they have been
	// truncated.  note that it's not possible to determine if data is missing
	// if a multiple of 8 bytes has been lost because the storage spec doesn't
	// provide for a count.
	if len(bytes)%8 != 0 {
		return ErrInsufficientBytes
	}

	for i := 0; i < len(bytes); i += 8 {
		buf := bytes[i : i+8]
		value := binary.BigEndian.Uint64(buf)
		s[value] = struct{}{}
	}

	return nil
}

func (s explicitStorage) copy() storage {
	o := make(explicitStorage)
	for k, v := range s {
		o[k] = v
	}

	return o
}
