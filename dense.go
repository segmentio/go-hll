package hll

import (
	"encoding/binary"
)

// denseStorage is essentially a bit vector composed of uint64 words.  It is
// composed of uint64s instead of bytes to minimize the amount of reads that
// span array inidices.  Doing so comes at the cost of extra work when
// reading/writing to bytes because the words have to be either reassembled or
// broken up.  This represents an active choice to make the Hll manipulation
// operations (union, cardinality, etc.) more performant at the cost of
// additional serialization overhead.
type denseStorage []uint64

// newDenseStorage allocates a new instance with sufficient space to store all
// of the register values.
func newDenseStorage(settings *settings) denseStorage {
	bytes := divideBy8RoundUp((1 << uint(settings.log2m)) * settings.regwidth)
	return make(denseStorage, divideBy8RoundUp(bytes))
}

// overCapacity always returns false for dense storage because there is no
// upgrade path.
func (s denseStorage) overCapacity(settings *settings) bool {
	return false
}

// sizeInBytes returns the number of bytes required to represent every register
// value.
func (s denseStorage) sizeInBytes(settings *settings) int {
	// NOTE : this does not calculate based on the array size b/c it's possible
	//        that not every byte in the final word is used.
	return divideBy8RoundUp((1 << uint(settings.log2m)) * settings.regwidth)
}

// writeBytes writes out each register values in order.
func (s denseStorage) writeBytes(settings *settings, bytes []byte) {

	byteOffset := 0
	nWords := cap(bytes) / 8
	for i := 0; i < nWords; i++ {
		binary.BigEndian.PutUint64(bytes[byteOffset:], s[i])
		byteOffset += 8
	}

	// deal with any remaining bytes.  the binary writing function above doesn't
	// handle the case where there are not exactly 8 bytes to write.
	remainder := cap(bytes) % 8
	if remainder > 0 {
		lastWord := s[nWords]
		for i := 0; i < remainder; i++ {
			bytes[byteOffset+i] = byte(lastWord >> uint(64-(8*(i+1))))
		}
	}
}

// fromBytes deserializes the binary register values into this storage instance.
func (s denseStorage) fromBytes(settings *settings, bytes []byte) error {

	// ensure that every register is accounted for in the input byte slice.
	if len(bytes) != divideBy8RoundUp((1<<uint(settings.log2m))*settings.regwidth) {
		return ErrInsufficientBytes
	}

	n := len(bytes)
	nWords := n / 8

	for i := 0; i < nWords; i++ {
		offset := i * 8
		s[i] = binary.BigEndian.Uint64(bytes[offset : offset+8])
	}

	// deal with any remaining bytes.  the binary reading function above doesn't
	// handle the case where there are not exactly 8 bytes to read.
	remainder := n % 8
	if remainder > 0 {
		lastValue := uint64(0)

		for i := 0; i < remainder; i++ {
			shiftAmount := uint(64 - (8 * (i + 1)))
			lastValue |= uint64(bytes[n-(remainder-i)]) << shiftAmount
		}

		s[len(s)-1] = lastValue
	}

	return nil
}

func (s denseStorage) copy() storage {
	o := make(denseStorage, len(s))
	copy(o, s)
	return o
}

func (s denseStorage) indicator(settings *settings) (float64, int) {

	numReg := 1 << uint(settings.log2m)

	idx := 0
	pos := 0
	curr := s[idx]
	mask := uint64(settings.mBitsMask << (64 - uint(settings.regwidth)))

	sum := float64(0)
	numberOfZeros := 0

	for i := 0; i < numReg; i++ {

		var value uint64

		bitsAvailable := 64 - pos

		if bitsAvailable >= settings.regwidth {

			value = (curr & mask) >> uint(64-pos-settings.regwidth)
			pos += settings.regwidth
			mask = mask >> uint(settings.regwidth)

		} else {

			nLowerBits := settings.regwidth - bitsAvailable

			upperBits := uint64(0)
			if bitsAvailable > 0 {
				upperBits = (curr & mask) << uint(nLowerBits)
			}

			// move index into the backing array forward and reset the position
			// and mask.
			idx++
			curr = s[idx]

			// read lower bits from the new location.
			lowerMask := uint64((1<<uint(nLowerBits))-1) << uint(64-nLowerBits)
			lowerBits := (curr & lowerMask) >> uint(64-nLowerBits)

			value = upperBits | lowerBits

			// prepare pos and mask for the next loop
			pos = nLowerBits
			mask = (settings.mBitsMask << uint(64-settings.regwidth)) >> uint(pos)
		}

		// compute the "indicator function" -- indicator(2^(-M[j])) where M[j]
		// is the 'j'th register value
		sum += 1.0 / float64(uint64(1)<<value)
		if value == 0 {
			numberOfZeros++
		}
	}

	return sum, numberOfZeros
}

func (s denseStorage) setIfGreater(settings *settings, regnum int, value byte) {

	idx, pos := s.calcPosition(int(regnum), int(settings.regwidth))
	nBits := int(settings.regwidth)

	// single index write.
	if pos+nBits <= 64 {
		mask := (uint64(1) << uint(settings.regwidth)) - 1
		partToWrite := uint64(value) & mask

		// shift into position if required.
		shiftLeft := uint(0)
		if pos+nBits < 64 {
			shiftLeft = uint(64 - (pos + nBits))
			partToWrite = partToWrite << shiftLeft
			mask = mask << shiftLeft
		}

		currVal := byte((mask & s[idx]) >> shiftLeft)
		if value > currVal {
			s[idx] = (^mask & s[idx]) | partToWrite
		}
	} else {
		nBitsUpper := uint(64 - pos)
		nBitsLower := uint(nBits) - nBitsUpper

		maskUpper := (uint64(1) << nBitsUpper) - 1
		maskLower := (uint64(1) << nBitsLower) - 1

		upper := (s[idx] & maskUpper) << nBitsLower
		lower := s[idx+1] >> (64 - nBitsLower)
		currVal := upper | lower

		if value >= byte(currVal) {
			partToWriteUpper := (uint64(value) >> nBitsLower) & maskUpper
			partToWriteLower := (uint64(value) & maskLower) << (64 - nBitsLower)

			maskLowerShifted := maskLower << (64 - nBitsLower)

			s[idx] = (^maskUpper & s[idx]) | partToWriteUpper
			s[idx+1] = (^maskLowerShifted & s[idx+1]) | partToWriteLower
		}
	}
}

// union is a special operation on denseStorage that will union other into the
// receiver as a linear pass through the two backing slices.
func (s denseStorage) union(settings *settings, other denseStorage) {

	numReg := 1 << uint(settings.log2m)

	idx := 0
	pos := 0

	thisWord := s[idx]
	otherWord := other[idx]
	computed := thisWord

	mask := settings.mBitsMask << uint(64-settings.regwidth)

	for i := 0; i < numReg; i++ {

		bitsAvailable := 64 - pos

		if bitsAvailable >= settings.regwidth {

			thisValue := thisWord & mask
			otherValue := otherWord & mask

			// NOTE : no need to shift into position to compare or mix back in.
			if otherValue > thisValue {
				computed = (^mask & computed) | otherValue
			}

			pos += settings.regwidth
			mask = mask >> uint(settings.regwidth)

		} else {

			// there are three possible outcomes to comparing the upper bits.
			// note that if the upper bits of one are greater than the other,
			// then it's strictly greater than the other value regardless of the
			// lower bits.  so:
			//
			// 1. the other's upper bits are greater...write all of the other's
			//    bits into our register.
			// 2. our upper bits are greater...keep our register value
			// 3. the upper bits are equal...compare the lower bits and possibly
			//    overwrite our register's lower bits with the other's.
			//
			otherIsGreater := false
			thisIsGreater := false

			if bitsAvailable > 0 {

				thisValue := thisWord & mask
				otherValue := otherWord & mask

				if otherValue > thisValue {
					computed = (^mask & computed) | otherValue
					otherIsGreater = true
				} else if otherValue < thisValue {
					thisIsGreater = true
				}
			}

			if computed != thisWord {
				s[idx] = computed
			}

			// move index into the backing array forward and reset the position
			// and mask.
			idx++
			thisWord = s[idx]
			otherWord = other[idx]
			nLowerBits := settings.regwidth - bitsAvailable
			computed = thisWord

			if !thisIsGreater {
				// read lower bits from the new location.
				lowerMask := uint64((1<<uint(nLowerBits))-1) << uint(64-nLowerBits)
				thisLowerBits := thisWord & lowerMask
				otherLowerBits := otherWord & lowerMask

				if (otherIsGreater && thisLowerBits != otherLowerBits) || otherLowerBits > thisLowerBits {
					computed = (^lowerMask & computed) | otherLowerBits
				}
			}

			// prepare pos and mask for the next loop
			pos = nLowerBits
			mask = (settings.mBitsMask << uint(64-settings.regwidth)) >> uint(pos)
		}
	}

	if computed != thisWord {
		s[idx] = computed
	}
}

// get extracts a single register value.  It is provided to enable union-ing two
// dense storage instance with different Hll settings.
func (s denseStorage) get(regnum, regwidth int) byte {

	idx, pos := s.calcPosition(regnum, regwidth)

	// single index read.
	if pos+regwidth <= 64 {

		// shift into position if required.
		shiftLeft := uint(0)
		if pos+regwidth < 64 {
			shiftLeft = uint(64 - (pos + regwidth))
		}

		mask := ((uint64(1) << uint(regwidth)) - 1) << shiftLeft

		return byte((mask & s[idx]) >> shiftLeft)
	}

	// boundary read
	nBitsUpper := uint(64 - pos)
	nBitsLower := uint(regwidth) - nBitsUpper

	maskUpper := (uint64(1) << nBitsUpper) - 1

	upper := (s[idx] & maskUpper) << nBitsLower
	lower := s[idx+1] >> (64 - nBitsLower)

	return byte(upper | lower)
}

func (s denseStorage) calcPosition(regnum, regwidth int) (int, int) {
	addr := regnum * regwidth
	idx := addr >> 6   /*divide by 64*/
	pos := addr & 0x3f /*remainder 64*/
	return idx, pos
}
