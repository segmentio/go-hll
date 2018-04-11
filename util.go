package hll

func divideBy8RoundUp(i int) int {
	result := i >> 3
	if remainder := i & 0x7; remainder > 0 {
		result++
	}
	return result
}

// readBits reads nBits from the provided address in the byte array and returns
// them as the LSB of a uint64.  The address is the 0-indexed bit position where
// 0 equates to the MSB in the 0th byte, 63 is be the LSB in the 0th byte, 64 is
// the MSB bit in the 1st byte, and so on.
func readBits(bytes []byte, addr int, nBits int) uint64 {

	idx := addr >> 3  /*divide by 8*/
	pos := addr & 0x7 /*mod 8*/
	value := uint64(0)

	bitsRequired := nBits
	for bitsRequired > 0 {

		bitsAvailable := 8 - pos
		if bitsAvailable > bitsRequired {
			bitsAvailable = bitsRequired
		}

		// this is effectively a no-op on the first loop...zero will stay zero.
		// on subsequent loops, it will shift the value down by the required
		// number of bits.
		value = value << uint(bitsAvailable)

		mask := ((byte(1) << uint(bitsAvailable)) - 1) << uint(8-pos-bitsAvailable)
		bits := bytes[idx] & mask
		if pos+bitsAvailable != 8 {
			bits = bits >> uint(8-(pos+bitsAvailable))
		}
		value |= uint64(bits)

		pos += bitsAvailable

		// advance to the next byte if required.
		if pos == 8 {
			idx++
			pos = 0
		}

		bitsRequired -= bitsAvailable
	}

	return value
}

// writeBits writes the nBits least significant bits of value to the provided
// address in the byte array.  The address is the 0-indexed bit position where 0
// equates to the MSB in the 0th byte, 63 is be the LSB in the 0th byte, 64 is
// the MSB bit in the 1st byte, and so on.
func writeBits(bytes []byte, addr int, value uint64, nBits int) {

	idx := addr >> 3  /*divide by 8*/
	pos := addr & 0x7 /*mod 8*/

	for nBits > 0 {

		bitsToWrite := 8 - pos
		if bitsToWrite > nBits {
			bitsToWrite = nBits
		}

		mask := byte(1<<uint(bitsToWrite)) - 1
		partToWrite := mask & byte(value>>uint(nBits-bitsToWrite))

		// shift into position if required.
		if pos+bitsToWrite != 8 {
			partToWrite = partToWrite << uint(8-(pos+bitsToWrite))
		}

		bytes[idx] = bytes[idx] | partToWrite

		pos += bitsToWrite
		if pos == 8 {
			idx++
			pos = 0
		}

		nBits -= bitsToWrite
	}
}
