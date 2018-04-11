package hll

import "sort"

type sparseStorage map[int32]byte

func (s sparseStorage) overCapacity(settings *settings) bool {
	return len(s) > settings.sparseThreshold
}

func (s sparseStorage) sizeInBytes(settings *settings) int {
	return divideBy8RoundUp(int(settings.log2m+settings.regwidth) * len(s))
}

func (s sparseStorage) writeBytes(settings *settings, bytes []byte) {

	// per the storage spec, the registers must be in sorted order.  i'm not
	// sure if other implementations will complain if that's not the case, but
	// better safe than sorry.
	sortedRegisters := make([]int32, 0, len(s))
	for reg := range s {
		sortedRegisters = append(sortedRegisters, int32(reg))
	}
	sort.Slice(sortedRegisters, func(i, j int) bool { return sortedRegisters[i] < sortedRegisters[j] })

	addr := 0
	bitsPerRegister := int(settings.log2m + settings.regwidth)

	for _, reg := range sortedRegisters {
		writeBits(bytes, addr, (uint64(reg)<<uint(settings.regwidth))|uint64(s[reg]), bitsPerRegister)
		addr += bitsPerRegister
	}
}

func (s sparseStorage) fromBytes(settings *settings, bytes []byte) error {

	bitsPerRegister := int(settings.regwidth + settings.log2m)
	regMask := byte((1 << uint(settings.regwidth)) - 1)

	// take the floor of the number of bits divided by the width of the regnum + width
	numRegisters := (8 * len(bytes)) / bitsPerRegister

	for i := 0; i < numRegisters; i++ {
		regAndVal := readBits(bytes, i*bitsPerRegister, bitsPerRegister)
		s[int32(regAndVal>>uint(settings.regwidth))] = byte(regAndVal) & regMask
	}

	return nil
}

func (s sparseStorage) copy() storage {
	o := make(sparseStorage)
	for k, v := range s {
		o[k] = v
	}

	return o
}

func (s sparseStorage) setIfGreater(settings *settings, regnum int, value byte) {
	if existing := s[int32(regnum)]; value > existing {
		s[int32(regnum)] = value
	}
}

func (s sparseStorage) indicator(settings *settings) (float64, int) {

	// compute the "indicator function" -- indicator(2^(-M[j])) where M[j] is the
	// 'j'th register value
	sum := float64(0)
	for _, v := range s {
		sum += 1.0 / float64(uint64(1)<<v)
	}

	// account for all the unset registers in the indicator.
	numberOfZeros := (1 << uint(settings.log2m)) - len(s)
	sum += float64(numberOfZeros)

	return sum, numberOfZeros
}
