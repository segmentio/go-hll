package hll

import (
	"bufio"
	"compress/gzip"
	"encoding/hex"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type addTestCase struct {
	hll         Hll
	toAdd       uint64
	cardinality uint64
}

type unionTestCase struct {
	hll         Hll
	toUnion     Hll
	cardinality uint64
}

// Test_IntegrationSuite runs through the integration tests produced by running
// the code at https://github.com/aggregateknowledge/java-hll/blob/master/src/test/java/net/agkn/hll/IntegrationTestGenerator.java
// Multiple runs were created by editing the constant REGWIDTH and LOG2M
// variables. For convenience, the test inputs are stored in this project as
// compressed files
func Test_IntegrationSuite(t *testing.T) {

	suites, err := ioutil.ReadDir("integration_tests")
	require.NoError(t, err)

	for _, suites := range suites {

		suiteDir := "integration_tests/" + suites.Name()
		files, err := ioutil.ReadDir(suiteDir)
		require.NoError(t, err)

		for _, file := range files {

			t.Run(suiteDir+"/"+file.Name(), func(t *testing.T) {

				test := suiteDir + "/" + file.Name()

				t.Parallel()

				reader, err := os.Open(test)
				require.NoError(t, err)
				defer reader.Close()

				decompresed, err := gzip.NewReader(reader)
				require.NoError(t, err)
				defer decompresed.Close()

				scanner := bufio.NewScanner(decompresed)
				require.True(t, scanner.Scan()) // discard header.
				require.NoError(t, scanner.Err())

				if strings.Contains(test, "_add_") {

					var hll Hll
					first := true
					lineNo := 2 // line 1 was discarded above.

					for scanner.Scan() {
						tt := parseAddTestCase(t, scanner, lineNo)

						if first {
							hll = tt.hll
						}

						hll.AddRaw(tt.toAdd)

						require.Equal(t, reflect.TypeOf(tt.hll.storage), reflect.TypeOf(hll.storage), "wrong storage type at line %d", lineNo)
						require.Equal(t, tt.cardinality, hll.Cardinality(), "incorrect cardinality at line %d, hll: \\x%s", lineNo, hex.EncodeToString(hll.ToBytes()))
						require.Equal(t, hex.EncodeToString(tt.hll.ToBytes()), hex.EncodeToString(hll.ToBytes()), "incorrect serialized value at line %d", lineNo)

						lineNo++
						first = false
					}
				} else {

					var hll Hll
					first := true
					lineNo := 2 // line 1 was discarded above.

					for scanner.Scan() {
						tt := parseUnionTestCase(t, scanner, lineNo)

						if first {
							hll = tt.hll
						}

						err := hll.StrictUnion(tt.toUnion)
						require.NoError(t, err)

						require.Equal(t, tt.cardinality, hll.Cardinality(), "incorrect cardinality at line %d, hll: \\x%s", lineNo, hex.EncodeToString(hll.ToBytes()))

						// NOTE : not always equal b/c sparse threshold is not written into the HLL
						if reflect.TypeOf(tt.hll.storage) == reflect.TypeOf(hll.storage) {
							require.Equal(t, hex.EncodeToString(tt.hll.ToBytes()), hex.EncodeToString(hll.ToBytes()), "incorrect serialized value at line %d", lineNo)
						}

						lineNo++
						first = false
					}
				}
			})
		}
	}
}

func parseAddTestCase(t *testing.T, scanner *bufio.Scanner, lineNo int) addTestCase {

	require.NoError(t, scanner.Err())
	line := scanner.Text()

	parts := strings.Split(line, ",")
	require.Equal(t, 3, len(parts), "required 3 columns at line %d", lineNo)

	cardinality, err := strconv.ParseFloat(parts[0], 64)
	require.NoError(t, err, "invalid cardinality at line %d", lineNo)

	toAdd, err := strconv.Atoi(parts[1])
	require.NoError(t, err, "invalid value at line %d", lineNo)

	return addTestCase{
		hll:         parseHll(t, parts[2], lineNo),
		toAdd:       uint64(toAdd),
		cardinality: uint64(math.Ceil(cardinality)),
	}
}

func parseUnionTestCase(t *testing.T, scanner *bufio.Scanner, lineNo int) unionTestCase {

	require.NoError(t, scanner.Err())
	line := scanner.Text()

	parts := strings.Split(line, ",")
	require.Equal(t, 4, len(parts), "required 4 columns at line %d", lineNo)

	cardinality, err := strconv.ParseFloat(parts[2], 64)
	require.NoError(t, err, "invalid cardinality at line %d", lineNo)

	return unionTestCase{
		hll:         parseHll(t, parts[3], lineNo),
		toUnion:     parseHll(t, parts[1], lineNo),
		cardinality: uint64(math.Ceil(cardinality)),
	}
}

func parseHll(t *testing.T, hexEncoded string, lineNo int) Hll {

	require.True(t, hexEncoded[0] == '\\' && hexEncoded[1] == 'x', "missing \\x at line %d", lineNo)
	bytes, err := hex.DecodeString(hexEncoded[2:])
	require.NoError(t, err, "invalid hex at line %d", lineNo)

	hll, err := FromBytes(bytes)
	require.NoError(t, err, "invalid Hll at line %d", lineNo)

	// this looks weird, but it is required so that the tests match up with the
	// outputs created by the Java library's test generator where this setting
	// gets hard-coded.  in normal use, the sparse threshold is auto-calculated.
	hll.settings.sparseThreshold = 850

	return hll
}
