# go-hll  [![CircleCI](https://circleci.com/gh/segmentio/go-hll.svg?style=shield)](https://circleci.com/gh/segmentio/go-hll) [![Go Report Card](https://goreportcard.com/badge/github.com/segmentio/go-hll)](https://goreportcard.com/report/github.com/segmentio/go-hll) [![GoDoc](https://godoc.org/github.com/segmentio/go-hll?status.svg)](https://godoc.org/github.com/segmentio/go-hll)

A Go implementation of [HyperLogLog](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) that is
storage-compatible with the [Aggregate Knowledge HLL Storage Spec](https://github.com/aggregateknowledge/hll-storage-spec).

## Overview
HyperLogLog (HLL) is a fixed-size, set-like structure used for distinct value counting with tunable precision. For
example, in 1280 bytes HLL can estimate the count of tens of billions of distinct values with only a few percent error.

In addition to the algorithm proposed in the [original paper](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf),
this implementation is augmented to improve its accuracy and memory use without sacrificing much speed.

## Motivation
While there are a handful of existing HLL implementations in Go, none of them implement the AK Storage Spec.   
The unified storage format is useful for reading and writing HLLs in a multi-lingual environment.  At Segment, most of 
our runtime code is written in Go, but we frequently persist data to PostgreSQL for long-term storage.  The Postgres HLL 
plugin is fairly ubiquitous--it's available for the standalone server, AWS RDS, AWS Aurora, and CitusDB.

An excellent description for the motivation behind the storage strategy can be found in the [Java HLL 
library's README](https://github.com/aggregateknowledge/java-hll#motivation).

## Hashing
A good hashing algorithm is crucial to achieving the pseudorandomness that HLL requires in order to perform its 
calculations.  The 64 bit variant of [MurmurHash3](https://github.com/spaolacci/murmur3) is recommended.  If using a 
seed, it must be constant for all inputs to a given HLL.  Further, if that HLL is to be unioned, then the same seed must
be used for all inputs to the other HLL.

See the [Java HLL README](https://github.com/aggregateknowledge/java-hll#the-importance-of-hashing) for a discussion on 
why MurmurHash3 is a good choice.

## Adaptations to Go
The API is intended to be as similar as possible to [Java HLL](https://github.com/aggregateknowledge/java-hll) and
[Postgresql HLL](https://github.com/aggregateknowledge/postgresql-hll).  There are a couple of features, though,
that make it more friendly to Go Programmers.

### Zero Value
If default settings are specified using the `hll.Defaults` function, the zero value can be used directly as an empty HLL.

Since its impossible to reason about an HLL without the settings, operations on a zero value in lieu of default settings 
will panic.

### StrictUnion
The other HLL implementations allow for two HLLs to be union-ed even if their log2m or regwidth parameters differ.
However, doing so can produce wildly inaccurate results.  This library provides an additional `StrictUnion` operation 
that will return an error if attempting a union on HLLs with incompatible settings.

## Building
Dependencies are managed with [dep](https://github.com/golang/dep).  Before building, ensure that dep is 
[installed](https://github.com/golang/dep) and on the path.

### Download Dependencies
```make dep```

### Test
```make test```

## Usage
```go
package main 

import (
	"fmt"
	
	"github.com/segmentio/go-hll"
)

func main() {
	
	// install default settings.
	hll.Defaults(hll.Settings{
		Log2m: 10,
		Regwidth: 4,
		ExplicitThreshold: hll.AutoExplicitThreshold,
		SparseEnabled: true,
	})
	
	// add elements.
	h := hll.Hll{}
	h.AddRaw(123456789)
	fmt.Print(h.Cardinality())  // prints "1"
	
	// union Hlls
	h2 := hll.Hll{}
	h2.AddRaw(123456789)
	h2.AddRaw(987654321)
	h2.Union(h)
	fmt.Print(h2.Cardinality()) // prints "2"
 
	// write to/read from bytes. 
	h3, _ := hll.FromBytes(h2.ToBytes())
	fmt.Print(h3.Cardinality()) // prints "2"
}
```

There is a compatibility battery run as part of the unit tests.  The battery was produced by the 
[Test Generator](https://github.com/aggregateknowledge/java-hll/blob/master/src/test/java/net/agkn/hll/IntegrationTestGenerator.java)
in the java-hll package.

## Additional Resources
* [HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm](http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
* [Understanding the HyperLogLog](https://pdfs.semanticscholar.org/75ba/51ffd9d2bed8a65029c9340d058f587059da.pdf)

## License
Released under the [MIT license](License.md).
