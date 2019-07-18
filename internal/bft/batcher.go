// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"time"
)

type Bundler struct { // TODO change name
	Pool         RequestPool
	BatchSize    int
	BatchTimeout time.Duration
	remainder    [][]byte
}

// NextBatch returns the next batch of requests to be proposed
func (b *Bundler) NextBatch() [][]byte {
	currBatch := make([][]byte, 0)
	remainderOccupied := len(b.remainder)
	if remainderOccupied > 0 {
		currBatch = b.remainder
	}
	b.remainder = make([][]byte, 0)
	timeout := time.After(b.BatchTimeout)
	for {
		select {
		case <-timeout:
			return b.buildBatch(remainderOccupied, currBatch)
		default:
			if b.Pool.Size() >= b.BatchSize-remainderOccupied {
				return b.buildBatch(remainderOccupied, currBatch)
			}
			time.Sleep(b.BatchTimeout / 100)
		}
	}
}

// takes the current batch and appends to it requests from the pool
func (b *Bundler) buildBatch(remainderOccupied int, currBatch [][]byte) [][]byte {
	reqs := b.Pool.NextRequests(b.BatchSize - remainderOccupied)
	for i := 0; i < len(reqs); i++ {
		currBatch = append(currBatch, reqs[i])
	}
	return currBatch
}

// BatchRemainder sets the remainder of requests to be included in the next batch
func (b *Bundler) BatchRemainder(remainder [][]byte) {
	if len(b.remainder) != 0 {
		panic("batch remainder should always be empty when setting remainder")
	}
	b.remainder = remainder
}
