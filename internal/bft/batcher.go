// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"time"
)

type Bundler struct { // TODO change name
	Pool      RequestPool
	BatchSize int
	Timeout   time.Duration
	remainder [][]byte
}

func (b *Bundler) NextBatch() [][]byte {
	currBatch := make([][]byte, 0)
	remainderOccupied := len(b.remainder)
	if remainderOccupied > 0 {
		currBatch = b.remainder
	}
	b.remainder = make([][]byte, 0)
	timeout := time.After(b.Timeout)
	for {
		select {
		case <-timeout:
			return b.buildBatch(remainderOccupied, currBatch)
		default:
			if b.Pool.SizeOfPool() >= b.BatchSize-remainderOccupied {
				return b.buildBatch(remainderOccupied, currBatch)
			}
			time.Sleep(b.Timeout / 100)
		}
	}
}

func (b *Bundler) buildBatch(remainderOccupied int, currBatch [][]byte) [][]byte {
	reqs := b.Pool.NextRequests(b.BatchSize - remainderOccupied)
	reqsBytes := make([][]byte, 0)
	for i := 0; i < len(reqs); i++ {
		reqsBytes = append(reqsBytes, reqs[i].Request)
	}
	currBatch = append(currBatch, reqsBytes...)
	return currBatch
}

func (b *Bundler) BatchRemainder(remainder [][]byte) {
	if len(b.remainder) != 0 {
		panic("batch remainder should always be empty when setting remainder")
	}
	b.remainder = remainder
}
