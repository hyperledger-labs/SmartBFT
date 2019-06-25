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
	requetsOccupied := 0
	for {
		select {
		case <-timeout:
			return currBatch
		default:
			reqs := b.Pool.NextRequests(b.BatchSize - remainderOccupied)
			reqsBytes := make([][]byte, 0)
			for i := requetsOccupied; i < len(reqs); i++ {
				reqsBytes = append(reqsBytes, reqs[i].Request)
			}
			requetsOccupied = len(reqs)
			currBatch = append(currBatch, reqsBytes...)
			if len(currBatch) == b.BatchSize {
				return currBatch
			}
		}
	}
}

func (b *Bundler) BatchRemainder(remainder [][]byte) {
	if len(b.remainder) != 0 {
		panic("batch remainder should always be empty when setting remainder")
	}
	b.remainder = remainder
}
