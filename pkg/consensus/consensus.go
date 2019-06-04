// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package smartbft

import (
	"sync"

	bft "github.com/SmartBFT-Go/consensus/pkg/api"
)

// Consensus submits requests to be total ordered,
// and delivers to the application proposals by invoking Deliver() on it.
// The proposals contain batches of requests assembled together by the Assembler.
type Consensus struct {
	Application      bft.Application
	Comm             bft.Comm
	Assembler        bft.Assembler
	WAL1             bft.WriteAheadLog
	WAL2             bft.WriteAheadLog
	Signer           bft.Signer
	Verifier         bft.Verifier
	RequestInspector bft.RequestInspector
	Synchronizer     bft.Synchronizer
	Logger           bft.Logger
}

// Future waits until an event occurs
type Future interface {
	Wait()
}

func (c *Consensus) Start() Future {
	var wg sync.WaitGroup
	// TODO: start an actual node and run it...
	return &wg
}

// SubmitRequest submits a request to be total ordered into the consensus
func (c *Consensus) SubmitRequest(req []byte) {

}
