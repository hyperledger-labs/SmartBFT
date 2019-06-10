// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"sync"

	algorithm "github.com/SmartBFT-Go/consensus/internal/bft"
	bft "github.com/SmartBFT-Go/consensus/pkg/api"
	types "github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/protos"
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
	view             *algorithm.View
}

func (c *Consensus) Complain() {
	panic("implement me")
}

func (c *Consensus) SyncIfNeeded() {
	c.Synchronizer.Sync()
}

func (c *Consensus) Decide(proposal types.Proposal, signatures []types.Signature) {
	c.Application.Deliver(proposal, signatures)
}

// Future waits until an event occurs
type Future interface {
	Wait()
}

func (c *Consensus) Start() Future {
	c.view = &algorithm.View{
		Verifier:        c.Verifier,
		Signer:          c.Signer,
		Comm:            c.Comm,
		Logger:          c.Logger,
		Decider:         c,
		Number:          0,
		Sync:            c,
		FailureDetector: c,
		LeaderID:        0,
		PrevHeader:      nil,
		N:               4,
	}
	var wg sync.WaitGroup
	// TODO: start an actual node and run it...
	return &wg
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	c.view.HandleMessage(sender, m)
}

// SubmitRequest submits a request to be total ordered into the consensus
func (c *Consensus) SubmitRequest(req []byte) {

}
