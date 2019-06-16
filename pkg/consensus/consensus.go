// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"sync/atomic"

	algorithm "github.com/SmartBFT-Go/consensus/internal/bft"
	bft "github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// Consensus submits requests to be total ordered,
// and delivers to the application proposals by invoking Deliver() on it.
// The proposals contain batches of requests assembled together by the Assembler.
type Consensus struct {
	SelfID           int
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
	View             *algorithm.View
	nextSeq          uint64
}

func (c *Consensus) Complain() {
	panic("implement me")
}

func (c *Consensus) SyncIfNeeded() {
	c.Synchronizer.Sync()
}

func (c *Consensus) Decide(proposal types.Proposal, signatures []types.Signature) {
	atomic.AddUint64(&c.nextSeq, 1)
	c.Application.Deliver(proposal, signatures)
}

// Future waits until an event occurs
type Future interface {
	Wait()
}

func (c *Consensus) Start() Future {
	c.View = &algorithm.View{
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
	future := c.View.Start()
	return future
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	if algorithm.IsViewMessage(m) {
		c.View.HandleMessage(sender, m)
	}

}

// SubmitRequest submits a request to be total ordered into the consensus
func (c *Consensus) SubmitRequest(req []byte) {
	if !c.amLeader() {
		return
	}

	proposal, _ := c.Assembler.AssembleProposal(nil, [][]byte{req})
	msg := &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				Seq: atomic.LoadUint64(&c.nextSeq),
				Proposal: &protos.Proposal{
					Payload:  proposal.Payload,
					Header:   proposal.Header,
					Metadata: proposal.Metadata,
				},
			},
		},
	}

	c.Comm.Broadcast(msg)
	// Send the message to yourself
	c.View.HandleMessage(uint64(c.SelfID), msg)
}

func (c *Consensus) amLeader() bool {
	return c.SelfID == 0
}
