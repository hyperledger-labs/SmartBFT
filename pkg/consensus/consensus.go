// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"time"

	algorithm "github.com/SmartBFT-Go/consensus/internal/bft"
	bft "github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

const (
	DefaultRequestPoolSize = 200
)

// Consensus submits requests to be total ordered,
// and delivers to the application proposals by invoking Deliver() on it.
// The proposals contain batches of requests assembled together by the Assembler.
type Consensus struct {
	SelfID       uint64
	N            uint64
	BatchSize    int
	BatchTimeout time.Duration
	bft.Comm
	Application       bft.Application
	Assembler         bft.Assembler
	WAL               bft.WriteAheadLog
	WALInitialContent [][]byte
	Signer            bft.Signer
	Verifier          bft.Verifier
	RequestInspector  bft.RequestInspector
	Synchronizer      bft.Synchronizer
	Logger            bft.Logger
	Metadata          protos.ViewMetadata

	controller *algorithm.Controller
}

func (c *Consensus) Complain() {
	panic("implement me")
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) {
	c.Application.Deliver(proposal, signatures)
}

// Future waits until an event occurs
type Future interface {
	Wait()
}

func (c *Consensus) Start() Future {
	requestTimeout := 2 * c.BatchTimeout // Request timeout should be at least as batch timeout

	pool := algorithm.NewPool(c.Logger, c.RequestInspector, algorithm.PoolOptions{QueueSize: DefaultRequestPoolSize, RequestTimeout: requestTimeout})

	batcher := &algorithm.Bundler{
		CloseChan:    make(chan struct{}),
		Pool:         pool,
		BatchSize:    c.BatchSize,
		BatchTimeout: c.BatchTimeout,
	}

	c.controller = &algorithm.Controller{
		ProposerBuilder:  c,
		WAL:              c.WAL,
		ID:               c.SelfID,
		N:                c.N,
		Batcher:          batcher,
		RequestPool:      pool,
		RequestTimeout:   requestTimeout,
		Verifier:         c.Verifier,
		Logger:           c.Logger,
		Assembler:        c.Assembler,
		Application:      c,
		FailureDetector:  c,
		Synchronizer:     c.Synchronizer,
		Comm:             c,
		Signer:           c.Signer,
		RequestInspector: c.RequestInspector,
	}

	pool.SetTimeoutHandler(c.controller)

	future := c.controller.Start(c.Metadata.ViewId, c.Metadata.LatestSequence)
	return future
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	if algorithm.IsViewMessage(m) {
		c.controller.ProcessMessages(sender, m)
	}

}

func (c *Consensus) HandleRequest(sender uint64, req []byte) {
	// TODO: check if we're the leader at some layer, and also verify the message,
	// and discard it.
	c.controller.SubmitRequest(req)
}

func (c *Consensus) SubmitRequest(req []byte) error {
	c.Logger.Debugf("Submit Request: %s", c.RequestInspector.RequestID(req))
	return c.controller.SubmitRequest(req)
}

func (c *Consensus) BroadcastConsensus(m *protos.Message) {
	for _, node := range c.Comm.Nodes() {
		// Do not send to yourself
		if c.SelfID == node {
			continue
		}
		c.Comm.SendConsensus(node, m)
	}
}

func (c *Consensus) NewProposer(leader, proposalSequence, viewNum uint64, quorumSize int) algorithm.Proposer {
	persistedState := &algorithm.PersistedState{
		Entries: c.WALInitialContent,
		Logger:  c.Logger,
		WAL:     c.WAL,
	}

	view := &algorithm.View{
		N:                c.N,
		LeaderID:         leader,
		SelfID:           c.SelfID,
		Quorum:           quorumSize,
		Number:           viewNum,
		Decider:          c.controller,
		FailureDetector:  c.controller.FailureDetector,
		Sync:             c.Synchronizer,
		Logger:           c.Logger,
		Comm:             c,
		Verifier:         c.Verifier,
		Signer:           c.Signer,
		ProposalSequence: proposalSequence,
		State:            &algorithm.PersistedState{WAL: c.WAL},
	}

	persistedState.Restore(view)

	if proposalSequence > view.ProposalSequence {
		view.ProposalSequence = proposalSequence
	}

	if viewNum > view.Number {
		view.Number = viewNum
	}

	return view
}
