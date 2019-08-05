// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"time"

	"sync"

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

	controller         *algorithm.Controller
	restoreOnceFromWAL sync.Once
}

func (c *Consensus) Complain() {
	c.Logger.Warnf("Something bad happened!")
}

func (c *Consensus) Sync() (protos.ViewMetadata, uint64) {
	return protos.ViewMetadata{}, 0
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) {
	c.Application.Deliver(proposal, signatures)
}

func (c *Consensus) Start() {
	requestTimeout := 2 * c.BatchTimeout // Request timeout should be at least as batch timeout
	opts := algorithm.PoolOptions{
		QueueSize:         DefaultRequestPoolSize,
		RequestTimeout:    requestTimeout,
		LeaderFwdTimeout:  requestTimeout,
		AutoRemoveTimeout: requestTimeout,
	}

	c.controller = &algorithm.Controller{
		ProposerBuilder:  c,
		WAL:              c.WAL,
		ID:               c.SelfID,
		N:                c.N,
		RequestTimeout:   requestTimeout,
		Verifier:         c.Verifier,
		Logger:           c.Logger,
		Assembler:        c.Assembler,
		Application:      c,
		FailureDetector:  c,
		Synchronizer:     c,
		Comm:             c,
		Signer:           c.Signer,
		RequestInspector: c.RequestInspector,
	}

	pool := algorithm.NewPool(c.Logger, c.RequestInspector, c.controller, opts)
	batchBuilder := algorithm.NewBatchBuilder(pool, c.BatchSize, c.BatchTimeout)
	c.controller.RequestPool = pool
	c.controller.Batcher = batchBuilder

	// If we delivered to the application proposal with sequence i,
	// then we are expecting to be proposed a proposal with sequence i+1.
	c.controller.Start(c.Metadata.ViewId, c.Metadata.LatestSequence+1)
}

func (c *Consensus) Stop() {
	c.controller.Stop()
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	if algorithm.IsViewMessage(m) {
		c.controller.ProcessMessages(sender, m)
	}

}

func (c *Consensus) HandleRequest(sender uint64, req []byte) {
	c.controller.HandleRequest(sender, req)
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
		State:            persistedState,
	}

	c.restoreOnceFromWAL.Do(func() {
		persistedState.Restore(view)
	})

	if proposalSequence > view.ProposalSequence {
		view.ProposalSequence = proposalSequence
	}

	if viewNum > view.Number {
		view.Number = viewNum
	}

	return view
}
