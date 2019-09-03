// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package consensus

import (
	"sync/atomic"
	"time"

	algorithm "github.com/SmartBFT-Go/consensus/internal/bft"
	bft "github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// Consensus submits requests to be total ordered,
// and delivers to the application proposals by invoking Deliver() on it.
// The proposals contain batches of requests assembled together by the Assembler.
type Consensus struct {
	bft.Comm

	Config            Configuration
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
	LastProposal      types.Proposal
	LastSignatures    []types.Signature
	Scheduler         <-chan time.Time
	ViewChangerTicker <-chan time.Time

	viewChanger   *algorithm.ViewChanger
	controller    *algorithm.Controller
	state         *algorithm.PersistedState
	numberOfNodes uint64
}

func (c *Consensus) Complain(viewNum uint64, stopView bool) {
	c.viewChanger.StartViewChange(viewNum, stopView)
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) {
	c.Application.Deliver(proposal, signatures)
}

func (c *Consensus) Start() {
	c.numberOfNodes = uint64(len(c.Nodes()))
	inFlight := algorithm.InFlightData{}

	c.state = &algorithm.PersistedState{
		InFlightProposal: &inFlight,
		Entries:          c.WALInitialContent,
		Logger:           c.Logger,
		WAL:              c.WAL,
	}

	cpt := types.Checkpoint{}
	cpt.Set(c.LastProposal, c.LastSignatures)

	c.viewChanger = &algorithm.ViewChanger{
		SelfID:      c.Config.SelfID,
		N:           c.numberOfNodes,
		Logger:      c.Logger,
		Comm:        c,
		Signer:      c.Signer,
		Verifier:    c.Verifier,
		Application: c,
		Checkpoint:  &cpt,
		InFlight:    &inFlight,
		State:       c.state,
		// Controller later
		// RequestsTimer later
		Ticker:            c.ViewChangerTicker,
		ResendTimeout:     c.Config.ViewChangeResendInterval,
		TimeoutViewChange: c.Config.ViewChangeTimeout,
		State:             c.state,
		InMsqQSize:        c.Config.IncomingMessageBufferSize,
	}

	c.controller = &algorithm.Controller{
		Checkpoint:       &cpt,
		WAL:              c.WAL,
		ID:               c.Config.SelfID,
		N:                c.numberOfNodes,
		Verifier:         c.Verifier,
		Logger:           c.Logger,
		Assembler:        c.Assembler,
		Application:      c,
		FailureDetector:  c,
		Synchronizer:     c.Synchronizer,
		Comm:             c,
		Signer:           c.Signer,
		RequestInspector: c.RequestInspector,
		ViewChanger:      c.viewChanger,
		ViewSequences:    &atomic.Value{},
	}

	c.viewChanger.Synchronizer = c.controller

	c.controller.ProposerBuilder = c.proposalMaker()

	opts := algorithm.PoolOptions{
		QueueSize:         int64(c.Config.RequestPoolSize),
		RequestTimeout:    c.Config.RequestTimeout,
		LeaderFwdTimeout:  c.Config.RequestLeaderFwdTimeout,
		AutoRemoveTimeout: c.Config.RequestAutoRemoveTimeout,
	}
	pool := algorithm.NewPool(c.Logger, c.RequestInspector, c.controller, opts)
	batchBuilder := algorithm.NewBatchBuilder(pool, c.Config.RequestBatchMaxSize, c.Config.RequestBatchMaxInterval)
	leaderMonitor := algorithm.NewHeartbeatMonitor(
		c.Scheduler,
		c.Logger,
		c.Config.LeaderHeartbeatTimeout,
		c.Config.LeaderHeartbeatCount,
		c,
		c.controller,
		c.controller.ViewSequences)
	c.controller.RequestPool = pool
	c.controller.Batcher = batchBuilder
	c.controller.LeaderMonitor = leaderMonitor

	c.viewChanger.Controller = c.controller
	c.viewChanger.RequestsTimer = pool
	c.viewChanger.ViewSequences = c.controller.ViewSequences

	viewSeq, err := c.state.RestoreNewViewIfApplicable()
	if err != nil {
		c.Logger.Warnf("Failed restoring new view, error: %v", err)
	}
	if viewSeq == nil {
		c.Logger.Debugf("No new view to restore")
	} else {
		// Check if metadata should be taken from the restored new view or from the application
		if viewSeq.Seq >= c.Metadata.LatestSequence {
			c.Logger.Debugf("Restoring from new view with view %d and seq %d, while application has view %d and seq %d", viewSeq.View, viewSeq.Seq, c.Metadata.ViewId, c.Metadata.LatestSequence)
			c.viewChanger.Start(viewSeq.View)
			c.controller.Start(viewSeq.View, viewSeq.Seq+1)
			return
		}
	}
	// If we delivered to the application proposal with sequence i,
	// then we are expecting to be proposed a proposal with sequence i+1.
	c.viewChanger.Start(c.Metadata.ViewId)
	c.controller.Start(c.Metadata.ViewId, c.Metadata.LatestSequence+1)
}

func (c *Consensus) Stop() {
	c.viewChanger.Stop()
	c.controller.Stop()
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	c.controller.ProcessMessages(sender, m)
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
		if c.Config.SelfID == node {
			continue
		}
		c.Comm.SendConsensus(node, m)
	}
}

func (c *Consensus) proposalMaker() *algorithm.ProposalMaker {
	return &algorithm.ProposalMaker{
		State:           c.state,
		Comm:            c,
		Decider:         c.controller,
		Logger:          c.Logger,
		Signer:          c.Signer,
		SelfID:          c.Config.SelfID,
		Sync:            c.controller,
		FailureDetector: c,
		Verifier:        c.Verifier,
		N:               c.numberOfNodes,
		InMsqQSize:      c.Config.IncomingMessageBufferSize,
		ViewSequences:   c.controller.ViewSequences,
	}
}
