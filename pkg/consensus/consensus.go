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
	SelfID           uint64
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
	controller       *algorithm.Controller
	nextSeq          uint64
	Requests         [][]byte
}

func (c *Consensus) Complain() {
	panic("implement me")
}

func (c *Consensus) Sync() (protos.ViewMetadata, uint64) {
	panic("implement me")
}

func (c *Consensus) BatchRemainder(remainder [][]byte) {
	panic("implement me")
}

func (c *Consensus) NextBatch() [][]byte {
	reqNum := atomic.LoadUint64(&c.nextSeq)
	if len(c.Requests) <= int(reqNum) {
		return nil
	}
	return [][]byte{c.Requests[reqNum]}
}

func (c *Consensus) Deliver(proposal types.Proposal, signatures []types.Signature) {
	atomic.AddUint64(&c.nextSeq, 1)
	c.Application.Deliver(proposal, signatures)
}

// Future waits until an event occurs
type Future interface {
	Wait()
}

func (c *Consensus) Start() Future {
	c.controller = &algorithm.Controller{
		ID:              c.SelfID,
		N:               4,
		Batcher:         c,
		Verifier:        c.Verifier,
		Logger:          c.Logger,
		Assembler:       c.Assembler,
		Application:     c,
		FailureDetector: c,
		Synchronizer:    c,
		Comm:            c.Comm,
		Signer:          c.Signer,
	}
	future := c.controller.Start(0, 0)
	return future
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	if algorithm.IsViewMessage(m) {
		c.controller.ProcessMessages(sender, m)
	}

}

func (c *Consensus) amLeader() bool {
	return c.SelfID == 0
}
