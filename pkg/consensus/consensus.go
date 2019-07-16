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
	SelfID           uint64
	N                uint64
	BatchSize        int
	BatchTimeout     time.Duration
	Application      bft.Application
	Comm             bft.Comm
	Assembler        bft.Assembler
	WAL              bft.WriteAheadLog
	Signer           bft.Signer
	Verifier         bft.Verifier
	RequestInspector bft.RequestInspector
	Synchronizer     bft.Synchronizer
	Logger           bft.Logger
	controller       *algorithm.Controller
	nextSeq          uint64
}

func (c *Consensus) Complain() {
	panic("implement me")
}

func (c *Consensus) Sync() (protos.ViewMetadata, uint64) {
	panic("implement me")
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
	pool := algorithm.NewPool(c.Logger, c.RequestInspector, 200)

	batcher := &algorithm.Bundler{
		Pool:         pool,
		BatchSize:    c.BatchSize,
		BatchTimeout: c.BatchTimeout,
	}

	c.controller = &algorithm.Controller{
		WAL:              c.WAL,
		ID:               c.SelfID,
		N:                c.N,
		Batcher:          batcher,
		RequestPool:      pool,
		Verifier:         c.Verifier,
		Logger:           c.Logger,
		Assembler:        c.Assembler,
		Application:      c,
		FailureDetector:  c,
		Synchronizer:     c,
		Comm:             c.Comm,
		Signer:           c.Signer,
		RequestInspector: c.RequestInspector,
	}
	future := c.controller.Start(0, 0)
	return future
}

func (c *Consensus) HandleMessage(sender uint64, m *protos.Message) {
	if algorithm.IsViewMessage(m) {
		c.controller.ProcessMessages(sender, m)
	}

}

func (c *Consensus) SubmitRequest(req []byte) error {
	c.Logger.Debugf("Submit Request")
	return c.controller.SubmitRequest(req)
}
