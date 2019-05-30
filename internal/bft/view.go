// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"github.com/SmartBFT-Go/consensus/pkg/bft"
	"github.com/SmartBFT-Go/consensus/protos"
	"sync/atomic"
)

type Decider interface {
	Decide(proposal bft.Proposal, signatures []bft.Signature)
}

type FailureDetector interface {
	Complain()
}

type Synchronizer interface {
	SyncIfNeeded()
}

type View struct {
	// Configuration
	ClusterSize      uint64
	LeaderID         uint64
	Number           uint64
	Decider          Decider
	FailureDetector  FailureDetector
	Sync             Synchronizer
	Logger           bft.Logger
	ProposalSequence *uint64 // should be accessed atomically
	// Runtime
	incMsgs   chan *incMsg
	proposals chan *protos.Proposal // size of 1
	abortChan chan struct{}
}

type incMsg struct {
	*protos.Message
	sender uint64
}

func (v *View) ProcessMessages() {
	for {
		select {
		case <-v.abortChan:
			return
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		}
	}
}

func (v *View) HandleMessage(sender uint64, m *protos.Message) {
	msg := &incMsg{sender: sender, Message: m}
	select {
	case <-v.abortChan:
		return
	case v.incMsgs <- msg:
	}
}

func (v *View) processMsg(sender uint64, m *protos.Message) {
	// Ensure view number is equal to our view
	msgViewNum := bft.ViewNumber(m)
	//propSeq    := bft.ProposalSequence(m)
	// TODO: what if proposal sequence is wrong?
	// we should handle it...
	// but if we got a prepare or commit for sequence i,
	// when we're in sequence i+1 then we should still send a prepare/commit again.
	// leaving this as a TODO for now.

	if msgViewNum != v.Number {
		v.Logger.Warningf("Got message %v from %d of view %d, expected view %d", m, sender, msgViewNum, v.Number)
		if sender != v.LeaderID {
			return
		}
		// Else, we got a message with a wrong view from the leader.
		// TODO: invoke Sync()
		v.FailureDetector.Complain()
	}

	if pp := m.GetPrePrepare(); pp != nil {
		v.handlePrePrepare(sender, pp)
		return
	}

	if prp := m.GetPrepare(); prp != nil {
		v.handlePrepare(sender, prp)
		return
	}

	if cmt := m.GetCommit(); cmt != nil {
		v.handleCommit(sender, cmt)
		return
	}
}

func (v *View) handlePrePrepare(sender uint64, pp *protos.PrePrepare) {
	if sender != v.LeaderID {
		v.Logger.Warningf("Got pre-prepare from %d but the leader is %d", sender, v.LeaderID)
		return
	}

	select {
	case v.proposals <- pp.Proposal:
		// Put proposal into the proposal channel
	default:
		// A proposal is currently being handled.
		// Log a warning because we shouldn't get 2 proposals from the leader within such a short time.
		currentProposalSeq := atomic.LoadUint64(v.ProposalSequence)
		v.Logger.Warningf("Got proposal %d but currently handling proposal %d", pp.Seq, currentProposalSeq)
	}
}

func (v *View) handlePrepare(sender uint64, prp *protos.Prepare) {

}

func (v *View) handleCommit(sender uint64, cmt *protos.Commit) {

}

func (v *View) Propose() {

}

func (v *View) Abort() {
	select {
	case <-v.abortChan:
		// already aborted
		return
	default:
		close(v.abortChan)
	}
}
