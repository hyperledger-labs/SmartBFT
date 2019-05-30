// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync/atomic"

	"github.com/SmartBFT-Go/consensus/pkg/bft"
	"github.com/SmartBFT-Go/consensus/protos"
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

type Comm interface {
	Broadcast(m *protos.Message)
}

type View struct {
	// Configuration
	N                int
	F                int
	LeaderID         uint64
	Number           uint64
	Decider          Decider
	FailureDetector  FailureDetector
	Sync             Synchronizer
	Logger           bft.Logger
	Comm             Comm
	Verifier         bft.Verifier
	ProposalSequence *uint64 // should be accessed atomically
	PrevHeader       []byte
	// Runtime
	incMsgs   chan *incMsg
	proposals chan bft.Proposal // size of 1
	// Current proposal
	prepares *voteSet
	commits  *voteSet
	// Next proposal
	nextPrepares *voteSet
	nextCommits  *voteSet

	abortChan chan struct{}
}

func (v *View) ProcessMessages() {
	v.setupVotes()

	for {
		select {
		case <-v.abortChan:
			return
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		}
	}
}

func (v *View) setupVotes() {
	// Prepares
	acceptPrepares := func(_ uint64, message *protos.Message) bool {
		return message.GetPrepare() != nil
	}

	v.prepares = &voteSet{
		validVote: acceptPrepares,
	}
	v.prepares.clear()

	v.nextPrepares = &voteSet{
		validVote: acceptPrepares,
	}
	v.nextPrepares.clear()

	// Commits
	acceptCommits := func(sender uint64, message *protos.Message) bool {
		commit := message.GetCommit()
		if commit == nil {
			return false
		}
		if commit.Signature == nil {
			return false
		}
		// Sender needs to match the inner signature sender
		return commit.Signature.Signer == sender
	}

	v.commits = &voteSet{
		validVote: acceptCommits,
	}
	v.commits.clear()

	v.nextCommits = &voteSet{
		validVote: acceptCommits,
	}
	v.nextCommits.clear()
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
	currentProposalSeq := atomic.LoadUint64(v.ProposalSequence)
	msgProposalSeq := bft.ProposalSequence(m)

	// This message is either for this proposal or the next one (we might be behind the rest)
	if msgProposalSeq != currentProposalSeq && msgProposalSeq != currentProposalSeq+1 {
		v.Logger.Warningf("Got message from %d with sequence %d but our sequence is %d", sender, msgProposalSeq, currentProposalSeq)
		return
	}

	msgForNextProposal := msgProposalSeq == currentProposalSeq+1

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
		if msgForNextProposal {
			v.nextPrepares.registerVote(sender, m)
		} else {
			v.prepares.registerVote(sender, m)
		}
		return
	}

	if cmt := m.GetCommit(); cmt != nil {
		if msgForNextProposal {
			v.nextCommits.registerVote(sender, m)
		} else {
			v.nextCommits.registerVote(sender, m)
		}
		return
	}
}

func (v *View) handlePrePrepare(sender uint64, pp *protos.PrePrepare) {
	if pp.Proposal == nil {
		v.Logger.Warningf("Got pre-prepare with empty proposal")
		return
	}
	if sender != v.LeaderID {
		v.Logger.Warningf("Got pre-prepare from %d but the leader is %d", sender, v.LeaderID)
		return
	}

	prop := pp.Proposal
	proposal := bft.Proposal{
		VerificationSequence: prop.VerificationSequence,
		Metadata:             prop.Metadata,
		Payload:              prop.Payload,
		Header:               prop.Header,
	}
	select {
	case v.proposals <- proposal:
	default:
		// A proposal is currently being handled.
		// Log a warning because we shouldn't get 2 proposals from the leader within such a short time.
		// We can have an outstanding proposal in the channel, but not more than 1.
		currentProposalSeq := atomic.LoadUint64(v.ProposalSequence)
		v.Logger.Warningf("Got proposal %d but currently still not processed proposal %d", pp.Seq, currentProposalSeq)
	}
}

func (v *View) Run() {
	for {
		select {
		case <-v.abortChan:
			return
		default:
			v.doStep()
		}
	}
}

func (v *View) doStep() {
	proposalSequence := atomic.LoadUint64(v.ProposalSequence)
	proposal := v.processPrePrepare(proposalSequence)
	if proposal == nil {
		// Aborted view
		return
	}

	v.processPrepares(proposal)

	signatures := v.processCommits(proposal)
	if len(signatures) == 0 {
		return
	}

	v.maybeDecide(proposal, signatures)
}

func (v *View) processPrePrepare(proposalSequence uint64) *bft.Proposal {
	proposal := <-v.proposals
	err := v.Verifier.VerifyProposal(proposal, v.PrevHeader)
	if err != nil {
		v.Logger.Warningf("Received bad proposal from %d: %v", v.LeaderID, err)
		v.FailureDetector.Complain()
		v.Sync.SyncIfNeeded()
		v.Abort()
		return nil
	}

	msg := &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				Seq:    proposalSequence,
				View:   v.Number,
				Digest: proposal.Digest(),
			},
		},
	}

	v.Comm.Broadcast(msg)
	return &proposal
}

func (v *View) processPrepares(proposal *bft.Proposal) {
	expectedDigest := proposal.Digest()
	collectedDigests := 0

	for collectedDigests < 2*v.F {
		select {
		case <-v.abortChan:
			return
		case vote := <-v.prepares.votes:
			prepare := vote.GetPrepare()
			if prepare.Digest != expectedDigest {
				v.Logger.Warningf("Got digest %s but expected %s", prepare.Digest, expectedDigest)
				continue
			}
			collectedDigests++
		}
	}
}

func (v *View) processCommits(proposal *bft.Proposal) []bft.Signature {
	expectedDigest := proposal.Digest()
	signatures := make(map[uint64]bft.Signature)

	for len(signatures) < 2*v.F {
		select {
		case <-v.abortChan:
			return nil
		case vote := <-v.commits.votes:
			commit := vote.GetCommit()
			if commit.Digest != expectedDigest {
				v.Logger.Warningf("Got digest %s but expected %s", commit.Digest, expectedDigest)
				continue
			}

			err := v.Verifier.VerifyConsenterSig(commit.Signature.Signer, commit.Signature.Value, proposal.Metadata)
			if err != nil {
				v.Logger.Warningf("Couldn't verify %d's signature: %v", commit.Signature.Signer, err)
				continue
			}

			signatures[commit.Signature.Signer] = bft.Signature{
				Id:    commit.Signature.Signer,
				Value: commit.Signature.Value,
			}
		}
	}

	var res []bft.Signature
	for _, sig := range signatures {
		res = append(res, sig)
	}
	return res
}

func (v *View) maybeDecide(proposal *bft.Proposal, signatures []bft.Signature) {
	v.Decider.Decide(*proposal, signatures)
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

type voteSet struct {
	validVote func(voter uint64, message *protos.Message) bool
	voted     map[uint64]struct{}
	votes     chan *protos.Message
}

func (vs *voteSet) clear() {
	// Drain the votes channel
	for len(vs.votes) > 0 {
		<-vs.votes
	}

	vs.voted = make(map[uint64]struct{})
}

func (vs *voteSet) registerVote(voter uint64, message *protos.Message) {
	if !vs.validVote(voter, message) {
		return
	}

	_, hasVoted := vs.voted[voter]
	if hasVoted {
		// Received double vote
		return
	}

	vs.voted[voter] = struct{}{}
	vs.votes <- message
}

type incMsg struct {
	*protos.Message
	sender uint64
}
