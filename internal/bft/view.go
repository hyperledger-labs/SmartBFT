// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

type Phase uint8

const (
	COMMITTED = iota
	PROPOSED
	PREPARED
	ABORT
)

type State interface {
	// Save saves the current message.
	Save(message *protos.Message) error

	// Restore restores the given view to its latest state
	// before a crash, if applicable.
	Restore(*View) error
}

type View struct {
	// Configuration
	ID               uint64
	N                uint64
	LeaderID         uint64
	Quorum           int
	Number           uint64
	Decider          Decider
	FailureDetector  FailureDetector
	Sync             Synchronizer
	Logger           Logger
	Comm             Comm
	Verifier         Verifier
	Signer           Signer
	ProposalSequence uint64
	PrevHeader       []byte
	State            State
	Phase            Phase
	// Runtime
	incMsgs           chan *incMsg
	myProposalSig     *types.Signature
	inFlightProposal  *types.Proposal
	inFlightRequests  []types.RequestInfo
	lastBroadcastSent *protos.Message
	// Current proposal
	prePrepare chan *protos.Message
	prepares   *voteSet
	commits    *voteSet
	// Next proposal
	nextPrePrepare chan *protos.Message
	nextPrepares   *voteSet
	nextCommits    *voteSet

	abortChan chan struct{}

	lock sync.RWMutex // a lock on the sequence and all votes
}

func (v *View) Start() Future {
	v.incMsgs = make(chan *incMsg, 10*v.N) // TODO channel size should be configured
	v.abortChan = make(chan struct{})

	var viewEnds sync.WaitGroup
	viewEnds.Add(2)

	v.prePrepare = make(chan *protos.Message, 1)
	v.nextPrePrepare = make(chan *protos.Message, 1)

	v.setupVotes()

	go func() {
		defer viewEnds.Done()
		v.processMessages()
	}()
	go func() {
		defer viewEnds.Done()
		v.run()
	}()

	return &viewEnds
}

func (v *View) processMessages() {
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
	v.prepares.clear(v.N)

	v.nextPrepares = &voteSet{
		validVote: acceptPrepares,
	}
	v.nextPrepares.clear(v.N)

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
	v.commits.clear(v.N)

	v.nextCommits = &voteSet{
		validVote: acceptCommits,
	}
	v.nextCommits.clear(v.N)
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
	msgViewNum := viewNumber(m)
	if msgViewNum != v.Number {
		v.Logger.Warnf("Got message %v from %d of view %d, expected view %d", m, sender, msgViewNum, v.Number)
		// TODO  when do we send the error message?
		if sender != v.LeaderID {
			return
		}
		// Else, we got a message with a wrong view from the leader.
		if msgViewNum > v.Number {
			v.Sync.Sync()
		}
		v.FailureDetector.Complain()
		v.Abort()
		return
	}

	// TODO: what if proposal sequence is wrong?
	// we should handle it...
	// but if we got a prepare or commit for sequence i,
	// when we're in sequence i+1 then we should still send a prepare/commit again.
	// leaving this as a task for now.

	v.lock.RLock()
	defer v.lock.RUnlock()

	msgProposalSeq := proposalSequence(m)
	v.Logger.Debugf("Got message %v from %d with seq %d", m, sender, msgProposalSeq)
	// This message is either for this proposal or the next one (we might be behind the rest)
	if msgProposalSeq != v.ProposalSequence && msgProposalSeq != v.ProposalSequence+1 {
		v.Logger.Warnf("Got message from %d with sequence %d but our sequence is %d", sender, msgProposalSeq, v.ProposalSequence)
		return
	}

	msgForNextProposal := msgProposalSeq == v.ProposalSequence+1

	if pp := m.GetPrePrepare(); pp != nil {
		if pp.Proposal == nil {
			v.Logger.Warnf("Got pre-prepare with empty proposal")
			return
		}
		if sender != v.LeaderID {
			v.Logger.Warnf("Got pre-prepare from %d but the leader is %d", sender, v.LeaderID)
			return
		}
		if msgForNextProposal {
			v.nextPrePrepare <- m
		} else {
			v.prePrepare <- m
		}
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
			v.commits.registerVote(sender, m)
		}
		return
	}
}

func (v *View) run() {
	for {
		select {
		case <-v.abortChan:
			return
		default:
			v.doPhase()
		}
	}
}

func (v *View) doPhase() {
	switch v.Phase {
	case PROPOSED:
		v.Comm.BroadcastConsensus(v.lastBroadcastSent)
		v.Phase = v.processPrepares()
	case PREPARED:
		v.Comm.BroadcastConsensus(v.lastBroadcastSent)
		v.Phase = v.prepared()
	default:
		v.Phase = v.processProposal()
	}
}

func (v *View) prepared() Phase {
	proposal := v.inFlightProposal
	signatures, phase := v.processCommits(proposal)
	if phase == ABORT {
		return ABORT
	}

	v.lock.RLock()
	seq := v.ProposalSequence
	v.lock.RUnlock()

	v.Logger.Infof("Processed commits for proposal with seq %d", seq)

	v.maybeDecide(proposal, signatures, v.inFlightRequests)
	return COMMITTED
}

func (v *View) processProposal() Phase {
	v.inFlightProposal = nil
	v.inFlightRequests = nil
	v.lastBroadcastSent = nil

	var proposal types.Proposal
	var receivedProposal *protos.Message
	select {
	case <-v.abortChan:
		return ABORT
	case msg := <-v.prePrepare:
		receivedProposal = msg
		prop := msg.GetPrePrepare().Proposal
		proposal = types.Proposal{
			VerificationSequence: int64(prop.VerificationSequence),
			Metadata:             prop.Metadata,
			Payload:              prop.Payload,
			Header:               prop.Header,
		}
	}
	// TODO think if there is any other validation the node should run on a proposal
	requests, err := v.Verifier.VerifyProposal(proposal, v.PrevHeader)
	if err != nil {
		v.Logger.Warnf("Received bad proposal from %d: %v", v.LeaderID, err)
		v.FailureDetector.Complain()
		v.Sync.Sync()
		v.Abort()
		return ABORT
	}

	v.lock.RLock()
	seq := v.ProposalSequence
	v.lock.RUnlock()

	msg := &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				Seq:    seq,
				View:   v.Number,
				Digest: proposal.Digest(),
			},
		},
	}

	// We are about to send a prepare for a pre-prepare,
	// so we record the pre-prepare.
	v.State.Save(receivedProposal)
	v.lastBroadcastSent = msg
	v.inFlightProposal = &proposal
	v.inFlightRequests = requests

	if v.ID == v.LeaderID {
		v.Comm.BroadcastConsensus(receivedProposal)
	}

	v.Logger.Infof("Processed proposal with seq %d", seq)
	return PROPOSED
}

func (v *View) processPrepares() Phase {
	proposal := v.inFlightProposal
	expectedDigest := proposal.Digest()
	collectedDigests := 0

	for collectedDigests < v.Quorum-1 {
		select {
		case <-v.abortChan:
			return ABORT
		case vote := <-v.prepares.votes:
			prepare := vote.GetPrepare()
			if prepare.Digest != expectedDigest {
				// TODO is it ok that the vote is already registered?
				v.lock.RLock()
				seq := v.ProposalSequence
				v.lock.RUnlock()
				v.Logger.Warnf("Got wrong digest at processPrepares for prepare with seq %d, expecting %v but got %v, we are in seq %d", prepare.Seq, expectedDigest, prepare.Digest, seq)
				continue
			}
			collectedDigests++
		}
	}

	v.myProposalSig = v.Signer.SignProposal(*proposal)

	v.lock.RLock()
	seq := v.ProposalSequence
	v.lock.RUnlock()

	msg := &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   v.Number,
				Digest: expectedDigest,
				Seq:    seq,
				Signature: &protos.Signature{
					Signer: v.myProposalSig.Id,
					Value:  v.myProposalSig.Value,
					Msg:    v.myProposalSig.Msg,
				},
			},
		},
	}

	// We received enough prepares to send a commit.
	// Save the commit message we are about to send.
	v.State.Save(msg)
	v.lastBroadcastSent = msg

	v.Logger.Infof("Processed prepares for proposal with seq %d", seq)
	return PREPARED
}

func (v *View) processCommits(proposal *types.Proposal) ([]types.Signature, Phase) {
	var signatures []types.Signature

	signatureCollector := &voteVerifier{
		validVotes:     make(chan types.Signature, cap(v.commits.votes)),
		expectedDigest: proposal.Digest(),
		proposal:       proposal,
		v:              v,
	}

	for len(signatures) < v.Quorum-1 {
		select {
		case <-v.abortChan:
			return nil, ABORT
		case vote := <-v.commits.votes:
			// Valid votes end up written into the 'validVotes' channel.
			go func(vote *protos.Message) {
				signatureCollector.verifyVote(vote)
			}(vote)
		case signature := <-signatureCollector.validVotes:
			signatures = append(signatures, signature)
		}
	}

	return signatures, COMMITTED
}

type voteVerifier struct {
	v              *View
	proposal       *types.Proposal
	expectedDigest string
	validVotes     chan types.Signature
}

func (vv *voteVerifier) verifyVote(vote *protos.Message) {
	commit := vote.GetCommit()
	if commit.Digest != vv.expectedDigest {
		vv.v.Logger.Warnf("Got wrong digest at processCommits for seq %d", commit.Seq)
		return
	}

	err := vv.v.Verifier.VerifyConsenterSig(types.Signature{
		Id:    commit.Signature.Signer,
		Value: commit.Signature.Value,
		Msg:   commit.Signature.Msg,
	}, *vv.proposal)
	if err != nil {
		vv.v.Logger.Warnf("Couldn't verify %d's signature: %v", commit.Signature.Signer, err)
		return
	}

	vv.validVotes <- types.Signature{
		Id:    commit.Signature.Signer,
		Value: commit.Signature.Value,
		Msg:   commit.Signature.Msg,
	}
}

func (v *View) maybeDecide(proposal *types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	v.lock.RLock()
	seq := v.ProposalSequence
	v.lock.RUnlock()
	v.Logger.Infof("Deciding on seq %d", seq)
	v.startNextSeq()
	signatures = append(signatures, *v.myProposalSig)
	v.Decider.Decide(*proposal, signatures, requests)
}

func (v *View) startNextSeq() {
	v.lock.Lock()
	defer v.lock.Unlock()

	prevSeq := v.ProposalSequence

	v.ProposalSequence++

	nextSeq := v.ProposalSequence

	v.Logger.Infof("Sequence: %d-->%d", prevSeq, nextSeq)

	// swap next prePrepare
	tmp := v.prePrepare
	v.prePrepare = v.nextPrePrepare
	// clear tmp
	for len(tmp) > 0 {
		<-tmp
	}
	tmp = make(chan *protos.Message, 1)
	v.nextPrePrepare = tmp

	// swap next prepares
	tmpVotes := v.prepares
	v.prepares = v.nextPrepares
	tmpVotes.clear(v.N)
	v.nextPrepares = tmpVotes

	// swap next commits
	tmpVotes = v.commits
	v.commits = v.nextCommits
	tmpVotes.clear(v.N)
	v.nextCommits = tmpVotes
}

func (v *View) GetMetadata() []byte {
	v.lock.RLock()
	propSeq := v.ProposalSequence
	v.lock.RUnlock()
	md := &protos.ViewMetadata{
		ViewId:         v.Number,
		LatestSequence: propSeq,
	}
	metadata, err := proto.Marshal(md)
	if err != nil {
		v.Logger.Panicf("Faild marshaling metadata: %v")
	}
	return metadata
}

// Propose broadcasts a prePrepare message with the given proposal
func (v *View) Propose(proposal types.Proposal) {
	v.lock.RLock()
	seq := v.ProposalSequence
	v.lock.RUnlock()
	msg := &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: v.Number,
				Seq:  seq,
				Proposal: &protos.Proposal{
					Header:               proposal.Header,
					Payload:              proposal.Payload,
					Metadata:             proposal.Metadata,
					VerificationSequence: uint64(proposal.VerificationSequence),
				},
			},
		},
	}
	// Send the proposal to yourself in order to pre-prepare yourself and record
	// it in the WAL before sending it to other nodes.
	v.HandleMessage(v.LeaderID, msg)
	v.Logger.Debugf("Proposing proposal sequence %d", seq)
}

// Abort forces the view to end
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

func (vs *voteSet) clear(n uint64) {
	// Drain the votes channel
	for len(vs.votes) > 0 {
		<-vs.votes
	}

	vs.voted = make(map[uint64]struct{}, n)
	vs.votes = make(chan *protos.Message, n)
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
