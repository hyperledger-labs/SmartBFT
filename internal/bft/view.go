// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type Phase uint8

const (
	COMMITTED = iota
	PROPOSED
	PREPARED
	ABORT
)

//go:generate mockery -dir . -name State -case underscore -output ./mocks/

type State interface {
	// Save saves the current message.
	Save(message *protos.Message) error

	// Restore restores the given view to its latest state
	// before a crash, if applicable.
	Restore(*View) error
}

type View struct {
	// Configuration
	SelfID           uint64
	N                uint64
	LeaderID         uint64
	Quorum           int
	Number           uint64
	Decider          Decider
	FailureDetector  FailureDetector
	Sync             api.Synchronizer
	Logger           api.Logger
	Comm             api.Comm
	Verifier         api.Verifier
	Signer           api.Signer
	ProposalSequence uint64
	State            State
	Phase            Phase
	// Runtime
	incMsgs           chan *incMsg
	myProposalSig     *types.Signature
	inFlightProposal  *types.Proposal
	inFlightRequests  []types.RequestInfo
	lastBroadcastSent *protos.Message
	// Current sequence sent prepare and commit
	currPrepareSent *protos.Message
	currCommitSent  *protos.Message
	// Prev sequence sent prepare and commit
	// to help lagging replicas catch up
	prevPrepareSent *protos.Message
	prevCommitSent  *protos.Message
	// Current proposal
	prePrepare chan *protos.Message
	prepares   *voteSet
	commits    *voteSet
	// Next proposal
	nextPrePrepare chan *protos.Message
	nextPrepares   *voteSet
	nextCommits    *voteSet

	abortChan chan struct{}
}

func (v *View) Start() Future { //TODO the future should be return by Close() or implemented by View itself
	v.incMsgs = make(chan *incMsg, 10*v.N) // TODO channel size should be configured
	v.abortChan = make(chan struct{})

	var viewEnds sync.WaitGroup
	viewEnds.Add(1)

	v.prePrepare = make(chan *protos.Message, 1)
	v.nextPrePrepare = make(chan *protos.Message, 1)

	v.setupVotes()

	go func() {
		defer viewEnds.Done()
		v.run()
	}()

	return &viewEnds
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
		v.Logger.Warnf("%d got message %v from %d of view %d, expected view %d", v.SelfID, m, sender, msgViewNum, v.Number)
		// TODO  when do we send the error message?
		if sender != v.LeaderID {
			return
		}
		v.FailureDetector.Complain()
		// Else, we got a message with a wrong view from the leader.
		if msgViewNum > v.Number {
			v.Sync.Sync()
		}
		v.Abort()
		return
	}

	msgProposalSeq := proposalSequence(m)

	if msgProposalSeq == v.ProposalSequence-1 && v.ProposalSequence > 0 {
		v.handlePrevSeqMessage(msgProposalSeq, sender, m)
		return
	}

	v.Logger.Debugf("%d got message %v from %d with seq %d", v.SelfID, m, sender, msgProposalSeq)
	// This message is either for this proposal or the next one (we might be behind the rest)
	if msgProposalSeq != v.ProposalSequence && msgProposalSeq != v.ProposalSequence+1 {
		v.Logger.Warnf("%d got message from %d with sequence %d but our sequence is %d", v.SelfID, sender, msgProposalSeq, v.ProposalSequence)
		return
	}

	msgForNextProposal := msgProposalSeq == v.ProposalSequence+1

	if pp := m.GetPrePrepare(); pp != nil {
		if pp.Proposal == nil {
			v.Logger.Warnf("%d got pre-prepare from %d with empty proposal", v.SelfID, sender)
			return
		}
		if sender != v.LeaderID {
			v.Logger.Warnf("%d got pre-prepare from %d but the leader is %d", v.SelfID, sender, v.LeaderID)
			return
		}
		if msgForNextProposal {
			v.nextPrePrepare <- m
		} else {
			v.prePrepare <- m
		}
		return
	}

	// Else, it's a prepare or a commit.
	// Ignore votes from ourselves.
	if sender == v.SelfID {
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
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		default:
			v.doPhase()
		}
	}
}

func (v *View) doPhase() {
	switch v.Phase {
	case PROPOSED:
		v.Comm.BroadcastConsensus(v.lastBroadcastSent) // broadcast here serves also recovery
		v.Phase = v.processPrepares()
	case PREPARED:
		v.Comm.BroadcastConsensus(v.lastBroadcastSent)
		v.Phase = v.prepared()
	case COMMITTED:
		v.Phase = v.processProposal()
	case ABORT:
		return
	default:
		v.Logger.Panicf("Unknown phase in view : %v", v)
	}
}

func (v *View) prepared() Phase {
	proposal := v.inFlightProposal
	signatures, phase := v.processCommits(proposal)
	if phase == ABORT {
		return ABORT
	}

	seq := v.ProposalSequence

	v.Logger.Infof("%d processed commits for proposal with seq %d", v.SelfID, seq)

	v.decide(proposal, signatures, v.inFlightRequests)
	return COMMITTED
}

func (v *View) processProposal() Phase {
	v.prevPrepareSent = v.currPrepareSent
	v.prevCommitSent = v.currCommitSent
	v.currPrepareSent = nil
	v.currCommitSent = nil
	v.inFlightProposal = nil
	v.inFlightRequests = nil
	v.lastBroadcastSent = nil

	var proposal types.Proposal
	var receivedProposal *protos.Message

	var gotPrePrepare bool
	for !gotPrePrepare {
		select {
		case <-v.abortChan:
			return ABORT
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case msg := <-v.prePrepare:
			gotPrePrepare = true
			receivedProposal = msg
			prop := msg.GetPrePrepare().Proposal
			proposal = types.Proposal{
				VerificationSequence: int64(prop.VerificationSequence),
				Metadata:             prop.Metadata,
				Payload:              prop.Payload,
				Header:               prop.Header,
			}
		}
	}

	requests, err := v.verifyProposal(proposal)
	if err != nil {
		v.Logger.Warnf("%d received bad proposal from %d: %v", v.SelfID, v.LeaderID, err)
		v.FailureDetector.Complain()
		v.Sync.Sync()
		v.Abort()
		return ABORT
	}

	seq := v.ProposalSequence

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
	v.currPrepareSent = proto.Clone(msg).(*protos.Message)
	v.currPrepareSent.GetPrepare().Assist = true
	v.inFlightProposal = &proposal
	v.inFlightRequests = requests

	if v.SelfID == v.LeaderID {
		v.Comm.BroadcastConsensus(receivedProposal)
	}

	v.Logger.Infof("Processed proposal with seq %d", seq)
	return PROPOSED
}

func (v *View) processPrepares() Phase {
	proposal := v.inFlightProposal
	expectedDigest := proposal.Digest()
	collectedDigests := 0

	var voterIDs []uint64

	for collectedDigests < v.Quorum-1 {
		select {
		case <-v.abortChan:
			return ABORT
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case vote := <-v.prepares.votes:
			prepare := vote.GetPrepare()
			if prepare.Digest != expectedDigest {
				// TODO is it ok that the vote is already registered?
				seq := v.ProposalSequence
				v.Logger.Warnf("Got wrong digest at processPrepares for prepare with seq %d, expecting %v but got %v, we are in seq %d", prepare.Seq, expectedDigest, prepare.Digest, seq)
				continue
			}
			collectedDigests++
			voterIDs = append(voterIDs, vote.sender)
		}
	}

	v.Logger.Infof("%d collected %d prepares from %v", v.SelfID, collectedDigests, voterIDs)

	v.myProposalSig = v.Signer.SignProposal(*proposal)

	seq := v.ProposalSequence

	msg := &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   v.Number,
				Digest: expectedDigest,
				Seq:    seq,
				Signature: &protos.Signature{
					Signer: v.myProposalSig.Id,
					Value:  v.myProposalSig.Value,
					Msg:    v.myProposalSig.Msg, // some information about the proposal that was signed (not the entire proposal)
				},
			},
		},
	}

	// We received enough prepares to send a commit.
	// Save the commit message we are about to send.
	v.State.Save(msg)
	v.currCommitSent = proto.Clone(msg).(*protos.Message)
	v.currCommitSent.GetCommit().Assist = true
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

	var voterIDs []uint64

	for len(signatures) < v.Quorum-1 {
		select {
		case <-v.abortChan:
			return nil, ABORT
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case vote := <-v.commits.votes:
			// Valid votes end up written into the 'validVotes' channel.
			go func(vote *protos.Message) {
				signatureCollector.verifyVote(vote)
			}(vote.Message)
		case signature := <-signatureCollector.validVotes:
			signatures = append(signatures, signature)
			voterIDs = append(voterIDs, signature.Id)
		}
	}

	v.Logger.Infof("%d collected %d commits from %v", v.SelfID, len(signatures), voterIDs)

	return signatures, COMMITTED
}

func (v *View) verifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	// Verify proposal has correct structure and contains authorized requests.
	requests, err := v.Verifier.VerifyProposal(proposal)
	if err != nil {
		v.Logger.Warnf("Received bad proposal: %v", err)
		return nil, err
	}

	// Verify proposal's metadata is valid.
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, md); err != nil {
		return nil, err
	}

	if md.ViewId != v.Number {
		v.Logger.Warnf("Expected view number %d but got %d", v.Number, md.ViewId)
		return nil, errors.New("invalid view number")
	}

	if md.LatestSequence != v.ProposalSequence {
		v.Logger.Warnf("Expected proposal sequence %d but got %d", v.ProposalSequence, md.LatestSequence)
		return nil, errors.New("invalid proposal sequence")
	}

	expectedSeq := v.Verifier.VerificationSequence()
	if uint64(proposal.VerificationSequence) != expectedSeq {
		v.Logger.Warnf("Expected verification sequence %d but got %d", expectedSeq, proposal.VerificationSequence)
		return nil, errors.New("verification sequence mismatch")
	}

	return requests, nil
}

func (v *View) handlePrevSeqMessage(msgProposalSeq, sender uint64, m *protos.Message) {
	if m.GetPrePrepare() != nil {
		v.Logger.Warnf("Got pre-prepare for sequence %d but we're in sequence %d", msgProposalSeq, v.ProposalSequence)
		return
	}
	msgType := "prepare"
	if m.GetCommit() != nil {
		msgType = "commit"
	}

	var found bool

	switch msgType {
	case "prepare":
		// This is an assist message, we don't need to reply to it.
		if m.GetPrepare().Assist {
			return
		}
		if v.prevPrepareSent != nil {
			v.Comm.SendConsensus(sender, v.prevPrepareSent)
			found = true
		}
	case "commit":
		// This is an assist message, we don't need to reply to it.
		if m.GetCommit().Assist {
			return
		}
		if v.prevCommitSent != nil {
			v.Comm.SendConsensus(sender, v.prevCommitSent)
			found = true
		}
	}

	prevMsgFound := fmt.Sprintf("but didn't have a previous %s to send back.", msgType)
	if found {
		prevMsgFound = fmt.Sprintf("sent back previous %s.", msgType)
	}
	v.Logger.Debugf("Got %s for previous sequence (%d) from %d, %s", msgType, msgProposalSeq, sender, prevMsgFound)
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

func (v *View) decide(proposal *types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	// first make preparations for the next sequence so that the view will be ready to continue right after delivery
	v.startNextSeq()
	v.Logger.Infof("Deciding on seq %d", v.ProposalSequence)
	signatures = append(signatures, *v.myProposalSig)
	v.Decider.Decide(*proposal, signatures, requests)
}

func (v *View) startNextSeq() {
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
	propSeq := v.ProposalSequence
	md := &protos.ViewMetadata{
		ViewId:         v.Number,
		LatestSequence: propSeq,
	}
	metadata, err := proto.Marshal(md)
	if err != nil {
		v.Logger.Panicf("Failed marshaling metadata: %v", err)
	}
	return metadata
}

// Propose broadcasts a prePrepare message with the given proposal
func (v *View) Propose(proposal types.Proposal) {
	seq := v.ProposalSequence
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

type vote struct {
	*protos.Message
	sender uint64
}

type voteSet struct {
	validVote func(voter uint64, message *protos.Message) bool
	voted     map[uint64]struct{}
	votes     chan *vote
}

func (vs *voteSet) clear(n uint64) {
	// Drain the votes channel
	for len(vs.votes) > 0 {
		<-vs.votes
	}

	vs.voted = make(map[uint64]struct{}, n)
	vs.votes = make(chan *vote, n)
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
	vs.votes <- &vote{Message: message, sender: voter}
}

type incMsg struct {
	*protos.Message
	sender uint64
}
