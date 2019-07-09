// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"fmt"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	proposal = types.Proposal{
		Header:               []byte{0},
		Payload:              []byte{1},
		Metadata:             []byte{2},
		VerificationSequence: 1,
	}

	digest = proposal.Digest()

	wrongProposal = types.Proposal{
		Header:               []byte{1},
		Payload:              []byte{2},
		Metadata:             []byte{3},
		VerificationSequence: 1,
	}

	wrongDigest = wrongProposal.Digest()

	prePrepare = &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: 1,
				Seq:  0,
				Proposal: &protos.Proposal{
					Header:               []byte{0},
					Payload:              []byte{1},
					Metadata:             []byte{2},
					VerificationSequence: 1,
				},
			},
		},
	}

	prepare = &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   1,
				Seq:    0,
				Digest: digest,
			},
		},
	}

	commit1 = &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   1,
				Seq:    0,
				Digest: digest,
				Signature: &protos.Signature{
					Signer: 1,
					Value:  []byte{4},
				},
			},
		},
	}

	commit2 = &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   1,
				Seq:    0,
				Digest: digest,
				Signature: &protos.Signature{
					Signer: 2,
					Value:  []byte{4},
				},
			},
		},
	}
)

func TestViewBasic(t *testing.T) {
	// A simple test that starts a view and aborts it

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	state := &bft.StateRecorder{}
	view := &bft.View{
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
	}
	end := view.Start()
	view.Abort()
	end.Wait()
}

func TestBadPrePrepare(t *testing.T) {
	// Ensure that a prePrepare with a wrong view number sent by the leader causes a view abort,
	// and that if the same message is from a follower then it is simply ignored.
	// Same goes to a proposal that doesn't pass the verifier.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	verifyLog := make(chan struct{})
	log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Received bad proposal from 1:") {
			verifyLog <- struct{}{}
		}
		return nil
	})).Sugar()
	synchronizer := &mocks.Synchronizer{}
	syncWG := &sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		syncWG.Done()
	}).Return(protos.ViewMetadata{}, uint64(0))
	fd := &mocks.FailureDetector{}
	fdWG := &sync.WaitGroup{}
	fd.On("Complain", mock.Anything).Run(func(args mock.Arguments) {
		fdWG.Done()
	})
	state := &bft.StateRecorder{}
	view := &bft.View{
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
		Sync:             synchronizer,
		FailureDetector:  fd,
	}
	end := view.Start()

	// TODO check with wrong sequence number
	// prePrepare with wrong view number
	prePrepareWrongView := proto.Clone(prePrepare).(*protos.Message)
	prePrepareWrongViewGet := prePrepareWrongView.GetPrePrepare()
	prePrepareWrongViewGet.View = 2

	// sent from node who is not the leader, simply ignore
	view.HandleMessage(2, prePrepareWrongView)
	synchronizer.AssertNotCalled(t, "Sync")
	fd.AssertNotCalled(t, "Complain")

	// sent from the leader
	syncWG.Add(1)
	fdWG.Add(1)
	view.HandleMessage(1, prePrepareWrongView)
	syncWG.Wait()
	synchronizer.AssertCalled(t, "Sync")
	fdWG.Wait()
	fd.AssertCalled(t, "Complain")

	end.Wait()

	view.ProposalSequence = 0

	// check prePrepare with verifier returning error
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, errors.New(""))
	view.Verifier = verifier
	end = view.Start()

	// sent from node who is not the leader, simply ignore
	view.HandleMessage(2, prePrepare)

	// sent from the leader
	syncWG.Add(1)
	fdWG.Add(1)
	view.HandleMessage(1, prePrepare)
	<-verifyLog
	syncWG.Wait()
	fdWG.Wait()

	end.Wait()

}

func TestBadPrepare(t *testing.T) {
	// Ensure that a prepare with a wrong view number sent by the leader causes a view abort,
	// and that a prepare with a wrong digest doesn't pass inspection.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	digestLog := make(chan struct{})
	log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Got wrong digest") {
			digestLog <- struct{}{}
		}
		return nil
	})).Sugar()
	synchronizer := &mocks.Synchronizer{}
	syncWG := &sync.WaitGroup{}
	synchronizer.On("Sync", mock.Anything).Run(func(args mock.Arguments) {
		syncWG.Done()
	}).Return(protos.ViewMetadata{}, uint64(0))
	fd := &mocks.FailureDetector{}
	fdWG := &sync.WaitGroup{}
	fd.On("Complain", mock.Anything).Run(func(args mock.Arguments) {
		fdWG.Done()
	})
	comm := &mocks.Comm{}
	commWG := sync.WaitGroup{}
	comm.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    4,
		Value: []byte{4},
	})
	state := &bft.StateRecorder{}
	view := &bft.View{
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
		Sync:             synchronizer,
		FailureDetector:  fd,
		Comm:             comm,
		Verifier:         verifier,
		Signer:           signer,
	}
	end := view.Start()

	commWG.Add(1)
	view.HandleMessage(1, prePrepare)
	commWG.Wait()

	// prepare with wrong view
	prepareWronngView := proto.Clone(prepare).(*protos.Message)
	prepareWronngViewGet := prepareWronngView.GetPrepare()
	prepareWronngViewGet.View = 2

	// sent from the leader
	syncWG.Add(1)
	fdWG.Add(1)
	view.HandleMessage(1, prepareWronngView)
	syncWG.Wait()
	fdWG.Wait()

	end.Wait()

	view.ProposalSequence = 0

	end = view.Start()

	commWG.Add(1)
	view.HandleMessage(1, prePrepare)
	commWG.Wait()

	// prepare with wrong digest
	prepareWronngDigest := proto.Clone(prepare).(*protos.Message)
	prepareWronngDigestGet := prepareWronngDigest.GetPrepare()
	prepareWronngDigestGet.Digest = wrongDigest

	view.HandleMessage(1, prepareWronngDigest)
	<-digestLog
	view.HandleMessage(2, prepareWronngDigest)
	<-digestLog
	signer.AssertNotCalled(t, "SignProposal", mock.Anything)

	view.Abort()
	end.Wait()

}

func TestBadCommit(t *testing.T) {
	// Ensure that a commit with a wrong digest or a bad signature doesn't pass inspection.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	digestLog := make(chan struct{})
	verifyLog := make(chan struct{})
	log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Got wrong digest") {
			digestLog <- struct{}{}
		}
		if strings.Contains(entry.Message, "Couldn't verify 2's signature:") {
			verifyLog <- struct{}{}
		}
		return nil
	})).Sugar()
	comm := &mocks.Comm{}
	comm.On("Broadcast", mock.Anything)
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(errors.New(""))
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    4,
		Value: []byte{4},
	})
	state := &bft.StateRecorder{}
	view := &bft.View{
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
		Comm:             comm,
		Verifier:         verifier,
		Signer:           signer,
	}
	end := view.Start()

	view.HandleMessage(1, prePrepare)

	view.HandleMessage(1, prepare)
	view.HandleMessage(2, prepare)

	// commit with wrong digest
	commitWrongDigest := proto.Clone(commit1).(*protos.Message)
	commitWrongDigestGet := commitWrongDigest.GetCommit()
	commitWrongDigestGet.Digest = wrongDigest

	view.HandleMessage(1, commitWrongDigest)
	<-digestLog

	view.HandleMessage(2, commit2)
	<-verifyLog

	view.Abort()
	end.Wait()
}

func TestNormalPath(t *testing.T) {
	// A test that takes a view through all 3 phases (prePrepare, prepare, and commit) until it reaches a decision.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	comm := &mocks.Comm{}
	commWG := sync.WaitGroup{}
	comm.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
		fmt.Println("Sending", args.Get(0))
		commWG.Done()
	})
	decider := &mocks.Decider{}
	deciderWG := sync.WaitGroup{}
	decidedProposal := make(chan types.Proposal)
	decidedSigs := make(chan []types.Signature)
	decider.On("Decide", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		deciderWG.Done()
		proposal, _ := args.Get(0).(types.Proposal)
		decidedProposal <- proposal
		sigs, _ := args.Get(1).([]types.Signature)
		decidedSigs <- sigs
	})
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    4,
		Value: []byte{4},
	})
	state := &bft.StateRecorder{}
	view := &bft.View{
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		ID:               1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
		Comm:             comm,
		Decider:          decider,
		Verifier:         verifier,
		Signer:           signer,
	}
	end := view.Start()

	commWG.Add(2)
	view.Propose(proposal)
	commWG.Wait()

	commWG.Add(1)
	view.HandleMessage(1, prepare)
	view.HandleMessage(2, prepare)
	commWG.Wait()

	deciderWG.Add(1)
	view.HandleMessage(1, commit1)
	view.HandleMessage(2, commit2)
	deciderWG.Wait()
	dProp := <-decidedProposal
	assert.Equal(t, proposal, dProp)
	dSigs := <-decidedSigs
	assert.Equal(t, 3, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 1 && sig.Id != 2 && sig.Id != 4 {
			assert.Fail(t, "signatures is from a different node with id", sig.Id)
		}
	}

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	commWG.Add(2)
	view.HandleMessage(1, prePrepareNext)
	commWG.Wait()

	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	commWG.Add(1)
	view.HandleMessage(1, prepareNext)
	view.HandleMessage(2, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1

	deciderWG.Add(1)
	view.HandleMessage(1, commit1Next)
	view.HandleMessage(2, commit2Next)
	deciderWG.Wait()
	dProp = <-decidedProposal
	assert.Equal(t, proposal, dProp)
	dSigs = <-decidedSigs
	assert.Equal(t, 3, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 1 && sig.Id != 2 && sig.Id != 4 {
			assert.Fail(t, "signatures is from a different node with id", sig.Id)
		}
	}

	view.Abort()
	end.Wait()
}

func TestTwoSequences(t *testing.T) {
	// A test that takes a view through all 3 phases of two consecutive sequences,
	// when all messages are sent in advanced for both sequences.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	comm := &mocks.Comm{}
	commWG := sync.WaitGroup{}
	comm.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	decider := &mocks.Decider{}
	deciderWG := sync.WaitGroup{}
	decidedProposal := make(chan types.Proposal, 1)
	decidedSigs := make(chan []types.Signature, 1)
	decider.On("Decide", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		deciderWG.Done()
		proposal, _ := args.Get(0).(types.Proposal)
		decidedProposal <- proposal
		sigs, _ := args.Get(1).([]types.Signature)
		decidedSigs <- sigs
	})
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    4,
		Value: []byte{4},
	})
	state := &bft.StateRecorder{}
	view := &bft.View{
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
		Comm:             comm,
		Decider:          decider,
		Verifier:         verifier,
		Signer:           signer,
	}
	end := view.Start()

	commWG.Add(1)
	view.HandleMessage(1, prePrepare)
	commWG.Wait()

	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1

	commWG.Add(1)
	view.HandleMessage(1, prepare)
	view.HandleMessage(1, prepareNext)
	view.HandleMessage(2, prepare)
	view.HandleMessage(2, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1

	deciderWG.Add(2)
	view.HandleMessage(1, commit1)
	view.HandleMessage(1, commit1Next)
	view.HandleMessage(2, commit2)
	view.HandleMessage(2, commit2Next)

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1

	commWG.Add(2)
	view.HandleMessage(1, prePrepareNext)
	commWG.Wait()

	deciderWG.Wait()
	dProp := <-decidedProposal
	assert.Equal(t, proposal, dProp)
	dSigs := <-decidedSigs
	assert.Equal(t, 3, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 1 && sig.Id != 2 && sig.Id != 4 {
			assert.Fail(t, "signatures is from a different node with id", sig.Id)
		}
	}
	dProp = <-decidedProposal
	assert.Equal(t, proposal, dProp)
	dSigs = <-decidedSigs
	assert.Equal(t, 3, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 1 && sig.Id != 2 && sig.Id != 4 {
			assert.Fail(t, "signatures is from a different node with id", sig.Id)
		}
	}

	view.Abort()
	end.Wait()

}
