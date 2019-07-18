// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"sync/atomic"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	proposal = types.Proposal{
		Header:  []byte{0},
		Payload: []byte{1},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 0,
			ViewId:         1,
		}),
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
					Header:  []byte{0},
					Payload: []byte{1},
					Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence: 0,
						ViewId:         1,
					}),
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

	commit3 = &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   1,
				Seq:    0,
				Digest: digest,
				Signature: &protos.Signature{
					Signer: 3,
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

	var synchronizer *mocks.SynchronizerMock
	var fd *mocks.FailureDetector
	var syncWG *sync.WaitGroup
	var fdWG *sync.WaitGroup

	for _, testCase := range []struct {
		description           string
		sender                uint64
		expectedErr           string
		setup                 func()
		corruptProposal       func(*protos.PrePrepare)
		assert                func()
		verifyProposalReturns error
	}{
		{
			description: "wrong view number",
			expectedErr: "from 1 of view 2, expected view 1",
			sender:      1,
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {
				proposal.View++
			},
			assert: func() {
				syncWG.Wait()
				synchronizer.AssertCalled(t, "Sync")
				fdWG.Wait()
				fd.AssertCalled(t, "Complain")
			},
		},
		{
			description: "sent from wrong node",
			expectedErr: "Got pre-prepare from 2 but the leader is 1",
			sender:      2,
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {},
			assert:          func() {},
		},
		{
			description:           "bad proposal",
			expectedErr:           "Received bad proposal from 1: unauthorized client",
			sender:                1,
			verifyProposalReturns: errors.New("unauthorized client"),
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {},
			assert: func() {
				syncWG.Wait()
				synchronizer.AssertCalled(t, "Sync")
				fdWG.Wait()
				fd.AssertCalled(t, "Complain")
			},
		},
		{
			description: "bad verification sequence",
			expectedErr: "Expected verification sequence 1 but got 2",
			sender:      1,
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {
				proposal.Proposal.VerificationSequence++
			},
			assert: func() {
				syncWG.Wait()
				synchronizer.AssertCalled(t, "Sync")
				fdWG.Wait()
				fd.AssertCalled(t, "Complain")
			},
		},
		{
			description: "nil proposal",
			expectedErr: "Got pre-prepare with empty proposal",
			sender:      1,
			setup:       func() {},
			corruptProposal: func(proposal *protos.PrePrepare) {
				proposal.Proposal = nil
			},
			assert: func() {},
		},
		{
			description: "wrong view number in metadata",
			expectedErr: "Received bad proposal from 1: invalid view number",
			sender:      1,
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {
				proposal.Proposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence: 0,
					ViewId:         2,
				})
			},
			assert: func() {
				syncWG.Wait()
				synchronizer.AssertCalled(t, "Sync")
				fdWG.Wait()
				fd.AssertCalled(t, "Complain")
			},
		},
		{
			description: "wrong proposal sequence in metadata",
			expectedErr: "Received bad proposal from 1: invalid proposal sequence",
			sender:      1,
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {
				proposal.Proposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence: 1,
					ViewId:         1,
				})
			},
			assert: func() {
				syncWG.Wait()
				synchronizer.AssertCalled(t, "Sync")
				fdWG.Wait()
				fd.AssertCalled(t, "Complain")
			},
		},
		{
			description: "corrupt metadata in proposal",
			expectedErr: "Received bad proposal from 1: proto: smartbftprotos.ViewMetadata: illegal tag 0 (wire type 1)",
			sender:      1,
			setup: func() {
				syncWG.Add(1)
				fdWG.Add(1)
			},
			corruptProposal: func(proposal *protos.PrePrepare) {
				proposal.Proposal.Metadata = []byte{1, 2, 3}
			},
			assert: func() {
				syncWG.Wait()
				synchronizer.AssertCalled(t, "Sync")
				fdWG.Wait()
				fd.AssertCalled(t, "Complain")
			},
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			var errorLogged sync.WaitGroup
			errorLogged.Add(1)

			log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, testCase.expectedErr) {
					errorLogged.Done()
				}
				return nil
			})).Sugar()
			synchronizer = &mocks.SynchronizerMock{}
			syncWG = &sync.WaitGroup{}
			synchronizer.On("Sync").Run(func(args mock.Arguments) {
				syncWG.Done()
			}).Return(protos.ViewMetadata{}, uint64(0))
			fd = &mocks.FailureDetector{}
			fdWG = &sync.WaitGroup{}
			fd.On("Complain", mock.Anything).Run(func(args mock.Arguments) {
				fdWG.Done()
			})
			state := &bft.StateRecorder{}
			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyProposal", mock.Anything).Return(nil, testCase.verifyProposalReturns)
			verifier.On("VerificationSequence").Return(uint64(1))
			view := &bft.View{
				Verifier:         verifier,
				SelfID:           3,
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

			proposalSentByLeader := proto.Clone(prePrepare).(*protos.Message)
			testCase.corruptProposal(proposalSentByLeader.GetPrePrepare())

			testCase.setup()
			view.HandleMessage(testCase.sender, proposalSentByLeader)
			errorLogged.Wait()
			testCase.assert()
			view.Abort()
			end.Wait()
		})
	}
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
	synchronizer := &mocks.SynchronizerMock{}
	syncWG := &sync.WaitGroup{}
	synchronizer.On("Sync", mock.Anything).Run(func(args mock.Arguments) {
		syncWG.Done()
	}).Return(protos.ViewMetadata{}, uint64(0))
	fd := &mocks.FailureDetector{}
	fdWG := &sync.WaitGroup{}
	fd.On("Complain", mock.Anything).Run(func(args mock.Arguments) {
		fdWG.Done()
	})
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
	signer := &mocks.SignerMock{}
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
	comm := &mocks.CommMock{}
	comm.On("BroadcastConsensus", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(errors.New(""))
	signer := &mocks.SignerMock{}
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
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
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
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	signer := &mocks.SignerMock{}
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
		SelfID:           1,
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
	view.HandleMessage(2, prepare)
	view.HandleMessage(3, prepare)
	commWG.Wait()

	deciderWG.Add(1)
	view.HandleMessage(2, commit2)
	view.HandleMessage(3, commit3)
	deciderWG.Wait()
	dProp := <-decidedProposal
	assert.Equal(t, proposal, dProp)
	dSigs := <-decidedSigs
	assert.Equal(t, 3, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 2 && sig.Id != 3 && sig.Id != 4 {
			assert.Fail(t, "signatures is from a different node with id", sig.Id)
		}
	}

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 1,
		ViewId:         1,
	})

	nextProp := types.Proposal{
		Header:               prePrepareNextGet.Proposal.Header,
		Payload:              prePrepareNextGet.Proposal.Payload,
		Metadata:             prePrepareNextGet.Proposal.Metadata,
		VerificationSequence: 1,
	}

	commWG.Add(2)
	view.HandleMessage(1, prePrepareNext)
	commWG.Wait()

	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	commWG.Add(1)
	view.HandleMessage(2, prepareNext)
	view.HandleMessage(3, prepareNext)
	commWG.Wait()

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = nextProp.Digest()

	commit3Next := proto.Clone(commit3).(*protos.Message)
	commit3NextGet := commit3Next.GetCommit()
	commit3NextGet.Seq = 1
	commit3NextGet.Digest = nextProp.Digest()

	deciderWG.Add(1)
	view.HandleMessage(2, commit2Next)
	view.HandleMessage(3, commit3Next)
	deciderWG.Wait()
	dProp = <-decidedProposal
	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 1,
		ViewId:         1,
	})
	assert.Equal(t, secondProposal, dProp)
	dSigs = <-decidedSigs
	assert.Equal(t, 3, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 2 && sig.Id != 3 && sig.Id != 4 {
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
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
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
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	signer := &mocks.SignerMock{}
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

	secondProposal := types.Proposal{
		Header:  []byte{0},
		Payload: []byte{1},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 1,
			ViewId:         1,
		}),
		VerificationSequence: 1,
	}

	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = secondProposal.Digest()

	commWG.Add(1)
	view.HandleMessage(1, prepare)
	view.HandleMessage(1, prepareNext)
	view.HandleMessage(2, prepare)
	view.HandleMessage(2, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1
	commit1NextGet.Digest = secondProposal.Digest()

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = secondProposal.Digest()

	deciderWG.Add(2)
	view.HandleMessage(1, commit1)
	view.HandleMessage(1, commit1Next)
	view.HandleMessage(2, commit2)
	view.HandleMessage(2, commit2Next)

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.Proposal.Metadata = secondProposal.Metadata

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
	assert.Equal(t, secondProposal, dProp)
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

func TestViewPersisted(t *testing.T) {
	for _, testCase := range []struct {
		description        string
		crashAfterProposed bool
		crashAfterPrepared bool
	}{
		{
			description: "No crashes",
		},
		{
			description:        "Crash after receiving proposal",
			crashAfterProposed: true,
		},
		{
			description:        "Crash after receiving prepares",
			crashAfterPrepared: true,
		},
		{
			description:        "Crash after both",
			crashAfterPrepared: true,
			crashAfterProposed: true,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			verifier := &mocks.VerifierMock{}
			verifier.On("VerificationSequence").Return(uint64(1))
			verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
			verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			var prepareSent sync.WaitGroup
			var commitSent sync.WaitGroup

			comm := &mocks.CommMock{}
			comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
				msg := args.Get(0).(*protos.Message)
				prepare := msg.GetPrepare() != nil
				commit := msg.GetCommit() != nil

				if prepare {
					prepareSent.Done()
					return
				}

				if commit {
					commitSent.Done()
					return
				}

				t.Fatalf("Sent a message that isn't a prepare nor a commit")
			})

			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			log := basicLog.Sugar()

			signer := &mocks.SignerMock{}
			signer.On("SignProposal", mock.Anything).Return(&types.Signature{Value: []byte{4}, Id: 2})

			var deciderWG sync.WaitGroup
			deciderWG.Add(1)
			decider := &mocks.Decider{}
			decider.On("Decide", mock.Anything, mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
				deciderWG.Done()
			})

			state := &mocks.State{}

			view := &bft.View{
				Signer:           signer,
				Decider:          decider,
				Comm:             comm,
				Verifier:         verifier,
				SelfID:           2,
				State:            state,
				Logger:           log,
				N:                4,
				LeaderID:         1,
				Quorum:           3,
				Number:           1,
				ProposalSequence: 0,
			}

			persistedState := &bft.PersistedState{
				Logger: log,
				WAL:    &wal.EphemeralWAL{},
			}
			var persistedToLog sync.WaitGroup
			persistedToLog.Add(1)
			state.On("Save", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				persistedToLog.Done()
				persistedState.Save(args.Get(0).(*protos.Message))
			})

			end := view.Start()

			prepareSent.Add(1)

			view.HandleMessage(1, prePrepare)

			// Wait until the node persists the proposal to WAL.
			persistedToLog.Wait()

			// After persistence, the node will broadcast a prepare.
			prepareSent.Wait()

			if testCase.crashAfterProposed {
				// Simulate a crash.
				view.Abort()
				end.Wait()

				// Recover the view from WAL.
				persistedState.Restore(view)

				// It should broadcast a prepare right after starting it.
				prepareSent.Add(1)

				// Restart the view.
				end = view.Start()

				// Wait for the prepare to be sent again.
				prepareSent.Wait()
			}

			// It should persist to WAL the commit after receiving enough prepares.
			persistedToLog.Add(1)

			// Get the prepares from the rest of the nodes.
			view.HandleMessage(1, prepare)
			view.HandleMessage(3, prepare)

			// It should broadcast a commit right after persisting the commit to WAL.
			commitSent.Add(1)

			// Wait until the node persists the proposal to WAL.
			persistedToLog.Wait()

			// Wait until the node broadcasts a commit.
			commitSent.Wait()

			if testCase.crashAfterPrepared {
				// Simulate a crash.
				view.Abort()
				end.Wait()

				// Recover the view from WAL.
				persistedState.Restore(view)

				// It should broadcast a commit again after it is restored.
				commitSent.Add(1)

				// Restart the view.
				end = view.Start()

				// Wait until the node broadcasts the commit again.
				commitSent.Wait()
			}

			// Get the commits from nodes.
			view.HandleMessage(1, commit1)
			view.HandleMessage(3, commit3)

			// Wait for the proposal to be committed.
			deciderWG.Wait()

			view.Abort()
			end.Wait()
		})
	}
}

func TestViewLaggingCatchup(t *testing.T) {
	// Scenario: 4 nodes total, while 1 node (node 4)
	// is disconnected while proposal 0 is decided on.
	// After it is committed and all 3 nodes move to sequence 1,
	// node 4 receives the proposal late, and must catchup
	// with the rest of the nodes.
	// Then, a similar thing happens with node 3.

	// Setup 3 online nodes and 1 offline nodes

	network := newNetwork(t, 4)
	network.start()

	v1 := network[1]
	v2 := network[2]
	v3 := network[3]
	v4 := network[4]

	t.Run("Node 4 is behind", func(t *testing.T) {
		network.disconnect(4)

		// Have the leader send a proposal
		v1.deciderWG.Add(1)
		v2.deciderWG.Add(1)
		v3.deciderWG.Add(1)
		v1.Propose(proposal)

		// Wait for all 3 nodes to commit sequence 0
		v1.deciderWG.Wait()
		v2.deciderWG.Wait()
		v3.deciderWG.Wait()

		// Make v4 online and start the second proposal
		network.connect(4)
		v4.deciderWG.Add(1)
		// Send artificially a pre-prepare from the leader to v4.
		latePrePrepare := &protos.Message{
			Content: &protos.Message_PrePrepare{
				PrePrepare: &protos.PrePrepare{
					View: 1,
					Seq:  0,
					Proposal: &protos.Proposal{
						Header:               proposal.Header,
						Payload:              proposal.Payload,
						Metadata:             proposal.Metadata,
						VerificationSequence: uint64(proposal.VerificationSequence),
					},
				},
			},
		}

		v4.HandleMessage(1, latePrePrepare)
		// Wait for node 4 to commit sequence 0
		v4.deciderWG.Wait()
	})

	t.Run("Node 3 is behind", func(t *testing.T) {
		nextProposal := types.Proposal{
			Header:  []byte{0},
			Payload: []byte{1},
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 1,
				ViewId:         1,
			}),
			VerificationSequence: 1,
		}

		latePrePrepare := &protos.Message{
			Content: &protos.Message_PrePrepare{
				PrePrepare: &protos.PrePrepare{
					View: 1,
					Seq:  1,
					Proposal: &protos.Proposal{
						Header:               nextProposal.Header,
						Payload:              nextProposal.Payload,
						Metadata:             nextProposal.Metadata,
						VerificationSequence: uint64(proposal.VerificationSequence),
					},
				},
			},
		}

		// Disconnect node 3 this time.
		network.disconnect(3)

		v1.deciderWG.Add(1)
		v2.deciderWG.Add(1)
		v3.deciderWG.Add(1)
		v4.deciderWG.Add(1)

		v1.Propose(nextProposal)
		// Wait for all nodes but node 3 to commit.
		v1.deciderWG.Wait()
		v2.deciderWG.Wait()
		v4.deciderWG.Wait()

		// Reconnect back node 3
		network.connect(3)

		// Resend the proposal to node 3
		v3.HandleMessage(1, latePrePrepare)

		// Wait for node 3 to commit sequence 1
		v3.deciderWG.Wait()
	})

	network.stop()
}

type testedNetwork map[uint64]*testedView

func (tn testedNetwork) disconnect(id uint64) {
	tn[id].setOffline()
}

func (tn testedNetwork) connect(id uint64) {
	tn[id].setOnline()
}

func (tn testedNetwork) start() {
	for id, view := range tn {
		tn[id].end = view.Start()
	}
}

func (tn testedNetwork) stop() {
	for _, view := range tn {
		view.Abort()
		view.end.Wait()
	}
}

func newNetwork(t *testing.T, size int) testedNetwork {
	network := make(testedNetwork)
	for id := 1; id <= size; id++ {
		v := newView(t, uint64(id), network)
		network[uint64(id)] = v
	}
	return network
}

type testedView struct {
	offline   uint32
	network   *testedNetwork
	deciderWG sync.WaitGroup
	*bft.View
	end bft.Future
}

func (tv *testedView) setOffline() {
	atomic.StoreUint32(&tv.offline, 1)
}

func (tv *testedView) setOnline() {
	atomic.StoreUint32(&tv.offline, 0)
}

func (tv *testedView) HandleMessage(sender uint64, m *protos.Message) {
	if atomic.LoadUint32(&tv.offline) == uint32(1) {
		return
	}
	tv.View.HandleMessage(sender, m)
}

func newView(t *testing.T, selfID uint64, network map[uint64]*testedView) *testedView {
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	tv := &testedView{}

	comm := &mocks.CommMock{}
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(*protos.Message)
		for _, view := range network {
			view.HandleMessage(selfID, m)
		}
	})
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		dst := args.Get(0).(uint64)
		m := args.Get(1).(*protos.Message)
		network[dst].HandleMessage(selfID, m)
	})

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	signer := &mocks.SignerMock{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{Value: []byte{4}, Id: selfID})

	decider := &mocks.Decider{}
	decider.On("Decide", mock.Anything, mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
		tv.deciderWG.Done()
	})

	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)

	tv.View = &bft.View{
		Signer:           signer,
		Decider:          decider,
		Comm:             comm,
		Verifier:         verifier,
		SelfID:           selfID,
		State:            state,
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Quorum:           3,
		Number:           1,
		ProposalSequence: 0,
	}

	return tv
}
