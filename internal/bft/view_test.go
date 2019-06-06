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

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestViewBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	view := &bft.View{
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Number:           1,
		ProposalSequence: new(uint64),
	}
	end := view.Start()
	view.Abort()
	end.Wait()
}

func TestBadPrePrepare(t *testing.T) {
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
	synchronizer.On("SyncIfNeeded", mock.Anything).Run(func(args mock.Arguments) {
		syncWG.Done()
	})
	fd := &mocks.FailureDetector{}
	fdWG := &sync.WaitGroup{}
	fd.On("Complain", mock.Anything).Run(func(args mock.Arguments) {
		fdWG.Done()
	})
	view := &bft.View{
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Number:           1,
		ProposalSequence: new(uint64),
		Sync:             synchronizer,
		FailureDetector:  fd,
	}
	end := view.Start()

	// TODO check with wrong sequence number
	// prePrepare with wrong view number
	msg := &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View:     2,
				Seq:      0,
				Proposal: &protos.Proposal{},
			},
		},
	}

	// sent from node who is not the leader, simply ignore
	view.HandleMessage(2, msg)
	synchronizer.AssertNotCalled(t, "SyncIfNeeded")
	fd.AssertNotCalled(t, "Complain")

	// sent from the leader
	syncWG.Add(1)
	fdWG.Add(1)
	view.HandleMessage(1, msg)
	syncWG.Wait()
	synchronizer.AssertCalled(t, "SyncIfNeeded")
	fdWG.Wait()
	fd.AssertCalled(t, "Complain")

	end.Wait()

	// check prePrepare with verifier returning error
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(errors.New(""))
	view.Verifier = verifier
	end = view.Start()

	msg = &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View:     1,
				Seq:      0,
				Proposal: &protos.Proposal{},
			},
		},
	}

	// sent from node who is not the leader, simply ignore
	view.HandleMessage(2, msg)

	// sent from the leader
	syncWG.Add(1)
	fdWG.Add(1)
	view.HandleMessage(1, msg)
	<-verifyLog
	syncWG.Wait()
	fdWG.Wait()

	end.Wait()

}

func TestBadPrepare(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	digestLog := make(chan struct{})
	log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Got digest") && strings.Contains(entry.Message, "but expected") {
			digestLog <- struct{}{}
		}
		return nil
	})).Sugar()
	synchronizer := &mocks.Synchronizer{}
	syncWG := &sync.WaitGroup{}
	synchronizer.On("SyncIfNeeded", mock.Anything).Run(func(args mock.Arguments) {
		syncWG.Done()
	})
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
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil)
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    4,
		Value: []byte{4},
	})
	view := &bft.View{
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Number:           1,
		ProposalSequence: new(uint64),
		Sync:             synchronizer,
		FailureDetector:  fd,
		Comm:             comm,
		Verifier:         verifier,
		Signer:           signer,
	}
	end := view.Start()

	pp := &protos.Message{
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

	commWG.Add(1)
	view.HandleMessage(1, pp)
	commWG.Wait()

	proposal := types.Proposal{
		Header:               []byte{0},
		Payload:              []byte{1},
		Metadata:             []byte{2},
		VerificationSequence: 1,
	}
	digest := proposal.Digest()

	// prepare with wrong view
	prepare := &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   2,
				Seq:    0,
				Digest: digest,
			},
		},
	}

	// sent from the leader
	syncWG.Add(1)
	fdWG.Add(1)
	view.HandleMessage(1, prepare)
	syncWG.Wait()
	fdWG.Wait()

	end.Wait()

	end = view.Start()

	commWG.Add(1)
	view.HandleMessage(1, pp)
	commWG.Wait()

	wrongProposal := types.Proposal{
		Header:               []byte{1},
		Payload:              []byte{2},
		Metadata:             []byte{3},
		VerificationSequence: 1,
	}
	wrongDigest := wrongProposal.Digest()

	// prepare with wrong digest
	prepare = &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   1,
				Seq:    0,
				Digest: wrongDigest,
			},
		},
	}

	view.HandleMessage(1, prepare)
	<-digestLog
	view.HandleMessage(2, prepare)
	<-digestLog
	signer.AssertNotCalled(t, "SignProposal", mock.Anything)

	view.Abort()
	end.Wait()

}

func TestNormalPath(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	synchronizer := &mocks.Synchronizer{}
	synchronizer.On("SyncIfNeeded", mock.Anything)
	fd := &mocks.FailureDetector{}
	fd.On("Complain", mock.Anything)
	comm := &mocks.Comm{}
	commWG := sync.WaitGroup{}
	comm.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	decider := &mocks.Decider{}
	deciderWG := sync.WaitGroup{}
	decidedProposal := make(chan types.Proposal)
	decidedSigs := make(chan []types.Signature)
	decider.On("Decide", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		deciderWG.Done()
		proposal, _ := args.Get(0).(types.Proposal)
		decidedProposal <- proposal
		sigs, _ := args.Get(1).([]types.Signature)
		decidedSigs <- sigs
	})
	verifier := &mocks.Verifier{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    4,
		Value: []byte{4},
	})
	view := &bft.View{
		Logger:           log,
		N:                4,
		LeaderID:         1,
		Number:           1,
		ProposalSequence: new(uint64),
		Sync:             synchronizer,
		FailureDetector:  fd,
		Comm:             comm,
		Decider:          decider,
		Verifier:         verifier,
		Signer:           signer,
	}
	end := view.Start()

	pp := &protos.Message{
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

	commWG.Add(1)
	view.HandleMessage(1, pp)
	commWG.Wait()

	proposal := types.Proposal{
		Header:               []byte{0},
		Payload:              []byte{1},
		Metadata:             []byte{2},
		VerificationSequence: 1,
	}
	digest := proposal.Digest()

	prepare := &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				View:   1,
				Seq:    0,
				Digest: digest,
			},
		},
	}

	commWG.Add(1)
	view.HandleMessage(1, prepare)
	view.HandleMessage(2, prepare)
	commWG.Wait()

	deciderWG.Add(1)
	commit1 := &protos.Message{
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
	view.HandleMessage(1, commit1)
	commit2 := &protos.Message{
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
	view.HandleMessage(2, commit2)
	deciderWG.Wait()
	dProp := <-decidedProposal
	assert.Equal(t, proposal, dProp)
	dSigs := <-decidedSigs
	assert.Equal(t, 2, len(dSigs))
	for _, sig := range dSigs {
		if sig.Id != 1 && sig.Id != 2 {
			assert.Fail(t, "signatures is from a different node with id", sig.Id)
		}
	}

	view.Abort()
	end.Wait()
}
