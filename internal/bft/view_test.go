// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"sync"
	"testing"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/protos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
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
	log := basicLog.Sugar()
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
	decider.On("Decide", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		deciderWG.Done()
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

	digest := types.Proposal{
		Header:               []byte{0},
		Payload:              []byte{1},
		Metadata:             []byte{2},
		VerificationSequence: 1,
	}.Digest()

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
	view.HandleMessage(3, prepare)
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
	commit3 := &protos.Message{
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
	view.HandleMessage(3, commit3)
	deciderWG.Wait()

	view.Abort()
	end.Wait()
}
