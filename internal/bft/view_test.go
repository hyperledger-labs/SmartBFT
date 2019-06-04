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
