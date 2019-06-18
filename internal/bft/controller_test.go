// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

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

func TestControllerBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	app := &mocks.Application{}
	app.On("Deliver", mock.Anything, mock.Anything)
	controller := bft.Controller{
		ID:          2, // not the leader
		N:           4,
		Logger:      log,
		Application: app,
	}
	end := controller.Start(1, 0)
	controller.Decide(types.Proposal{}, nil)
	controller.Stop()
	controller.Stop()
	end.Wait()
}

func TestQuorum(t *testing.T) {
	// Ensure that quorum size is as expected.

	type quorum struct {
		N uint64
		F int
		Q int
	}

	quorums := []quorum{{4, 1, 3}, {5, 1, 4}, {6, 1, 4}, {7, 2, 5}, {8, 2, 6},
		{9, 2, 6}, {10, 3, 7}, {11, 3, 8}, {12, 3, 8}}

	for _, testCase := range quorums {
		t.Run(fmt.Sprintf("%d nodes", testCase.N), func(t *testing.T) {
			verifier := &mocks.Verifier{}
			verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil)
			comm := &mocks.Comm{}
			comm.On("Broadcast", mock.Anything)
			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			verifyLog := make(chan struct{}, 1)
			log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, fmt.Sprintf("The number of nodes (N) is %d,"+
					" F is %d, and the quorum size is %d", testCase.N, testCase.F, testCase.Q)) {
					verifyLog <- struct{}{}
				}
				return nil
			})).Sugar()
			controller := bft.Controller{
				ID:     2, // not the leader
				N:      testCase.N,
				Logger: log,
			}
			end := controller.Start(1, 0)
			<-verifyLog
			controller.Stop()
			end.Wait()
		})
	}

}

func TestControllerViewChanged(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	controller := bft.Controller{
		ID:     4, // not the leader
		N:      4,
		Logger: log,
	}
	end := controller.Start(1, 0)

	controller.ViewChanged(2, 1)
	controller.ViewChanged(3, 2)

	controller.Stop()
	end.Wait()
}

func TestControllerLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	batcher := &mocks.Batcher{}
	batcher.On("NextBatch").Return([][]byte{})
	controller := bft.Controller{
		ID:      1, // the leader
		N:       4,
		Logger:  log,
		Batcher: batcher,
	}
	end := controller.Start(1, 0)
	controller.Stop()
	end.Wait()
}

func TestLeaderPropose(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.Verifier{}
	verifier.On("VerifyRequest", req).Return(nil)
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	assembler := &mocks.Assembler{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{})
	comm := &mocks.Comm{}
	commWG := sync.WaitGroup{}
	comm.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	signer := &mocks.Signer{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    1,
		Value: []byte{4},
	})
	app := &mocks.Application{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	})
	controller := bft.Controller{
		ID:          1, // the leader
		N:           4,
		Logger:      log,
		Batcher:     batcher,
		Verifier:    verifier,
		Assembler:   assembler,
		Comm:        comm,
		Signer:      signer,
		Application: app,
	}
	commWG.Add(1)
	end := controller.Start(1, 0)
	commWG.Wait() // propose

	commWG.Add(1)
	controller.ProcessMessages(1, prePrepare)
	commWG.Wait()

	commWG.Add(1)
	controller.ProcessMessages(2, prepare)
	controller.ProcessMessages(3, prepare)
	commWG.Wait()

	controller.ProcessMessages(2, commit2)
	commit3 := proto.Clone(commit2).(*protos.Message)
	commit3Get := commit3.GetCommit()
	commit3Get.Signature.Signer = 3
	appWG.Add(1)  // deliver
	commWG.Add(1) // next proposal
	controller.ProcessMessages(3, commit3)
	appWG.Wait()
	commWG.Wait()

	controller.Stop()
	end.Wait()
}

func TestLeaderChange(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.Verifier{}
	verifier.On("VerifyRequest", req).Return(nil)
	assembler := &mocks.Assembler{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{})
	comm := &mocks.Comm{}
	commWG := sync.WaitGroup{}
	comm.On("Broadcast", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
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
	controller := bft.Controller{
		ID:              2, // the next leader
		N:               4,
		Logger:          log,
		Batcher:         batcher,
		Verifier:        verifier,
		Assembler:       assembler,
		Comm:            comm,
		Synchronizer:    synchronizer,
		FailureDetector: fd,
	}
	end := controller.Start(1, 0)

	prePrepareWrongView := proto.Clone(prePrepare).(*protos.Message)
	prePrepareWrongViewGet := prePrepareWrongView.GetPrePrepare()
	prePrepareWrongViewGet.View = 2
	fdWG.Add(1)
	syncWG.Add(1)
	controller.ProcessMessages(1, prePrepareWrongView)
	fdWG.Wait()
	syncWG.Wait()

	commWG.Add(1)
	controller.ViewChanged(2, 0)
	commWG.Wait()
	batcher.AssertNumberOfCalls(t, "NextBatch", 1)
	verifier.AssertNumberOfCalls(t, "VerifyRequest", 1)
	assembler.AssertNumberOfCalls(t, "AssembleProposal", 1)
	controller.Stop()
	end.Wait()
}
