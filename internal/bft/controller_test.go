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
	"github.com/SmartBFT-Go/consensus/pkg/wal"
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
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything)
	controller := bft.Controller{
		ID:          4, // not the leader
		N:           4,
		Logger:      log,
		Application: app,
	}
	end := controller.Start(1, 0)
	controller.ViewChanged(2, 1)
	controller.ViewChanged(3, 2)
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
			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
			comm := &mocks.CommMock{}
			comm.On("BroadcastConsensus", mock.Anything)
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

func TestControllerLeaderBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	batcher := &mocks.Batcher{}
	batcherChan := make(chan struct{})
	var once sync.Once
	batcher.On("NextBatch").Run(func(args mock.Arguments) {
		once.Do(func() {
			batcherChan <- struct{}{}
		})
	}).Return([][]byte{})
	controller := bft.Controller{
		ID:      1, // the leader
		N:       4,
		Logger:  log,
		Batcher: batcher,
	}
	end := controller.Start(1, 0)
	<-batcherChan
	controller.Stop()
	end.Wait()
	batcher.AssertCalled(t, "NextBatch")
}

func TestLeaderPropose(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{}).Once()
	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 1,
		ViewId:         1,
	})
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	signer := &mocks.SignerMock{}
	signer.On("SignProposal", mock.Anything).Return(&types.Signature{
		Id:    1,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	})
	controller := bft.Controller{
		WAL:         &wal.EphemeralWAL{},
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
	commWG.Add(2)
	end := controller.Start(1, 0)
	commWG.Wait() // propose

	commWG.Add(1)
	controller.ProcessMessages(2, prepare)
	controller.ProcessMessages(3, prepare)
	commWG.Wait()

	controller.ProcessMessages(2, commit2)
	commit3 := proto.Clone(commit2).(*protos.Message)
	commit3Get := commit3.GetCommit()
	commit3Get.Signature.Signer = 3
	appWG.Add(1)  // deliver
	commWG.Add(2) // next proposal
	controller.ProcessMessages(3, commit3)
	appWG.Wait()
	commWG.Wait()

	controller.Stop()
	end.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 1)
}

func TestLeaderChange(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)

	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 0,
		ViewId:         2,
	})

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
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
	controller := bft.Controller{
		WAL:             &wal.EphemeralWAL{},
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
	prePrepareWrongViewGet.Proposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 0,
		ViewId:         2,
	})
	fdWG.Add(1)
	syncWG.Add(1)
	controller.ProcessMessages(1, prePrepareWrongView)
	fdWG.Wait()
	syncWG.Wait()

	commWG.Add(2)
	controller.ViewChanged(2, 0)
	commWG.Wait()
	batcher.AssertNumberOfCalls(t, "NextBatch", 1)
	verifier.AssertNumberOfCalls(t, "VerifyRequest", 1)
	assembler.AssertNumberOfCalls(t, "AssembleProposal", 1)
	comm.AssertNumberOfCalls(t, "BroadcastConsensus", 2)
	controller.Stop()
	end.Wait()
}
