// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/metrics/disabled"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestControllerBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything)
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		Checkpoint:    &types.Checkpoint{},
		Batcher:       batcher,
		RequestPool:   pool,
		LeaderMonitor: leaderMon,
		ID:            1, // not the leader
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Application:   app,
		Comm:          comm,
		Verifier:      verifier,
		StartedWG:     &startedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	controller.Start(1, 0, 0, false)

	leaderMon.On("ProcessMsg", uint64(2), heartbeat)
	controller.ProcessMessages(2, heartbeat)
	controller.ViewChanged(2, 1)
	controller.ViewChanged(3, 2)
	controller.AbortView(3)
	controller.AbortView(3)
	controller.Stop()
	controller.Stop()
}

func TestControllerLeaderBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcherChan := make(chan struct{})
	var once sync.Once
	batcher.On("NextBatch").Run(func(args mock.Arguments) {
		once.Do(func() {
			batcherChan <- struct{}{}
		})
	}).Return([][]byte{})
	pool := &mocks.RequestPool{}
	pool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("Close")
	commMock := &mocks.CommMock{}
	commMock.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerificationSequence").Return(uint64(1))

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &types.Checkpoint{},
		RequestPool:   pool,
		LeaderMonitor: leaderMon,
		ID:            2, // the leader
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Batcher:       batcher,
		Comm:          commMock,
		Verifier:      verifier,
		StartedWG:     &startedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	controller.Start(1, 0, 0, false)
	<-batcherChan
	controller.Stop()
	batcher.AssertCalled(t, "NextBatch")
}

func TestLeaderPropose(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("PopRemainder").Return([][]byte{})
	batcher.On("BatchRemainder", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{11, 23},
	}))
	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{}).Once()
	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
	})
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	comm.On("Nodes").Return([]uint64{11, 17, 23, 37})
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    17,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})
	reqPool := &mocks.RequestPool{}
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("Close")

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal:   types.Proposal{VerificationSequence: 0},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	collector := bft.StateCollector{
		SelfID:         11,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		WAL:           wal,
		ID:            17, // the leader
		N:             4,
		NodesList:     []uint64{11, 17, 23, 37},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		Signer:        signer,
		Application:   app,
		Checkpoint:    &types.Checkpoint{},
		ViewChanger:   &bft.ViewChanger{},
		Synchronizer:  synchronizer,
		Collector:     &collector,
		StartedWG:     &startedWG,
		MetricsView:   api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	commWG.Add(9)
	controller.Start(1, 0, 0, true)
	commWG.Wait() // propose + state request

	commWG.Add(3)
	controller.ProcessMessages(23, prepare)
	controller.ProcessMessages(37, prepare)
	commWG.Wait()

	commit23 := proto.Clone(commit2).(*protos.Message)
	commit23Get := commit23.GetCommit()
	commit23Get.Signature.Signer = 23
	controller.ProcessMessages(23, commit23)
	commit37 := proto.Clone(commit3).(*protos.Message)
	commit37Get := commit37.GetCommit()
	commit37Get.Signature.Signer = 37
	appWG.Add(1)  // deliver
	commWG.Add(6) // next proposal
	controller.ProcessMessages(37, commit37)
	appWG.Wait()
	commWG.Wait()

	controller.Stop()
	app.AssertNumberOfCalls(t, "Deliver", 1)
	leaderMon.AssertCalled(t, "HeartbeatWasSent")

	// Ensure checkpoint was updated
	expected := &protos.Proposal{
		Header:               proposal.Header,
		Payload:              proposal.Payload,
		Metadata:             proposal.Metadata,
		VerificationSequence: uint64(proposal.VerificationSequence),
	}
	proposal, signatures := controller.Checkpoint.Get()
	assert.Equal(t, expected, proposal)
	signaturesBySigners := make(map[uint64]*protos.Signature)
	for _, sig := range signatures {
		signaturesBySigners[sig.Signer] = sig
	}
	assert.Equal(t, map[uint64]*protos.Signature{
		17: {Signer: 17, Value: []byte{4}},
		23: {Signer: 23, Value: []byte{4}},
		37: {Signer: 37, Value: []byte{4}},
	}, signaturesBySigners)

	controller.Stop()
	collector.Stop()
	wal.Close()
}

func TestViewChanged(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("Reset")
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerificationSequence").Return(uint64(1))
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
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	reqPool := &mocks.RequestPool{}
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything)
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("Close")

	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal:   types.Proposal{VerificationSequence: 0},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	collector := bft.StateCollector{
		SelfID:         1,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &types.Checkpoint{},
		Signer:        signer,
		WAL:           wal,
		ID:            3, // the next leader
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		Synchronizer:  synchronizer,
		Collector:     &collector,
		StartedWG:     &startedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	configureProposerBuilder(controller)

	commWG.Add(3) // state request
	controller.Start(1, 0, 0, true)
	commWG.Wait()

	commWG.Add(6) // propose
	controller.ViewChanged(2, 0)
	commWG.Wait()
	batcher.AssertNumberOfCalls(t, "NextBatch", 1)
	assembler.AssertNumberOfCalls(t, "AssembleProposal", 1)
	comm.AssertNumberOfCalls(t, "SendConsensus", 9)
	leaderMon.AssertCalled(t, "HeartbeatWasSent")
	controller.Stop()
	collector.Stop()
	wal.Close()
}

func TestSyncPrevView(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	pool.On("Prune", mock.Anything)
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("InjectArtificialHeartbeat", mock.Anything, mock.Anything)
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	signer := &mocks.SignerMock{}
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    4,
		Value: []byte{4},
	})
	fd := &mocks.FailureDetector{}
	fd.On("Complain", mock.Anything, mock.Anything)
	synchronizer := &mocks.SynchronizerMock{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 0,
				ViewId:         0, // previous view number
			}),
			VerificationSequence: 1,
		},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	collector := bft.StateCollector{
		SelfID:         0,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()
	defer collector.Stop()

	controller := &bft.Controller{
		Collector:       &collector,
		InFlight:        &bft.InFlightData{},
		Batcher:         batcher,
		RequestPool:     pool,
		LeaderMonitor:   leaderMon,
		ID:              4, // not the leader
		N:               4,
		NodesList:       []uint64{1, 2, 3, 4},
		Logger:          log,
		Application:     app,
		Comm:            comm,
		ViewChanger:     &bft.ViewChanger{},
		Checkpoint:      &types.Checkpoint{},
		FailureDetector: fd,
		Synchronizer:    synchronizer,
		Verifier:        verifier,
		Signer:          signer,
		WAL:             wal,
		StartedWG:       &startedWG,
		MetricsView:     api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	leaderMonWG.Add(1)
	controller.Start(1, 0, 0, false)
	leaderMonWG.Wait()

	appWG.Add(1)
	controller.ProcessMessages(2, prePrepare)
	controller.ProcessMessages(2, prepare)
	controller.ProcessMessages(3, prepare)
	controller.ProcessMessages(2, commit2)
	controller.ProcessMessages(3, commit3)

	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 1)

	synchronizerWG.Add(1)
	leaderMonWG.Add(1)
	wrongViewMsg := proto.Clone(prePrepare).(*protos.Message)
	wrongViewMsgGet := wrongViewMsg.GetPrePrepare()
	wrongViewMsgGet.View = 2
	controller.ProcessMessages(2, wrongViewMsg)
	synchronizerWG.Wait()
	leaderMonWG.Wait() // wait for view to start before sending messages

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
	})
	controller.ProcessMessages(2, prePrepareNext)

	nextProp := types.Proposal{
		Header:               prePrepareNextGet.Proposal.Header,
		Payload:              prePrepareNextGet.Proposal.Payload,
		Metadata:             prePrepareNextGet.Proposal.Metadata,
		VerificationSequence: 1,
	}
	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	controller.ProcessMessages(2, prepareNext)
	controller.ProcessMessages(3, prepareNext)

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = nextProp.Digest()

	commit3Next := proto.Clone(commit3).(*protos.Message)
	commit3NextGet := commit3Next.GetCommit()
	commit3NextGet.Seq = 1
	commit3NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	controller.ProcessMessages(2, commit2Next)
	controller.ProcessMessages(3, commit3Next)

	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 2)

	controller.Stop()
	wal.Close()
}

func TestSyncPrevViewAnd2Twice(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	pool := &mocks.RequestPool{}
	pool.On("Close")
	pool.On("Prune", mock.Anything)
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("InjectArtificialHeartbeat", mock.Anything, mock.Anything)
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	leaderMon.On("Close")
	comm := &mocks.CommMock{}
	comm.On("SendConsensus", mock.Anything, mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	signer := &mocks.SignerMock{}
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    4,
		Value: []byte{4},
	})
	fd := &mocks.FailureDetector{}
	fd.On("Complain", mock.Anything, mock.Anything)
	var numCall int
	synchronizer := &mocks.SynchronizerMock{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		numCall++
		if numCall == 1 {
			synchronizerWG.Done()
		}
		if numCall == 2 {
			appWG.Done()
		}
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 1,
				ViewId:         0, // previous view number
			}),
			VerificationSequence: 1,
		},
		Signatures: nil,
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	collector := bft.StateCollector{
		SelfID:         0,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()
	defer collector.Stop()

	controller := &bft.Controller{
		Collector:       &collector,
		InFlight:        &bft.InFlightData{},
		Batcher:         batcher,
		RequestPool:     pool,
		LeaderMonitor:   leaderMon,
		ID:              4, // not the leader
		N:               4,
		NodesList:       []uint64{1, 2, 3, 4},
		Logger:          log,
		Application:     app,
		Comm:            comm,
		ViewChanger:     &bft.ViewChanger{},
		Checkpoint:      &types.Checkpoint{},
		FailureDetector: fd,
		Synchronizer:    synchronizer,
		Verifier:        verifier,
		Signer:          signer,
		WAL:             wal,
		StartedWG:       &startedWG,
		MetricsView:     api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	leaderMonWG.Add(1)
	controller.Start(1, 1, 0, false)
	leaderMonWG.Wait()

	synchronizerWG.Add(1)
	leaderMonWG.Add(1)
	wrongViewMsg := proto.Clone(prePrepare).(*protos.Message)
	wrongViewMsgGet := wrongViewMsg.GetPrePrepare()
	wrongViewMsgGet.View = 2
	controller.ProcessMessages(2, wrongViewMsg)
	synchronizerWG.Wait()
	leaderMonWG.Wait() // wait for view to start before sending messages

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 0,
		LatestSequence:  1,
		ViewId:          1,
	})
	controller.ProcessMessages(2, prePrepareNext)

	nextProp := types.Proposal{
		Header:               prePrepareNextGet.Proposal.Header,
		Payload:              prePrepareNextGet.Proposal.Payload,
		Metadata:             prePrepareNextGet.Proposal.Metadata,
		VerificationSequence: 1,
	}
	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	controller.ProcessMessages(2, prepareNext)
	controller.ProcessMessages(3, prepareNext)

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = nextProp.Digest()

	commit3Next := proto.Clone(commit3).(*protos.Message)
	commit3NextGet := commit3Next.GetCommit()
	commit3NextGet.Seq = 1
	commit3NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	controller.ProcessMessages(2, commit2Next)
	controller.ProcessMessages(3, commit3Next)

	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 0)

	controller.Stop()
	wal.Close()
}

func TestControllerLeaderRequestHandling(t *testing.T) {
	for _, testCase := range []struct {
		description      string
		startViewNum     uint64
		verifyReqReturns error
		shouldEnqueue    bool
		shouldVerify     bool
		waitForLoggedMsg string
	}{
		{
			description:      "not the leader",
			startViewNum:     2,
			waitForLoggedMsg: "Got request from 3 but the leader is 2, dropping request",
		},
		{
			description:      "bad request",
			startViewNum:     1,
			verifyReqReturns: errors.New("unauthorized user"),
			waitForLoggedMsg: "unauthorized user",
			shouldVerify:     true,
		},
		{
			description:      "good request",
			shouldEnqueue:    true,
			startViewNum:     1,
			waitForLoggedMsg: "Got request from 3",
			shouldVerify:     true,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			var submittedToPool sync.WaitGroup

			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)

			log := basicLog.Sugar()

			batcher := &mocks.Batcher{}
			batcher.On("Close")
			batcher.On("Reset")
			batcher.On("NextBatch").Run(func(arguments mock.Arguments) {
				time.Sleep(time.Hour)
			})

			pool := &mocks.RequestPool{}
			pool.On("Close")
			leaderMon := &mocks.LeaderMonitor{}
			leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything)
			leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
			leaderMon.On("Close")
			if testCase.shouldEnqueue {
				submittedToPool.Add(1)
				pool.On("Submit", mock.Anything).Return(nil).Run(func(_ mock.Arguments) {
					submittedToPool.Done()
				})
			}

			commMock := &mocks.CommMock{}
			commMock.On("SendConsensus", mock.Anything, mock.Anything)

			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyRequest", mock.Anything).Return(types.RequestInfo{}, testCase.verifyReqReturns)
			verifier.On("VerificationSequence").Return(uint64(0))

			synchronizer := &mocks.SynchronizerMock{}
			synchronizer.On("Sync").Run(func(args mock.Arguments) {}).Return(types.SyncResponse{Latest: types.Decision{
				Proposal:   types.Proposal{VerificationSequence: 0},
				Signatures: nil,
			}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

			collector := bft.StateCollector{
				SelfID:         0,
				N:              4,
				Logger:         log,
				CollectTimeout: 100 * time.Millisecond,
			}
			collector.Start()
			defer collector.Stop()

			startedWG := sync.WaitGroup{}
			startedWG.Add(1)

			controller := &bft.Controller{
				InFlight:      &bft.InFlightData{},
				Checkpoint:    &types.Checkpoint{},
				RequestPool:   pool,
				LeaderMonitor: leaderMon,
				ID:            1,
				N:             4,
				NodesList:     []uint64{0, 1, 2, 3},
				Logger:        log,
				Batcher:       batcher,
				Comm:          commMock,
				Verifier:      verifier,
				Synchronizer:  synchronizer,
				Collector:     &collector,
				StartedWG:     &startedWG,
			}
			controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}

			configureProposerBuilder(controller)
			controller.Start(testCase.startViewNum, 0, 0, true)

			controller.HandleRequest(3, []byte{1, 2, 3})

			submittedToPool.Wait()

			if !testCase.shouldVerify {
				verifier.AssertNotCalled(t, "VerifyRequest", mock.Anything)
			}
		})
	}
}

func createView(c *bft.Controller, leader, proposalSequence, viewNum, decisionsInView uint64, quorumSize int, vs *atomic.Value) *bft.View {
	mn := &mocks.MembershipNotifierMock{}
	mn.On("MembershipChange").Return(false)
	return &bft.View{
		RetrieveCheckpoint: c.Checkpoint.Get,
		N:                  c.N,
		LeaderID:           leader,
		SelfID:             c.ID,
		Quorum:             quorumSize,
		Number:             viewNum,
		Decider:            c,
		FailureDetector:    c.FailureDetector,
		Sync:               c,
		Logger:             c.Logger,
		Comm:               c,
		Verifier:           c.Verifier,
		Signer:             c.Signer,
		MembershipNotifier: mn,
		ProposalSequence:   proposalSequence,
		DecisionsInView:    decisionsInView,
		ViewSequences:      vs,
		State:              &bft.PersistedState{WAL: c.WAL, InFlightProposal: &bft.InFlightData{}},
		InMsgQSize:         int(c.N * 10),
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
		MetricsBlacklist:   api.NewMetricsBlacklist(&disabled.Provider{}),
	}
}

func configureProposerBuilder(controller *bft.Controller) *atomic.Value {
	pb := &mocks.ProposerBuilder{}
	vs := &atomic.Value{}
	pb.On("NewProposer", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(func(a uint64, b uint64, c uint64, d uint64, e int) bft.Proposer {
			return createView(controller, a, b, c, d, e, vs)
		})
	controller.ProposerBuilder = pb
	return vs
}

func TestSyncInform(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("Reset")
	batcher.On("NextBatch").Return([][]byte{req})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1},
	}))
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1},
	}), nil)

	secondProposal := proposal
	secondProposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  2,
		ViewId:          2,
	})

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(secondProposal).Once()

	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})

	commWithChan := &mocks.CommMock{}
	msgChan := make(chan *protos.Message)
	commWithChan.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msgChan <- args.Get(0).(*protos.Message)
	})
	commWithChan.On("Nodes").Return([]uint64{0, 1, 2, 3})

	reqPool := &mocks.RequestPool{}
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything)
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("Close")

	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)

	synchronizer := &mocks.SynchronizerMock{}
	synchronizerWG := sync.WaitGroup{}
	syncToView := uint64(2)
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	}).Return(types.SyncResponse{Latest: types.Decision{
		Proposal: types.Proposal{
			Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence: 1,
				ViewId:         syncToView,
			}),
			VerificationSequence: 1,
		},
		Signatures: []types.Signature{
			{ID: 1}, {ID: 2}, {ID: 3},
		},
	}, Reconfig: types.ReconfigSync{InReplicatedDecisions: false}})

	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	controllerMock := &mocks.ViewController{}
	controllerMock.On("AbortView", mock.Anything)

	collector := bft.StateCollector{
		SelfID:         0,
		N:              4,
		Logger:         log,
		CollectTimeout: 100 * time.Millisecond,
	}
	collector.Start()

	vc := &bft.ViewChanger{
		SelfID:              2,
		N:                   4,
		NodesList:           []uint64{0, 1, 2, 3},
		Logger:              log,
		Comm:                commWithChan,
		RequestsTimer:       reqTimer,
		Ticker:              make(chan time.Time),
		Controller:          controllerMock,
		InMsqQSize:          100,
		ControllerStartedWG: sync.WaitGroup{},
		MetricsViewChange:   api.NewMetricsViewChange(&disabled.Provider{}),
	}

	vc.ControllerStartedWG.Add(1)

	controller := &bft.Controller{
		InFlight:      &bft.InFlightData{},
		Signer:        signer,
		WAL:           wal,
		ID:            2,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Logger:        log,
		Batcher:       batcher,
		Verifier:      verifier,
		Assembler:     assembler,
		Comm:          comm,
		RequestPool:   reqPool,
		LeaderMonitor: leaderMon,
		Synchronizer:  synchronizer,
		ViewChanger:   vc,
		Checkpoint:    &types.Checkpoint{},
		Collector:     &collector,
		StartedWG:     &vc.ControllerStartedWG,
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}
	configureProposerBuilder(controller)

	controller.Checkpoint.Set(proposal, []types.Signature{{ID: 1}, {ID: 2}, {ID: 3}})

	vc.Start(1)

	synchronizerWG.Add(1)
	commWG.Add(9)
	controller.Start(1, 0, 0, true)
	synchronizerWG.Wait()
	commWG.Wait()

	vc.StartViewChange(2, true)
	msg := <-msgChan
	assert.NotNil(t, msg.GetViewChange())
	assert.Equal(t, syncToView+1, msg.GetViewChange().NextView) // view number did change according to info

	batcher.AssertNumberOfCalls(t, "NextBatch", 1)
	assembler.AssertNumberOfCalls(t, "AssembleProposal", 1)
	comm.AssertNumberOfCalls(t, "SendConsensus", 9)
	leaderMon.AssertCalled(t, "HeartbeatWasSent")

	controller.Stop()
	vc.Stop()
	collector.Stop()
	wal.Close()
}

func TestRotateFromLeaderToFollower(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)
	defer wal.Close()

	var restartTimersWG sync.WaitGroup
	restartTimersWG.Add(2)
	reqPool := &mocks.RequestPool{}
	reqPool.On("RestartTimers").Run(func(args mock.Arguments) {
		restartTimersWG.Done()
	})
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	leaderMon := &mocks.LeaderMonitor{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything)
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("InjectArtificialHeartbeat", uint64(3), mock.Anything)
	leaderMon.On("Close")
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Closed").Return(false)
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("PopRemainder").Return([][]byte{})
	batcher.On("BatchRemainder", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1, 3},
	}))

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(proposal, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    2,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:           &bft.InFlightData{},
		RequestPool:        reqPool,
		LeaderMonitor:      leaderMon,
		WAL:                wal,
		ID:                 2, // the first leader
		N:                  4,
		NodesList:          []uint64{1, 2, 3, 4},
		Logger:             log,
		Batcher:            batcher,
		Verifier:           verifier,
		Assembler:          assembler,
		Comm:               comm,
		Signer:             signer,
		Application:        app,
		Checkpoint:         &types.Checkpoint{},
		ViewChanger:        &bft.ViewChanger{},
		StartedWG:          &startedWG,
		LeaderRotation:     true,
		DecisionsPerLeader: 1,
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}
	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	commWG.Add(6)
	controller.Start(1, 0, 0, false)
	commWG.Wait() // propose (pre-prepare + prepare)

	commWG.Add(3)
	controller.ProcessMessages(3, prepare)
	controller.ProcessMessages(4, prepare)
	commWG.Wait() // commit

	controller.ProcessMessages(1, commit1)
	appWG.Add(1)       // deliver
	leaderMonWG.Add(1) // change role
	controller.ProcessMessages(3, commit3)
	appWG.Wait()
	commWG.Wait()
	leaderMonWG.Wait()

	// leader rotation (now 3 is the leader)

	prePrepareNext := proto.Clone(prePrepare).(*protos.Message)
	prePrepareNextGet := prePrepareNext.GetPrePrepare()
	prePrepareNextGet.Seq = 1
	prePrepareNextGet.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
	})
	commWG.Add(3) // sending prepare
	controller.ProcessMessages(3, prePrepareNext)
	commWG.Wait()

	nextProp := types.Proposal{
		Header:               prePrepareNextGet.Proposal.Header,
		Payload:              prePrepareNextGet.Proposal.Payload,
		Metadata:             prePrepareNextGet.Proposal.Metadata,
		VerificationSequence: 1,
	}
	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	commWG.Add(3) // sending commit
	controller.ProcessMessages(3, prepareNext)
	controller.ProcessMessages(4, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1
	commit1NextGet.Digest = nextProp.Digest()

	commit3Next := proto.Clone(commit3).(*protos.Message)
	commit3NextGet := commit3Next.GetCommit()
	commit3NextGet.Seq = 1
	commit3NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	leaderMonWG.Add(1)
	controller.ProcessMessages(1, commit1Next)
	controller.ProcessMessages(3, commit3Next)
	leaderMonWG.Wait()
	restartTimersWG.Wait()
	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 2)

	controller.Stop()
}

func TestRotateFromFollowerToLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	testDir, err := os.MkdirTemp("", "controller-unittest")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)
	wal, err := wal.Create(log, testDir, nil)
	assert.NoError(t, err)
	defer wal.Close()

	var restartTimersWG sync.WaitGroup
	restartTimersWG.Add(2)
	reqPool := &mocks.RequestPool{}
	reqPool.On("Prune", mock.Anything)
	reqPool.On("Close")
	reqPool.On("RestartTimers").Run(func(args mock.Arguments) {
		restartTimersWG.Done()
	})
	leaderMon := &mocks.LeaderMonitor{}
	leaderMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Leader, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		leaderMonWG.Done()
	})
	followerMonWG := sync.WaitGroup{}
	leaderMon.On("ChangeRole", bft.Follower, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		followerMonWG.Done()
	})
	leaderMon.On("HeartbeatWasSent")
	leaderMon.On("InjectArtificialHeartbeat", uint64(2), mock.Anything)
	leaderMon.On("Close")
	req := []byte{1}
	batcher := &mocks.Batcher{}
	batcher.On("Close")
	batcher.On("Reset")
	batcher.On("Closed").Return(false)
	batcher.On("NextBatch").Return([][]byte{req}).Once()
	batcher.On("PopRemainder").Return([][]byte{})
	batcher.On("BatchRemainder", mock.Anything)
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyRequest", req).Return(types.RequestInfo{}, nil)
	verifier.On("VerificationSequence").Return(uint64(1))
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("AuxiliaryData", mock.Anything).Return(bft.MarshalOrPanic(&protos.PreparesFrom{
		Ids: []uint64{1, 3},
	}))

	nextMD := bft.MarshalOrPanic(&protos.ViewMetadata{
		DecisionsInView: 1,
		LatestSequence:  1,
		ViewId:          1,
		PrevCommitSignatureDigest: bft.CommitSignaturesDigest([]*protos.Signature{
			commit1.GetCommit().Signature,
			commit2.GetCommit().Signature,
			commit3.GetCommit().Signature,
		}),
	})
	nextProp := types.Proposal{
		Header:               proposal.Header,
		Payload:              proposal.Payload,
		Metadata:             nextMD,
		VerificationSequence: 1,
	}

	assembler := &mocks.AssemblerMock{}
	assembler.On("AssembleProposal", mock.Anything, [][]byte{req}).Return(nextProp, [][]byte{}).Once()
	comm := &mocks.CommMock{}
	commWG := sync.WaitGroup{}
	comm.On("Nodes").Return([]uint64{1, 2, 3, 4})
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		commWG.Done()
	})
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return(nil)
	signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{
		ID:    3,
		Value: []byte{4},
	})
	app := &mocks.ApplicationMock{}
	appWG := sync.WaitGroup{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		appWG.Done()
	}).Return(types.Reconfig{InLatestDecision: false})

	startedWG := sync.WaitGroup{}
	startedWG.Add(1)

	controller := &bft.Controller{
		InFlight:           &bft.InFlightData{},
		RequestPool:        reqPool,
		LeaderMonitor:      leaderMon,
		WAL:                wal,
		ID:                 3, // the second leader
		N:                  4,
		NodesList:          []uint64{1, 2, 3, 4},
		Logger:             log,
		Batcher:            batcher,
		Verifier:           verifier,
		Assembler:          assembler,
		Comm:               comm,
		Signer:             signer,
		Application:        app,
		Checkpoint:         &types.Checkpoint{},
		ViewChanger:        &bft.ViewChanger{},
		StartedWG:          &startedWG,
		LeaderRotation:     true,
		DecisionsPerLeader: 1,
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
	}
	controller.Deliver = &bft.MutuallyExclusiveDeliver{C: controller}
	vs := configureProposerBuilder(controller)
	controller.ViewSequences = vs

	followerMonWG.Add(1) // change role
	controller.Start(1, 0, 0, false)
	followerMonWG.Wait()

	commWG.Add(3)
	controller.ProcessMessages(2, prePrepare)
	commWG.Wait() // prepare

	commWG.Add(3)
	controller.ProcessMessages(1, prepare)
	controller.ProcessMessages(4, prepare)
	commWG.Wait() // commit

	controller.ProcessMessages(1, commit1)
	time.Sleep(time.Second)
	appWG.Add(1)       // deliver
	leaderMonWG.Add(1) // change role
	commWG.Add(6)      // propose + prepare
	controller.ProcessMessages(2, commit2)
	appWG.Wait()
	leaderMonWG.Wait()
	commWG.Wait()

	// leader rotation (now 3 is the leader)

	prepareNext := proto.Clone(prepare).(*protos.Message)
	prepareNextGet := prepareNext.GetPrepare()
	prepareNextGet.Seq = 1
	prepareNextGet.Digest = nextProp.Digest()
	commWG.Add(3) // sending commit
	controller.ProcessMessages(1, prepareNext)
	controller.ProcessMessages(4, prepareNext)
	commWG.Wait()

	commit1Next := proto.Clone(commit1).(*protos.Message)
	commit1NextGet := commit1Next.GetCommit()
	commit1NextGet.Seq = 1
	commit1NextGet.Digest = nextProp.Digest()

	commit2Next := proto.Clone(commit2).(*protos.Message)
	commit2NextGet := commit2Next.GetCommit()
	commit2NextGet.Seq = 1
	commit2NextGet.Digest = nextProp.Digest()

	appWG.Add(1)
	followerMonWG.Add(1)
	controller.ProcessMessages(1, commit1Next)
	controller.ProcessMessages(2, commit2Next)
	followerMonWG.Wait()
	restartTimersWG.Wait()
	appWG.Wait()
	app.AssertNumberOfCalls(t, "Deliver", 2)

	controller.Stop()
}
