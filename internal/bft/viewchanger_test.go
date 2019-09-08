// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	viewChangeMsg = &protos.Message{
		Content: &protos.Message_ViewChange{
			ViewChange: &protos.ViewChange{
				NextView: 1,
				Reason:   "",
			},
		},
	}
	metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 1,
		ViewId:         0,
	})
	lastDecision = types.Proposal{
		Header:               []byte{0},
		Payload:              []byte{1},
		Metadata:             metadata,
		VerificationSequence: 1,
	}
	lastDecisionSignatures       = []types.Signature{{Id: 0, Value: []byte{4}, Msg: []byte{5}}, {Id: 1, Value: []byte{4}, Msg: []byte{5}}, {Id: 2, Value: []byte{4}, Msg: []byte{5}}}
	lastDecisionSignaturesProtos = []*protos.Signature{{Signer: 0, Value: []byte{4}, Msg: []byte{5}}, {Signer: 1, Value: []byte{4}, Msg: []byte{5}}, {Signer: 2, Value: []byte{4}, Msg: []byte{5}}}
	vd                           = &protos.ViewData{
		NextView: 1,
		LastDecision: &protos.Proposal{
			Header:               lastDecision.Header,
			Payload:              lastDecision.Payload,
			Metadata:             lastDecision.Metadata,
			VerificationSequence: uint64(lastDecision.VerificationSequence),
		},
		LastDecisionSignatures: lastDecisionSignaturesProtos,
	}
	vdBytes      = bft.MarshalOrPanic(vd)
	viewDataMsg1 = &protos.Message{
		Content: &protos.Message_ViewData{
			ViewData: &protos.SignedViewData{
				RawViewData: vdBytes,
				Signer:      0,
				Signature:   nil,
			},
		},
	}
)

func TestViewChangerBasic(t *testing.T) {
	// A simple test that starts a viewChanger and stops it

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})

	vc := &bft.ViewChanger{
		N:      4,
		Comm:   comm,
		Ticker: make(chan time.Time),
	}

	vc.Start(0)

	vc.Stop()
	vc.Stop()
}

func TestStartViewChange(t *testing.T) {
	// Test that when StartViewChange is called it broadcasts a message

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msgChan <- args.Get(0).(*protos.Message)
	})
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Once()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	controller := &mocks.ViewController{}
	controller.On("AbortView")

	vc := &bft.ViewChanger{
		N:             4,
		Comm:          comm,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		Logger:        log,
		Controller:    controller,
	}

	vc.Start(0)

	vc.StartViewChange(0, true)
	msg := <-msgChan
	assert.NotNil(t, msg.GetViewChange())

	vc.Stop()

	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
	controller.AssertNumberOfCalls(t, "AbortView", 1)
}

func TestViewChangeProcess(t *testing.T) {
	// Test the view change messages handling and process until sending a viewData message

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	broadcastChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(*protos.Message)
		broadcastChan <- m
	}).Twice()
	sendChan := make(chan *protos.Message)
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(1).(*protos.Message)
		sendChan <- m
	}).Twice()
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	controller := &mocks.ViewController{}
	controller.On("AbortView")

	vc := &bft.ViewChanger{
		SelfID:        0,
		N:             4,
		Comm:          comm,
		Signer:        signer,
		Logger:        log,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &types.Checkpoint{},
		Controller:    controller,
	}

	vc.Start(0)

	vc.HandleMessage(1, viewChangeMsg)
	vc.HandleMessage(2, viewChangeMsg)
	m := <-broadcastChan
	assert.NotNil(t, m.GetViewChange())
	m = <-sendChan
	assert.NotNil(t, m.GetViewData())
	comm.AssertCalled(t, "SendConsensus", uint64(1), mock.Anything)

	// sending viewChange messages with same view doesn't make a difference
	vc.HandleMessage(1, viewChangeMsg)
	vc.HandleMessage(2, viewChangeMsg)

	// sending viewChange messages with bigger view doesn't make a difference
	msg3 := proto.Clone(viewChangeMsg).(*protos.Message)
	msg3.GetViewChange().NextView = 3
	vc.HandleMessage(2, msg3)
	vc.HandleMessage(1, msg3)

	// sending viewChange messages with the next view
	msg2 := proto.Clone(viewChangeMsg).(*protos.Message)
	msg2.GetViewChange().NextView = 2
	vc.HandleMessage(2, msg2)
	vc.HandleMessage(3, msg2)
	m = <-broadcastChan
	assert.NotNil(t, m.GetViewChange())
	m = <-sendChan
	assert.NotNil(t, m.GetViewData())
	comm.AssertCalled(t, "SendConsensus", uint64(2), mock.Anything)

	reqTimer.AssertNumberOfCalls(t, "StopTimers", 2)
	controller.AssertNumberOfCalls(t, "AbortView", 2)

	vc.Stop()
}

func TestViewDataProcess(t *testing.T) {
	// Test the view data messages handling and process until sending a newView message

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	broadcastChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(*protos.Message)
		broadcastChan <- m
	}).Once()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	verifier := &mocks.VerifierMock{}
	verifierSigWG := sync.WaitGroup{}
	verifier.On("VerifySignature", mock.Anything).Run(func(args mock.Arguments) {
		verifierSigWG.Done()
	}).Return(nil)
	verifierConsenterSigWG := sync.WaitGroup{}
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		verifierConsenterSigWG.Done()
	}).Return(nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("RestartTimers")

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Ticker:        make(chan time.Time),
		Checkpoint:    &checkpoint,
		RequestsTimer: reqTimer,
	}

	vc.Start(1)

	verifierSigWG.Add(1)
	verifierConsenterSigWG.Add(3)
	vc.HandleMessage(0, viewDataMsg1)
	verifierSigWG.Wait()
	verifierConsenterSigWG.Wait()

	msg1 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg1.GetViewData().Signer = 1

	verifierSigWG.Add(1)
	verifierConsenterSigWG.Add(3)
	vc.HandleMessage(1, msg1)
	verifierSigWG.Wait()
	verifierConsenterSigWG.Wait()

	msg2 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg2.GetViewData().Signer = 2

	verifierSigWG.Add(4)
	verifierConsenterSigWG.Add(12)
	vc.HandleMessage(2, msg2)
	m := <-broadcastChan
	assert.NotNil(t, m.GetNewView())
	verifierSigWG.Wait()
	verifierConsenterSigWG.Wait()
	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(2), num)
	reqTimer.AssertCalled(t, "RestartTimers")

	vc.Stop()
}

func TestNewViewProcess(t *testing.T) {
	// Test the new view messages handling and process until calling controller

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	verifier := &mocks.VerifierMock{}
	verifierSigWG := sync.WaitGroup{}
	verifier.On("VerifySignature", mock.Anything).Run(func(args mock.Arguments) {
		verifierSigWG.Done()
	}).Return(nil)
	verifierConsenterSigWG := sync.WaitGroup{}
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		verifierConsenterSigWG.Done()
	}).Return(nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("RestartTimers")

	vc := &bft.ViewChanger{
		SelfID:        0,
		N:             4,
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Ticker:        make(chan time.Time),
		Checkpoint:    &checkpoint,
		RequestsTimer: reqTimer,
	}

	vc.Start(2)

	// create a valid viewData message
	vd2 := proto.Clone(vd).(*protos.ViewData)
	vd2.NextView = 2

	vdBytes := bft.MarshalOrPanic(vd2)
	signed := make([]*protos.SignedViewData, 0)
	for len(signed) < 3 { // quorum = 3
		msg := &protos.Message{
			Content: &protos.Message_ViewData{
				ViewData: &protos.SignedViewData{
					RawViewData: vdBytes,
					Signer:      uint64(len(signed)),
					Signature:   nil,
				},
			},
		}
		signed = append(signed, msg.GetViewData())
	}
	msg := &protos.Message{
		Content: &protos.Message_NewView{
			NewView: &protos.NewView{
				SignedViewData: signed,
			},
		},
	}

	verifierSigWG.Add(3)
	verifierConsenterSigWG.Add(9)
	vc.HandleMessage(2, msg)
	verifierSigWG.Wait()
	verifierConsenterSigWG.Wait()
	num := <-viewNumChan
	assert.Equal(t, uint64(2), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(2), num)
	reqTimer.AssertCalled(t, "RestartTimers")

	vc.Stop()
}

func TestNormalProcess(t *testing.T) {
	// Test a full view change process

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(*protos.Message)
		msgChan <- m
	})
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	controller.On("AbortView")
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Signer:        signer,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &checkpoint,
	}

	vc.Start(0)

	vc.HandleMessage(2, viewChangeMsg)
	vc.HandleMessage(3, viewChangeMsg)
	m := <-msgChan
	assert.NotNil(t, m.GetViewChange())

	vc.HandleMessage(0, viewDataMsg1)
	msg2 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg2.GetViewData().Signer = 2
	vc.HandleMessage(2, msg2)
	m = <-msgChan
	assert.NotNil(t, m.GetNewView())

	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(2), num)

	controller.AssertNumberOfCalls(t, "AbortView", 1)

	vc.Stop()
}

func TestBadViewDataMessage(t *testing.T) {
	// Test that bad view data messages don't cause a view change

	for _, test := range []struct {
		description           string
		mutateViewData        func(*protos.Message)
		mutateVerifySig       func(*mocks.VerifierMock)
		expectedMessageLogged string
	}{
		{
			description:           "wrong signer",
			expectedMessageLogged: "is not the sender",
			mutateViewData: func(m *protos.Message) {
				m.GetViewData().Signer = 10
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
		{
			description:           "invalid signature",
			expectedMessageLogged: "but signature is invalid",
			mutateViewData: func(m *protos.Message) {
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
				verifierMock.On("VerifySignature", mock.Anything).Return(errors.New(""))
			},
		},
		{
			description:           "wrong view",
			expectedMessageLogged: "is in view",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView: 10,
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
		{
			description:           "wrong leader",
			expectedMessageLogged: "is not the next leader",
			mutateViewData: func(m *protos.Message) {
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			var warningMsgLogged sync.WaitGroup
			warningMsgLogged.Add(1)
			log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, test.expectedMessageLogged) {
					warningMsgLogged.Done()
				}
				return nil
			})).Sugar()
			comm := &mocks.CommMock{}
			comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
			verifier := &mocks.VerifierMock{}
			test.mutateVerifySig(verifier)
			verifier.On("VerifySignature", mock.Anything).Return(nil)
			vc := &bft.ViewChanger{
				SelfID:   2,
				N:        4,
				Comm:     comm,
				Logger:   log,
				Verifier: verifier,
				Ticker:   make(chan time.Time),
			}

			vc.Start(1)

			msg := proto.Clone(viewDataMsg1).(*protos.Message)
			test.mutateViewData(msg)

			vc.HandleMessage(0, msg)
			warningMsgLogged.Wait()

			vc.Stop()
		})

	}
}

func TestResendViewChangeMessage(t *testing.T) {

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msgChan <- args.Get(0).(*protos.Message)
	})
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Once()
	ticker := make(chan time.Time)
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	controller := &mocks.ViewController{}
	controller.On("AbortView")

	vc := &bft.ViewChanger{
		N:                 4,
		Comm:              comm,
		RequestsTimer:     reqTimer,
		Ticker:            ticker,
		Logger:            log,
		Controller:        controller,
		ResendTimeout:     time.Second,
		TimeoutViewChange: 10 * time.Second,
	}

	vc.Start(0)
	startTime := time.Now()

	vc.StartViewChange(0, true)
	m := <-msgChan
	assert.NotNil(t, m.GetViewChange())

	// no resend
	ticker <- startTime.Add(time.Millisecond)

	// resend
	ticker <- startTime.Add(2 * time.Second)
	m = <-msgChan
	assert.NotNil(t, m.GetViewChange())

	// resend again
	ticker <- startTime.Add(4 * time.Second)
	m = <-msgChan
	assert.NotNil(t, m.GetViewChange())

	vc.Stop()

	comm.AssertNumberOfCalls(t, "BroadcastConsensus", 3) // start view change and two resends
	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
	controller.AssertNumberOfCalls(t, "AbortView", 1)

}

func TestViewChangerTimeout(t *testing.T) {
	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	comm.On("BroadcastConsensus", mock.Anything)
	reqTimerWG := sync.WaitGroup{}
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Run(func(args mock.Arguments) {
		reqTimerWG.Done()
	})
	ticker := make(chan time.Time)
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	synchronizer := &mocks.Synchronizer{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	})
	controller := &mocks.ViewController{}
	controllerWG := sync.WaitGroup{}
	controller.On("AbortView").Run(func(args mock.Arguments) {
		controllerWG.Done()
	})

	vc := &bft.ViewChanger{
		N:                 4,
		Comm:              comm,
		RequestsTimer:     reqTimer,
		Ticker:            ticker,
		Logger:            log,
		TimeoutViewChange: 10 * time.Second,
		ResendTimeout:     20 * time.Second,
		Synchronizer:      synchronizer,
		Controller:        controller,
	}

	vc.Start(0)
	startTime := time.Now()

	controllerWG.Add(1)
	reqTimerWG.Add(1)
	vc.StartViewChange(0, true) // start timer
	controllerWG.Wait()
	reqTimerWG.Wait()

	reqTimerWG.Add(1)
	synchronizerWG.Add(1)
	ticker <- startTime.Add(12 * time.Second) // timeout
	synchronizerWG.Wait()
	reqTimerWG.Wait()

	vc.Stop()

	synchronizer.AssertNumberOfCalls(t, "Sync", 1)
	reqTimer.AssertNumberOfCalls(t, "StopTimers", 2)
	controller.AssertNumberOfCalls(t, "AbortView", 1)
	comm.AssertNumberOfCalls(t, "BroadcastConsensus", 2)
}

func TestCommitLastDecision(t *testing.T) {

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(*protos.Message)
		msgChan <- m
	})
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil)
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	controller.On("AbortView")
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything)

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Signer:        signer,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &checkpoint,
		Application:   app,
	}

	vc.Start(0)

	vc.HandleMessage(2, viewChangeMsg)
	vc.HandleMessage(3, viewChangeMsg)
	m := <-msgChan
	assert.NotNil(t, m.GetViewChange())

	nextViewData := proto.Clone(vd).(*protos.ViewData)
	nextViewData.LastDecision.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 2,
		ViewId:         0,
	})
	nextViewDateBytes := bft.MarshalOrPanic(nextViewData)
	viewData := proto.Clone(viewDataMsg1).(*protos.Message)
	viewData.GetViewData().RawViewData = nextViewDateBytes

	vc.HandleMessage(0, viewData)
	msg2 := proto.Clone(viewData).(*protos.Message)
	msg2.GetViewData().Signer = 2
	vc.HandleMessage(2, msg2)
	m = <-msgChan
	assert.NotNil(t, m.GetNewView())

	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(3), num)

	controller.AssertNumberOfCalls(t, "AbortView", 1)
	app.AssertNumberOfCalls(t, "Deliver", 1)

	vc.Stop()

}

func TestFarBehindLastDecisionAndSync(t *testing.T) {
	// Scenario: a node gets a newView message with last decision with seq 3,
	// while its checkpoint shows that its last decision is only at seq 1,
	// the node calls sync to catch up and waits for it to come back (via inform),
	// the first sync informs on seq 2 and so sync is called again,
	// which informs on seq 3 as expected.

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	comm.On("BroadcastConsensus", mock.Anything)
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil)
	verifier.On("VerifyProposal", mock.Anything, mock.Anything).Return(nil, nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	controller.On("AbortView")
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything)
	synchronizer := &mocks.Synchronizer{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	})

	vc := &bft.ViewChanger{
		SelfID:        3,
		N:             4,
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Signer:        signer,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		InFlight:      &bft.InFlightData{},
		Application:   app,
		Checkpoint:    &checkpoint,
		Synchronizer:  synchronizer,
	}

	decisionAhead := types.Proposal{
		Payload: []byte{1},
		Header:  []byte{2},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 3,
			ViewId:         0,
		}),
		VerificationSequence: 1,
	}

	viewData := proto.Clone(vd).(*protos.ViewData)
	viewData.LastDecision = &protos.Proposal{
		Payload:              decisionAhead.Payload,
		Header:               decisionAhead.Header,
		Metadata:             decisionAhead.Metadata,
		VerificationSequence: uint64(decisionAhead.VerificationSequence),
	}
	viewData.InFlightPrepared = true

	vdBytes := bft.MarshalOrPanic(viewData)

	signed := make([]*protos.SignedViewData, 0)
	for len(signed) < 3 { // quorum = 3
		msg := &protos.Message{
			Content: &protos.Message_ViewData{
				ViewData: &protos.SignedViewData{
					RawViewData: vdBytes,
					Signer:      uint64(len(signed)),
					Signature:   nil,
				},
			},
		}
		signed = append(signed, msg.GetViewData())
	}

	msg := &protos.Message{
		Content: &protos.Message_NewView{
			NewView: &protos.NewView{
				SignedViewData: signed,
			},
		},
	}

	synchronizerWG.Add(2)

	vc.Start(1)

	vc.HandleMessage(1, msg)

	vc.InformNewView(0, 2) // lower sequence, will cause another sync

	synchronizerWG.Wait()

	vc.InformNewView(0, 3)

	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(4), num)

	vc.Stop()
}

func TestInFlightProposalInViewData(t *testing.T) {

	for _, test := range []struct {
		description    string
		getInFlight    func() *bft.InFlightData
		expectInflight bool
	}{
		{
			description: "in flight is nil",
			getInFlight: func() *bft.InFlightData {
				return &bft.InFlightData{}
			},
			expectInflight: false,
		},
		{
			description: "in flight same as last decision",
			getInFlight: func() *bft.InFlightData {
				inFlight := &bft.InFlightData{}
				inFlight.StoreProposal(lastDecision)
				return inFlight
			},
			expectInflight: false,
		},
		{
			description: "in flight is after last decision",
			getInFlight: func() *bft.InFlightData {
				inFlight := &bft.InFlightData{}
				proposal := lastDecision
				proposal.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence: 2,
					ViewId:         0,
				})
				inFlight.StoreProposal(proposal)
				return inFlight
			},
			expectInflight: true,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			comm := &mocks.CommMock{}
			comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
			broadcastChan := make(chan *protos.Message)
			comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
				m := args.Get(0).(*protos.Message)
				broadcastChan <- m
			}).Twice()
			sendChan := make(chan *protos.Message)
			comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				m := args.Get(1).(*protos.Message)
				sendChan <- m
			}).Twice()
			signer := &mocks.SignerMock{}
			signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
			basicLog, err := zap.NewDevelopment()
			assert.NoError(t, err)
			log := basicLog.Sugar()
			reqTimer := &mocks.RequestsTimer{}
			reqTimer.On("StopTimers")
			controller := &mocks.ViewController{}
			controller.On("AbortView")
			checkpoint := types.Checkpoint{}
			checkpoint.Set(lastDecision, lastDecisionSignatures)

			vc := &bft.ViewChanger{
				SelfID:        0,
				N:             4,
				Comm:          comm,
				Signer:        signer,
				Logger:        log,
				RequestsTimer: reqTimer,
				Ticker:        make(chan time.Time),
				InFlight:      test.getInFlight(),
				Checkpoint:    &checkpoint,
				Controller:    controller,
			}

			vc.Start(0)
			vc.HandleMessage(1, viewChangeMsg)
			vc.HandleMessage(2, viewChangeMsg)
			m := <-broadcastChan
			assert.NotNil(t, m.GetViewChange())
			m = <-sendChan
			assert.NotNil(t, m.GetViewData())
			viewData := &protos.ViewData{}
			assert.NoError(t, proto.Unmarshal(m.GetViewData().RawViewData, viewData))
			if test.expectInflight {
				assert.NotNil(t, viewData.InFlightProposal)
			} else {
				assert.Nil(t, viewData.InFlightProposal)
			}
			assert.NotNil(t, viewData.LastDecision)
			comm.AssertCalled(t, "SendConsensus", uint64(1), mock.Anything)

			vc.Stop()

			reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
			controller.AssertNumberOfCalls(t, "AbortView", 1)
		})
	}

}

func TestValidateLastDecision(t *testing.T) {

	for _, test := range []struct {
		description  string
		viewData     *protos.ViewData
		mutateVerify func(*mocks.VerifierMock)
		valid        bool
		sequence     uint64
	}{
		{
			description:  "last decision is not set",
			viewData:     &protos.ViewData{},
			mutateVerify: func(*mocks.VerifierMock) {},
		},
		{
			description: "last decision metadata is nil",
			viewData: &protos.ViewData{
				LastDecision: &protos.Proposal{},
			},
			mutateVerify: func(*mocks.VerifierMock) {},
			valid:        true,
		},
		{
			description: "unable to unmarshal last decision metadata",
			viewData: &protos.ViewData{
				LastDecision: &protos.Proposal{
					Metadata: []byte{0},
				},
			},
			mutateVerify: func(*mocks.VerifierMock) {},
		},
		{
			description: "last decision view is greater or equal to requested next view",
			viewData: &protos.ViewData{
				NextView: 1,
				LastDecision: &protos.Proposal{
					Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence: 1,
						ViewId:         1,
					}),
				},
			},
			mutateVerify: func(*mocks.VerifierMock) {},
		},
		{
			description: "not enough signatures",
			viewData: &protos.ViewData{
				NextView: 1,
				LastDecision: &protos.Proposal{
					Metadata: metadata,
				},
			},
			mutateVerify: func(*mocks.VerifierMock) {},
		},
		{
			description: "invalid signatures",
			viewData: &protos.ViewData{
				NextView: 1,
				LastDecision: &protos.Proposal{
					Metadata: metadata,
				},
				LastDecisionSignatures: lastDecisionSignaturesProtos,
			},
			mutateVerify: func(verifier *mocks.VerifierMock) {
				verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(errors.New(""))
			},
		},
		{
			description: "not enough valid signatures",
			viewData: &protos.ViewData{
				NextView: 1,
				LastDecision: &protos.Proposal{
					Metadata: metadata,
				},
				LastDecisionSignatures: []*protos.Signature{{Signer: 0, Value: []byte{4}, Msg: []byte{5}}, {Signer: 0, Value: []byte{4}, Msg: []byte{5}}, {Signer: 1, Value: []byte{4}, Msg: []byte{5}}},
			},
			mutateVerify: func(verifier *mocks.VerifierMock) {
				verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil)
			},
		},
		{
			description: "valid last decision",
			viewData: &protos.ViewData{
				NextView: 1,
				LastDecision: &protos.Proposal{
					Metadata: metadata,
				},
				LastDecisionSignatures: lastDecisionSignaturesProtos,
			},
			mutateVerify: func(verifier *mocks.VerifierMock) {
				verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil)
			},
			valid:    true,
			sequence: 1,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			verifier := &mocks.VerifierMock{}
			test.mutateVerify(verifier)
			err, seq := bft.ValidateLastDecision(test.viewData, 3, 4, verifier)
			if test.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			assert.Equal(t, test.sequence, seq)
		})
	}

}

func TestValidateInFlight(t *testing.T) {
	for _, test := range []struct {
		description      string
		inFlightProposal *protos.Proposal
		sequence         uint64
		valid            bool
	}{
		{
			description: "in flight proposal is not set",
			valid:       true,
		},
		{
			description:      "in flight proposal metadata is nil",
			inFlightProposal: &protos.Proposal{},
		},
		{
			description: "unable to unmarshal in flight proposal metadata",
			inFlightProposal: &protos.Proposal{
				Metadata: []byte{1},
			},
		},
		{
			description: "wrong in flight proposal sequence",
			inFlightProposal: &protos.Proposal{
				Metadata: metadata,
			},
			sequence: 1,
		},
		{
			description: "valid in flight proposal",
			inFlightProposal: &protos.Proposal{
				Metadata: metadata,
			},
			valid: true,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			err := bft.ValidateInFlight(test.inFlightProposal, test.sequence)
			if test.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestInformViewChanger(t *testing.T) {
	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msgChan <- args.Get(0).(*protos.Message)
	})
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Once()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	controller := &mocks.ViewController{}
	controller.On("AbortView")

	vc := &bft.ViewChanger{
		N:             4,
		Comm:          comm,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		Logger:        log,
		Controller:    controller,
	}

	vc.Start(0)

	info := uint64(2)
	vc.InformNewView(info, 1) // increase the view number
	vc.InformNewView(info, 1) // make sure that inform happened (channel size is 1)

	vc.StartViewChange(2, true)
	msg := <-msgChan
	assert.NotNil(t, msg.GetViewChange())
	assert.Equal(t, info+1, msg.GetViewChange().NextView) // view number did change according to info

	vc.Stop()

	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
	controller.AssertNumberOfCalls(t, "AbortView", 1)
}
