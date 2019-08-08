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

	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"

	"github.com/golang/protobuf/proto"

	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
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
	vd = &protos.ViewData{
		NextView: 1,
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
		N:            4,
		Comm:         comm,
		ResendTicker: make(chan time.Time),
	}

	vc.Start(0)

	vc.Stop()
	vc.Stop()
}

func TestStartViewChange(t *testing.T) {
	// Test that when StartViewChange is called it broadcasts a message and view numbers are correct

	comm := &mocks.CommMock{}
	comm.On("Nodes").Return([]uint64{0, 1, 2, 3})
	var msg *protos.Message
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msg = args.Get(0).(*protos.Message)
	})
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Once()

	vc := &bft.ViewChanger{
		N:             4,
		Comm:          comm,
		RequestsTimer: reqTimer,
		ResendTicker:  make(chan time.Time),
	}

	vc.Start(0)

	vc.StartViewChange()
	assert.NotNil(t, msg.GetViewChange())
	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)

	assert.Equal(t, uint64(0), vc.CurrView)
	assert.Equal(t, uint64(1), vc.NextView)

	vc.Stop()
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
	reqTimer.On("RestartTimers")

	vc := &bft.ViewChanger{
		SelfID:        0,
		N:             4,
		F:             1,
		Quorum:        3,
		Comm:          comm,
		Signer:        signer,
		Logger:        log,
		RequestsTimer: reqTimer,
		ResendTicker:  make(chan time.Time),
	}

	vc.Start(0)

	vc.HandleMessage(1, viewChangeMsg)
	vc.HandleMessage(2, viewChangeMsg)
	m := <-broadcastChan
	assert.NotNil(t, m.GetViewChange())
	m = <-sendChan
	assert.NotNil(t, m.GetViewData())
	comm.AssertCalled(t, "SendConsensus", uint64(1), mock.Anything)

	assert.Equal(t, uint64(1), vc.CurrView)
	assert.Equal(t, uint64(1), vc.Leader)

	// sending viewChange messages with same view doesn't make a difference
	vc.HandleMessage(1, viewChangeMsg)
	vc.HandleMessage(2, viewChangeMsg)
	assert.Equal(t, uint64(1), vc.CurrView)
	assert.Equal(t, uint64(1), vc.Leader)

	// sending viewChange messages with bigger view doesn't make a difference
	msg3 := proto.Clone(viewChangeMsg).(*protos.Message)
	msg3.GetViewChange().NextView = 3
	vc.HandleMessage(2, msg3)
	vc.HandleMessage(1, msg3)
	assert.Equal(t, uint64(1), vc.CurrView)
	assert.Equal(t, uint64(1), vc.Leader)

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

	assert.Equal(t, uint64(2), vc.CurrView)
	assert.Equal(t, uint64(2), vc.Leader)

	reqTimer.AssertNumberOfCalls(t, "StopTimers", 2)
	reqTimer.AssertNumberOfCalls(t, "RestartTimers", 2)

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
	verifierWG := sync.WaitGroup{}
	verifier.On("VerifySignature", mock.Anything).Run(func(args mock.Arguments) {
		verifierWG.Done()
	}).Return(nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
	}).Return(nil).Once()

	vc := &bft.ViewChanger{
		SelfID:       1,
		N:            4,
		F:            1,
		Quorum:       3,
		Comm:         comm,
		Logger:       log,
		Verifier:     verifier,
		Controller:   controller,
		ResendTicker: make(chan time.Time),
	}

	vc.Start(1)

	verifierWG.Add(1)
	vc.HandleMessage(0, viewDataMsg1)
	verifierWG.Wait()

	msg1 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg1.GetViewData().Signer = 1

	verifierWG.Add(1)
	vc.HandleMessage(1, msg1)
	verifierWG.Wait()

	msg2 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg2.GetViewData().Signer = 2

	verifierWG.Add(4)
	vc.HandleMessage(2, msg2)
	m := <-broadcastChan
	assert.NotNil(t, m.GetNewView())
	verifierWG.Wait()
	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)

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
	verifierWG := sync.WaitGroup{}
	verifier.On("VerifySignature", mock.Anything).Run(func(args mock.Arguments) {
		verifierWG.Done()
	}).Return(nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
	}).Return(nil).Once()

	vc := &bft.ViewChanger{
		SelfID:       0,
		N:            4,
		F:            1,
		Quorum:       3,
		Comm:         comm,
		Logger:       log,
		Verifier:     verifier,
		Controller:   controller,
		ResendTicker: make(chan time.Time),
	}

	vc.Start(2)

	// create a valid viewData message
	vd := &protos.ViewData{
		NextView: 2,
	}
	vdBytes := bft.MarshalOrPanic(vd)
	signed := make([]*protos.SignedViewData, 0)
	for len(signed) < vc.Quorum {
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

	verifierWG.Add(3)
	vc.HandleMessage(2, msg)
	verifierWG.Wait()
	num := <-viewNumChan
	assert.Equal(t, uint64(2), num)

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
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
	}).Return(nil).Once()
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		F:             1,
		Quorum:        3,
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Signer:        signer,
		RequestsTimer: reqTimer,
		ResendTicker:  make(chan time.Time),
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
				SelfID:       2,
				N:            4,
				F:            1,
				Quorum:       3,
				Comm:         comm,
				Logger:       log,
				Verifier:     verifier,
				ResendTicker: make(chan time.Time),
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
	var msg *protos.Message
	var resend bool
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		if !resend {
			msg = args.Get(0).(*protos.Message)
			resend = true
		} else {
			m := args.Get(0).(*protos.Message)
			msgChan <- m
		}
	})
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Once()
	ticker := make(chan time.Time)

	vc := &bft.ViewChanger{
		N:             4,
		Comm:          comm,
		RequestsTimer: reqTimer,
		ResendTicker:  ticker,
	}

	vc.Start(0)

	vc.StartViewChange()
	assert.NotNil(t, msg.GetViewChange())
	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)

	// resend
	ticker <- time.Time{}
	m := <-msgChan
	assert.NotNil(t, m.GetViewChange())

	// resend again
	ticker <- time.Time{}
	m = <-msgChan
	assert.NotNil(t, m.GetViewChange())

	vc.Stop()

}
