// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"sync"
	"testing"

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
		Comm: comm,
	}

	vc.Start()

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

	vc := &bft.ViewChanger{
		Comm: comm,
	}

	vc.Start()

	vc.StartViewChange()
	assert.NotNil(t, msg.GetViewChange())

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
		m, _ := args.Get(0).(*protos.Message)
		broadcastChan <- m
	}).Twice()
	leaderIDChan := make(chan uint64)
	sendChan := make(chan *protos.Message)
	comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		id := args.Get(0).(uint64)
		leaderIDChan <- id
		m := args.Get(1).(*protos.Message)
		sendChan <- m
	}).Twice()
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	vc := &bft.ViewChanger{
		SelfID: 0,
		N:      4,
		F:      1,
		Quorum: 3,
		Comm:   comm,
		Signer: signer,
		Logger: log,
	}

	vc.Start()

	vc.HandleMessage(1, viewChangeMsg)
	vc.HandleMessage(2, viewChangeMsg)
	m := <-broadcastChan
	assert.NotNil(t, m.GetViewChange())
	leader := <-leaderIDChan
	assert.Equal(t, uint64(1), leader)
	m = <-sendChan
	assert.NotNil(t, m.GetViewData())

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
	leader = <-leaderIDChan
	assert.Equal(t, uint64(2), leader)
	m = <-sendChan
	assert.NotNil(t, m.GetViewData())

	assert.Equal(t, uint64(2), vc.CurrView)
	assert.Equal(t, uint64(2), vc.Leader)

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
		SelfID:     1,
		N:          4,
		F:          1,
		Quorum:     3,
		Comm:       comm,
		Logger:     log,
		Verifier:   verifier,
		Controller: controller,
		CurrView:   1,
		Leader:     1,
	}

	vc.Start()

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
		SelfID:     0,
		N:          4,
		F:          1,
		Quorum:     3,
		Comm:       comm,
		Logger:     log,
		Verifier:   verifier,
		Controller: controller,
		CurrView:   2,
		Leader:     2,
	}

	vc.Start()

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
	vc := &bft.ViewChanger{
		SelfID:     1,
		N:          4,
		F:          1,
		Quorum:     3,
		Comm:       comm,
		Logger:     log,
		Verifier:   verifier,
		Controller: controller,
		Signer:     signer,
	}

	vc.Start()

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
