// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/SmartBFT/internal/bft"
	"github.com/hyperledger-labs/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger-labs/SmartBFT/pkg/api"
	"github.com/hyperledger-labs/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
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
	lastDecisionSignatures       = []types.Signature{{ID: 0, Value: []byte{4}, Msg: []byte{5}}, {ID: 1, Value: []byte{4}, Msg: []byte{5}}, {ID: 2, Value: []byte{4}, Msg: []byte{5}}}
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

func TestViewChangerBasic(*testing.T) {
	// A simple test that starts a viewChanger and stops it

	vc := &bft.ViewChanger{
		N:          4,
		NodesList:  []uint64{0, 1, 2, 3},
		Ticker:     make(chan time.Time),
		InMsqQSize: 100,
	}

	vc.Start(0)

	vc.Stop()
	vc.Stop()
}

func TestStartViewChange(t *testing.T) {
	// Test that when StartViewChange is called it broadcasts a message

	comm := &mocks.CommMock{}
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
	controller.On("AbortView", mock.Anything)

	vc := &bft.ViewChanger{
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Comm:          comm,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		Logger:        log,
		Controller:    controller,
		InMsqQSize:    100,
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

	for _, testCase := range []struct {
		description string
		speedup     bool
	}{
		{
			description: "without speedup",
			speedup:     false,
		},
		{
			description: "with speedup",
			speedup:     true,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			comm := &mocks.CommMock{}
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
			controller.On("AbortView", mock.Anything)
			state := &mocks.State{}
			state.On("Save", mock.Anything).Return(nil)

			vc := &bft.ViewChanger{
				SelfID:            0,
				N:                 4,
				NodesList:         []uint64{0, 1, 2, 3},
				Comm:              comm,
				Signer:            signer,
				Logger:            log,
				RequestsTimer:     reqTimer,
				Ticker:            make(chan time.Time),
				InFlight:          &bft.InFlightData{},
				Checkpoint:        &types.Checkpoint{},
				Controller:        controller,
				InMsqQSize:        100,
				State:             state,
				SpeedUpViewChange: testCase.speedup,
			}

			vc.Start(0)

			vc.HandleMessage(1, viewChangeMsg)
			vc.HandleMessage(2, viewChangeMsg)
			if !testCase.speedup {
				vc.HandleMessage(3, viewChangeMsg)
			}
			m := <-broadcastChan
			assert.NotNil(t, m.GetViewChange())
			m = <-sendChan
			assert.NotNil(t, m.GetViewData())
			comm.AssertCalled(t, "SendConsensus", uint64(1), mock.Anything)
			state.AssertNumberOfCalls(t, "Save", 1)

			// sending viewChange messages with same view doesn't make a difference
			vc.HandleMessage(1, viewChangeMsg)
			vc.HandleMessage(2, viewChangeMsg)
			if !testCase.speedup {
				vc.HandleMessage(3, viewChangeMsg)
			}

			// sending viewChange messages with bigger view doesn't make a difference
			msg3 := proto.Clone(viewChangeMsg).(*protos.Message)
			msg3.GetViewChange().NextView = 3
			vc.HandleMessage(2, msg3)
			vc.HandleMessage(1, msg3)
			if !testCase.speedup {
				vc.HandleMessage(3, msg3)
			}

			// sending viewChange messages with the next view
			msg2 := proto.Clone(viewChangeMsg).(*protos.Message)
			msg2.GetViewChange().NextView = 2
			vc.HandleMessage(2, msg2)
			vc.HandleMessage(1, msg2)
			if !testCase.speedup {
				vc.HandleMessage(3, msg2)
			}
			m = <-broadcastChan
			assert.NotNil(t, m.GetViewChange())
			m = <-sendChan
			assert.NotNil(t, m.GetViewData())
			comm.AssertCalled(t, "SendConsensus", uint64(2), mock.Anything)

			reqTimer.AssertNumberOfCalls(t, "StopTimers", 2)
			controller.AssertNumberOfCalls(t, "AbortView", 4)
			state.AssertNumberOfCalls(t, "Save", 2)

			vc.Stop()
		})
	}
}

func TestViewDataProcess(t *testing.T) {
	// Test the view data messages handling and process until sending a newView message

	comm := &mocks.CommMock{}
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
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("RestartTimers").Once()
	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Ticker:        make(chan time.Time),
		Checkpoint:    &checkpoint,
		InFlight:      &bft.InFlightData{},
		Signer:        signer,
		RequestsTimer: reqTimer,
		InMsqQSize:    100,
		State:         state,
	}

	vc.Start(1)

	verifierSigWG.Add(1)
	vc.HandleMessage(0, viewDataMsg1)
	verifierSigWG.Wait()

	msg1 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg1.GetViewData().Signer = 1

	verifierSigWG.Add(1)
	vc.HandleMessage(1, msg1)
	verifierSigWG.Wait()

	msg2 := proto.Clone(viewDataMsg1).(*protos.Message)
	msg2.GetViewData().Signer = 2

	verifierSigWG.Add(4)
	vc.HandleMessage(2, msg2)
	m := <-broadcastChan
	assert.NotNil(t, m.GetNewView())
	verifierSigWG.Wait()
	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(2), num)

	vc.Stop()
	reqTimer.AssertCalled(t, "RestartTimers")
	state.AssertCalled(t, "Save", mock.Anything)
}

func TestNewViewProcess(t *testing.T) {
	// Test the new view messages handling and process until calling controller

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	verifier := &mocks.VerifierMock{}
	verifierSigWG := sync.WaitGroup{}
	verifier.On("VerifySignature", mock.Anything).Run(func(args mock.Arguments) {
		verifierSigWG.Done()
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
	reqTimer.On("RestartTimers").Once()
	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)

	vc := &bft.ViewChanger{
		SelfID:        0,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Ticker:        make(chan time.Time),
		Checkpoint:    &checkpoint,
		RequestsTimer: reqTimer,
		InMsqQSize:    100,
		State:         state,
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
	vc.HandleMessage(2, msg)
	verifierSigWG.Wait()
	num := <-viewNumChan
	assert.Equal(t, uint64(2), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(2), num)

	vc.Stop()
	reqTimer.AssertCalled(t, "RestartTimers")
	state.AssertCalled(t, "Save", mock.Anything)
}

func TestNormalProcess(t *testing.T) {
	// Test a full view change process

	comm := &mocks.CommMock{}
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
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	controller.On("AbortView", mock.Anything)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Signer:        signer,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &checkpoint,
		InMsqQSize:    100,
		State:         state,
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

	controller.AssertNumberOfCalls(t, "AbortView", 2)
	state.AssertNumberOfCalls(t, "Save", 2)

	vc.Stop()
}

func TestBadViewDataMessage(t *testing.T) {
	// Test that bad view data messages don't cause a view change

	for _, test := range []struct {
		description           string
		mutateViewData        func(*protos.Message)
		mutateVerifySig       func(*mocks.VerifierMock)
		expectedMessageLogged string
		notLeader             bool
		deliver               bool
	}{
		{
			description:           "wrong leader",
			expectedMessageLogged: "is not the next leader",
			mutateViewData: func(m *protos.Message) {
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			notLeader: true,
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
			description:           "nil last decision",
			expectedMessageLogged: "the last decision is not set",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView:     1,
					LastDecision: nil,
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
		{
			description:           "genesis",
			expectedMessageLogged: "the last decision seq (0) is lower than",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView: 1,
					LastDecision: &protos.Proposal{
						Metadata: nil,
					},
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
		{
			description:           "wrong last decision view",
			expectedMessageLogged: "is greater or equal to requested next view",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView: 1,
					LastDecision: &protos.Proposal{
						Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
							LatestSequence: 1,
							ViewId:         1,
						}),
					},
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
		{
			description:           "greater last decision sequence",
			expectedMessageLogged: "greater than this node's current sequence",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView: 1,
					LastDecision: &protos.Proposal{
						Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
							LatestSequence: 3,
							ViewId:         0,
						}),
					},
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
		},
		{
			description:           "deliver last decision",
			expectedMessageLogged: "Delivering to app from deliverDecision the last decision proposal",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView: 1,
					LastDecision: &protos.Proposal{
						Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
							LatestSequence: 2,
							ViewId:         0,
						}),
					},
					LastDecisionSignatures: lastDecisionSignaturesProtos,
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
			},
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			deliver: true,
		},
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
			description:           "last decision not equal",
			expectedMessageLogged: "the last decisions are not equal",
			mutateViewData: func(m *protos.Message) {
				vd := &protos.ViewData{
					NextView: 1,
					LastDecision: &protos.Proposal{
						Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
							LatestSequence: 1,
							ViewId:         0,
						}),
					},
				}
				vdBytes := bft.MarshalOrPanic(vd)
				m.GetViewData().RawViewData = vdBytes
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
			verifier := &mocks.VerifierMock{}
			test.mutateVerifySig(verifier)
			verifier.On("VerifySignature", mock.Anything).Return(nil)
			verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
			verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
			verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
			app := &mocks.ApplicationMock{}
			var deliverWG sync.WaitGroup
			deliverWG.Add(1)
			app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				deliverWG.Done()
			}).Return(types.Reconfig{InLatestDecision: false})
			pruner := &mocks.Pruner{}
			pruner.On("MaybePruneRevokedRequests")
			checkpoint := types.Checkpoint{}
			checkpoint.Set(lastDecision, lastDecisionSignatures)

			selfId := 1
			if test.notLeader {
				selfId = 2
			}

			vc := &bft.ViewChanger{
				SelfID:      uint64(selfId),
				N:           4,
				NodesList:   []uint64{0, 1, 2, 3},
				Logger:      log,
				Verifier:    verifier,
				Checkpoint:  &checkpoint,
				Application: app,
				Pruner:      pruner,
				Ticker:      make(chan time.Time),
				InMsqQSize:  100,
			}

			vc.Start(1)

			msg := proto.Clone(viewDataMsg1).(*protos.Message)
			test.mutateViewData(msg)

			vc.HandleMessage(0, msg)
			warningMsgLogged.Wait()

			if test.deliver {
				deliverWG.Wait()
			}

			vc.Stop()
		})
	}
}

func TestBadNewViewMessage(t *testing.T) {
	for _, test := range []struct {
		description           string
		expectedMessageLogged string
		notLeader             bool
		sync                  bool
		deliver               bool
		mutateVerifySig       func(*mocks.VerifierMock)
		mutateNewView         func(*protos.Message)
	}{
		{
			description:           "wrong leader",
			expectedMessageLogged: "expected sender to be",
			notLeader:             true,
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
			},
		},
		{
			description:           "wrong view",
			expectedMessageLogged: "while the currView is",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
				viewData := proto.Clone(vd).(*protos.ViewData)
				viewData.NextView = 2
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(viewData),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
			},
		},
		{
			description:           "invalid signature",
			expectedMessageLogged: "but signature of",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
				verifierMock.On("VerifySignature", mock.Anything).Return(errors.New(""))
			},
			mutateNewView: func(m *protos.Message) {
			},
		},
		{
			description:           "different last decision",
			expectedMessageLogged: "is with the same sequence but is not equal",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
				viewData := proto.Clone(vd).(*protos.ViewData)
				viewData.LastDecision.Payload = nil
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(viewData),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
			},
		},
		{
			description:           "sync",
			expectedMessageLogged: "requested a sync",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
				viewData := proto.Clone(vd).(*protos.ViewData)
				viewData.LastDecision.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence: 3,
					ViewId:         0,
				})
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(viewData),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
			},
			sync: true,
		},
		{
			description:           "invalid last decision sequence",
			expectedMessageLogged: "greater or equal to requested next view",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
				viewData := proto.Clone(vd).(*protos.ViewData)
				viewData.LastDecision.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence: 1,
					ViewId:         1,
				})
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(viewData),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
			},
		},
		{
			description:           "last decision not set",
			expectedMessageLogged: "is not set",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
				viewData := proto.Clone(vd).(*protos.ViewData)
				viewData.LastDecision = nil
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(viewData),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
			},
		},
		{
			description:           "deliver",
			expectedMessageLogged: "but signature of",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
				verifierMock.On("VerifySignature", mock.Anything).Return(errors.New(""))
			},
			mutateNewView: func(m *protos.Message) {
				viewData := proto.Clone(vd).(*protos.ViewData)
				viewData.LastDecision.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence: 2,
					ViewId:         0,
				})
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(viewData),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
			},
			deliver: true,
		},
		{
			description:           "not enough",
			expectedMessageLogged: "valid view data messages while the quorum is",
			mutateVerifySig: func(verifierMock *mocks.VerifierMock) {
			},
			mutateNewView: func(m *protos.Message) {
				signed := make([]*protos.SignedViewData, 0)
				signed = append(signed, &protos.SignedViewData{
					RawViewData: bft.MarshalOrPanic(vd),
					Signer:      0,
					Signature:   nil,
				})
				m.GetNewView().SignedViewData = signed
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
			verifier := &mocks.VerifierMock{}
			test.mutateVerifySig(verifier)
			verifier.On("VerifySignature", mock.Anything).Return(nil)
			verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
			verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
			verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
			app := &mocks.ApplicationMock{}
			var deliverWG sync.WaitGroup
			deliverWG.Add(1)
			app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				deliverWG.Done()
			}).Return(types.Reconfig{InLatestDecision: false})
			pruner := &mocks.Pruner{}
			pruner.On("MaybePruneRevokedRequests")
			checkpoint := types.Checkpoint{}
			checkpoint.Set(lastDecision, lastDecisionSignatures)
			synchronizer := &mocks.Synchronizer{}
			synchronizerWG := sync.WaitGroup{}
			synchronizerWG.Add(1)
			synchronizer.On("Sync").Run(func(args mock.Arguments) {
				synchronizerWG.Done()
			})

			vc := &bft.ViewChanger{
				SelfID:       3,
				N:            4,
				NodesList:    []uint64{0, 1, 2, 3},
				Logger:       log,
				Checkpoint:   &checkpoint,
				Verifier:     verifier,
				Synchronizer: synchronizer,
				Application:  app,
				Pruner:       pruner,
				Ticker:       make(chan time.Time),
				InMsqQSize:   100,
			}

			vc.Start(1)

			vdBytes := bft.MarshalOrPanic(vd)
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
			test.mutateNewView(msg)

			if test.notLeader {
				vc.HandleMessage(0, msg)
			} else {
				vc.HandleMessage(1, msg)
			}

			if test.sync {
				synchronizerWG.Wait()
			}

			if test.deliver {
				deliverWG.Wait()
			}

			warningMsgLogged.Wait()

			vc.Stop()
		})
	}
}

func TestResendViewChangeMessage(t *testing.T) {
	comm := &mocks.CommMock{}
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
	controller.On("AbortView", mock.Anything)

	vc := &bft.ViewChanger{
		N:                 4,
		NodesList:         []uint64{0, 1, 2, 3},
		Comm:              comm,
		RequestsTimer:     reqTimer,
		Ticker:            ticker,
		Logger:            log,
		Controller:        controller,
		ResendTimeout:     time.Second,
		ViewChangeTimeout: 10 * time.Second,
		InMsqQSize:        100,
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
	controller.On("AbortView", mock.Anything).Run(func(args mock.Arguments) {
		controllerWG.Done()
	})

	vc := &bft.ViewChanger{
		N:                 4,
		NodesList:         []uint64{0, 1, 2, 3},
		Comm:              comm,
		RequestsTimer:     reqTimer,
		Ticker:            ticker,
		Logger:            log,
		ViewChangeTimeout: 10 * time.Second,
		ResendTimeout:     20 * time.Second,
		Synchronizer:      synchronizer,
		Controller:        controller,
		InMsqQSize:        100,
	}

	vc.Start(0)
	startTime := time.Now()

	controllerWG.Add(1)
	reqTimerWG.Add(1)
	vc.StartViewChange(0, true) // start timer
	controllerWG.Wait()
	reqTimerWG.Wait()

	synchronizerWG.Add(1)
	ticker <- startTime.Add(12 * time.Second) // timeout
	synchronizerWG.Wait()

	vc.Stop()

	synchronizer.AssertNumberOfCalls(t, "Sync", 1)
	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
	controller.AssertNumberOfCalls(t, "AbortView", 1)
	comm.AssertNumberOfCalls(t, "BroadcastConsensus", 1)
}

func TestBackOff(t *testing.T) {
	comm := &mocks.CommMock{}
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
	controller.On("AbortView", mock.Anything).Run(func(args mock.Arguments) {
		controllerWG.Done()
	})

	timeout := 10 * time.Second

	vc := &bft.ViewChanger{
		N:                 4,
		NodesList:         []uint64{0, 1, 2, 3},
		Comm:              comm,
		RequestsTimer:     reqTimer,
		Ticker:            ticker,
		Logger:            log,
		ViewChangeTimeout: timeout,
		ResendTimeout:     100 * time.Second,
		Synchronizer:      synchronizer,
		Controller:        controller,
		InMsqQSize:        100,
	}

	vc.Start(0)
	startTime := time.Now()

	controllerWG.Add(1)
	reqTimerWG.Add(1)
	vc.StartViewChange(0, true) // start timer
	controllerWG.Wait()
	reqTimerWG.Wait()

	synchronizerWG.Add(1)
	ticker <- startTime.Add(timeout + 2*time.Second) // timeout
	synchronizerWG.Wait()

	ticker <- startTime.Add(timeout + 2*time.Second) // no timeout

	synchronizerWG.Add(1)
	ticker <- startTime.Add(2*timeout + 2*time.Second) // timeout with backOff
	synchronizerWG.Wait()

	vc.Stop()

	synchronizer.AssertNumberOfCalls(t, "Sync", 2)
	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
	controller.AssertNumberOfCalls(t, "AbortView", 1)
	comm.AssertNumberOfCalls(t, "BroadcastConsensus", 1)
}

func TestCommitLastDecision(t *testing.T) {
	comm := &mocks.CommMock{}
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
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("VerifyProposal", mock.Anything).Return(nil, nil)
	verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	controller.On("AbortView", mock.Anything)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		prop := args.Get(0).(types.Proposal)
		sign := args.Get(1).([]types.Signature)
		checkpoint.Set(prop, sign)
	}).Return(types.Reconfig{InLatestDecision: false})

	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)
	pruner := &mocks.Pruner{}
	pruner.On("MaybePruneRevokedRequests")

	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
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
		InMsqQSize:    100,
		State:         state,
		Pruner:        pruner,
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

	controller.AssertNumberOfCalls(t, "AbortView", 2)
	app.AssertNumberOfCalls(t, "Deliver", 1)
	pruner.AssertNumberOfCalls(t, "MaybePruneRevokedRequests", 1)
	state.AssertNumberOfCalls(t, "Save", 2)

	vc.Stop()
}

func TestFarBehindLastDecisionAndSync(t *testing.T) {
	// Scenario: a node gets a newView message with last decision with seq 3,
	// while its checkpoint shows that its last decision is only at seq 1,
	// the node calls sync to catch up.

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	synchronizer := &mocks.Synchronizer{}
	synchronizerWG := sync.WaitGroup{}
	synchronizer.On("Sync").Run(func(args mock.Arguments) {
		synchronizerWG.Done()
	})

	vc := &bft.ViewChanger{
		SelfID:       3,
		N:            4,
		NodesList:    []uint64{0, 1, 2, 3},
		Logger:       log,
		Ticker:       make(chan time.Time),
		Checkpoint:   &checkpoint,
		Synchronizer: synchronizer,
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

	vc.Start(1)

	synchronizerWG.Add(1)
	vc.HandleMessage(1, msg)
	synchronizerWG.Wait()

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
			controller.On("AbortView", mock.Anything)
			checkpoint := types.Checkpoint{}
			checkpoint.Set(lastDecision, lastDecisionSignatures)
			state := &mocks.State{}
			state.On("Save", mock.Anything).Return(nil)

			vc := &bft.ViewChanger{
				SelfID:        0,
				N:             4,
				NodesList:     []uint64{0, 1, 2, 3},
				Comm:          comm,
				Signer:        signer,
				Logger:        log,
				RequestsTimer: reqTimer,
				Ticker:        make(chan time.Time),
				InFlight:      test.getInFlight(),
				Checkpoint:    &checkpoint,
				Controller:    controller,
				InMsqQSize:    100,
				State:         state,
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
			controller.AssertNumberOfCalls(t, "AbortView", 2)
			state.AssertNumberOfCalls(t, "Save", 1)
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
				verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, errors.New(""))
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
				verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
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
				verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
			},
			valid:    true,
			sequence: 1,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			verifier := &mocks.VerifierMock{}
			test.mutateVerify(verifier)
			seq, err := bft.ValidateLastDecision(test.viewData, 3, 4, verifier)
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
	msgChan := make(chan *protos.Message)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		msgChan <- args.Get(0).(*protos.Message)
	})
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers").Once()
	reqTimer.On("RestartTimers")
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	controller := &mocks.ViewController{}
	controller.On("AbortView", mock.Anything)

	vc := &bft.ViewChanger{
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Comm:          comm,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		Logger:        log,
		Controller:    controller,
		InMsqQSize:    100,
	}

	vc.Start(0)

	info := uint64(2)
	vc.InformNewView(info) // increase the view number
	vc.InformNewView(info) // make sure that inform happened (channel size is 1)

	vc.StartViewChange(2, true)
	msg := <-msgChan
	assert.NotNil(t, msg.GetViewChange())
	assert.Equal(t, info+1, msg.GetViewChange().NextView) // view number did change according to info

	vc.Stop()

	reqTimer.AssertNumberOfCalls(t, "StopTimers", 1)
	controller.AssertNumberOfCalls(t, "AbortView", 1)
	reqTimer.AssertCalled(t, "RestartTimers")
}

func TestRestoreViewChange(t *testing.T) {
	comm := &mocks.CommMock{}
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
	controller.On("AbortView", mock.Anything)

	vc := &bft.ViewChanger{
		SelfID:        0,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Comm:          comm,
		Signer:        signer,
		Logger:        log,
		RequestsTimer: reqTimer,
		Ticker:        make(chan time.Time),
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &types.Checkpoint{},
		Controller:    controller,
		InMsqQSize:    100,
	}

	restoreChan := make(chan struct{}, 1)
	restoreChan <- struct{}{}
	vc.Restore = restoreChan

	vc.Start(5)
	m := <-broadcastChan
	assert.NotNil(t, m.GetViewChange())
	assert.Equal(t, uint64(6), m.GetViewChange().NextView)
	m = <-sendChan
	assert.NotNil(t, m.GetViewData())

	comm.AssertNumberOfCalls(t, "SendConsensus", 1)
	vc.Stop()
}

func TestCheckInFlightNoProposal(t *testing.T) {
	expectedProposal := &protos.Proposal{
		Header:  []byte{0},
		Payload: []byte{1},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			ViewId:         0,
			LatestSequence: 2, // last decision sequence + 1
		}),
		VerificationSequence: uint64(1),
	}
	for _, test := range []struct {
		description    string
		mutateMessages func([]*protos.ViewData)
	}{
		{
			description: "all without in flight proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				// in flight is already unset
			},
		},
		{
			description: "all with old in flight proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = msg.LastDecision
				}
			},
		},
		{
			description: "quorum without in flight proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				// in flight is already unset in all
				messages[0].InFlightProposal = expectedProposal
			},
		},
		{
			description: "quorum with an old in flight proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = msg.LastDecision // set all with an old proposal (same as last decision)
				}
				messages[0].InFlightProposal = expectedProposal
			},
		},
		{
			description: "some with no in flight proposal and some with an old one",
			mutateMessages: func(messages []*protos.ViewData) {
				messages[0].InFlightProposal = messages[0].LastDecision
				messages[1].InFlightProposal = messages[1].LastDecision
				messages[2].InFlightProposal = expectedProposal
				messages[3].InFlightProposal = nil
			},
		},
		{
			description: "enough with a different in flight proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				messages[0].InFlightProposal = messages[0].LastDecision
				messages[1].InFlightProposal = nil
				messages[2].InFlightProposal = expectedProposal
				messages[3].InFlightProposal = expectedProposal
			},
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
			messages := make([]*protos.ViewData, 0)
			for i := 0; i < 4; i++ {
				messages = append(messages, proto.Clone(vd).(*protos.ViewData))
			}
			test.mutateMessages(messages)
			ok, _, _, err := bft.CheckInFlight(messages, 1, 3, 4, verifier)
			assert.NoError(t, err)
			assert.True(t, ok)
		})
	}
}

func TestCheckInFlightWithProposal(t *testing.T) {
	expectedProposal := &protos.Proposal{
		Header:  []byte{0},
		Payload: []byte{1},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			ViewId:         0,
			LatestSequence: 2, // last decision sequence + 1
		}),
		VerificationSequence: uint64(1),
	}
	for _, test := range []struct {
		description    string
		mutateMessages func([]*protos.ViewData)
		ok             bool
		no             bool
		proposal       *protos.Proposal
	}{
		{
			description: "all with expected in flight proposal and prepared",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
					msg.InFlightPrepared = true
				}
			},
			ok:       true,
			no:       false,
			proposal: expectedProposal,
		},
		{
			description: "quorum with expected in flight proposal and prepared, other with no in flight",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
					msg.InFlightPrepared = true
				}
				messages[0].InFlightProposal = nil
			},
			ok:       true,
			no:       false,
			proposal: expectedProposal,
		},
		{
			description: "quorum with expected in flight proposal and prepared, other with old in flight",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
					msg.InFlightPrepared = true
				}
				messages[0].InFlightProposal = messages[0].LastDecision
			},
			ok:       true,
			no:       false,
			proposal: expectedProposal,
		},
		{
			description: "quorum with expected in flight proposal and prepared, other with different in flight",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
					msg.InFlightPrepared = true
				}
				different := proto.Clone(expectedProposal).(*protos.Proposal)
				different.Payload = []byte{5}
				messages[0].InFlightProposal = different
			},
			ok:       true,
			no:       false,
			proposal: expectedProposal,
		},
		{
			description: "all with expected in flight proposal, only one is prepared, other with different in flight",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
				}
				different := proto.Clone(expectedProposal).(*protos.Proposal)
				different.Header = []byte{5}
				messages[0].InFlightProposal = different
				messages[3].InFlightPrepared = true
			},
			ok:       true,
			no:       false,
			proposal: expectedProposal,
		},
		{
			description: "all with expected in flight proposal, no one is prepared",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
					msg.InFlightPrepared = false
				}
			},
			ok:       true,
			no:       true,
			proposal: nil,
		},
		{
			description: "all with in flight proposal and prepared, but no quorum on any proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				for _, msg := range messages {
					msg.InFlightProposal = expectedProposal
					msg.InFlightPrepared = true
				}
				different := proto.Clone(expectedProposal).(*protos.Proposal)
				different.VerificationSequence = 10
				messages[2].InFlightProposal = different
				messages[3].InFlightProposal = different
			},
			ok:       false,
			no:       false,
			proposal: nil,
		},
		{
			description: "not enough preprepared for quorum on in flight proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				messages[2].InFlightProposal = expectedProposal
				messages[2].InFlightPrepared = true
			},
			ok:       true,
			no:       true,
			proposal: nil,
		},
		{
			description: "not enough for any in flight proposal, and also not enough for no proposal",
			mutateMessages: func(messages []*protos.ViewData) {
				messages[2].InFlightProposal = expectedProposal
				messages[2].InFlightPrepared = true
				different := proto.Clone(expectedProposal).(*protos.Proposal)
				different.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
					ViewId:         1,
					LatestSequence: 2, // last decision sequence + 1
				})
				messages[3].InFlightProposal = different
				messages[3].InFlightPrepared = true
				different2 := proto.Clone(expectedProposal).(*protos.Proposal)
				different2.VerificationSequence = 5
				messages[1].InFlightProposal = different2
				messages[1].InFlightPrepared = true
			},
			ok:       false,
			no:       false,
			proposal: nil,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
			messages := make([]*protos.ViewData, 0)
			for i := 0; i < 4; i++ {
				messages = append(messages, proto.Clone(vd).(*protos.ViewData))
			}
			test.mutateMessages(messages)
			ok, no, proposal, err := bft.CheckInFlight(messages, 1, 3, 4, verifier)
			assert.NoError(t, err)
			assert.Equal(t, test.ok, ok)
			assert.Equal(t, test.no, no)
			assert.Equal(t, test.proposal, proposal)
		})
	}
}

func TestCommitInFlight(t *testing.T) {
	comm := &mocks.CommMock{}
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
	signWG := sync.WaitGroup{}
	signer.On("SignProposal", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		signWG.Done()
	}).Return(&types.Signature{
		ID:    1,
		Value: []byte{4},
	})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	controller.On("AbortView", mock.Anything)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	app := &mocks.ApplicationMock{}
	appChan := make(chan types.Proposal)
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		prop := args.Get(0).(types.Proposal)
		sign := args.Get(1).([]types.Signature)
		checkpoint.Set(prop, sign)
		appChan <- prop
	}).Return(types.Reconfig{InLatestDecision: false})
	pruner := &mocks.Pruner{}
	pruner.On("MaybePruneRevokedRequests")

	sched := make(chan time.Time)
	vc := &bft.ViewChanger{
		SelfID:           1,
		N:                4,
		NodesList:        []uint64{0, 1, 2, 3},
		Comm:             comm,
		Logger:           log,
		Verifier:         verifier,
		Controller:       controller,
		Signer:           signer,
		RequestsTimer:    reqTimer,
		Ticker:           sched,
		InFlight:         &bft.InFlightData{},
		Checkpoint:       &checkpoint,
		ViewSequences:    &atomic.Value{},
		State:            &bft.StateRecorder{},
		InMsqQSize:       int(types.DefaultConfig.IncomingMessageBufferSize),
		Application:      app,
		Pruner:           pruner,
		MetricsBlacklist: api.NewMetricsBlacklist(&disabled.Provider{}),
		MetricsView:      api.NewMetricsView(&disabled.Provider{}),
	}

	vc.Start(0)

	vc.HandleMessage(2, viewChangeMsg)
	vc.HandleMessage(3, viewChangeMsg)
	m := <-msgChan
	assert.NotNil(t, m.GetViewChange())

	inFlightProposal := types.Proposal{
		Payload: []byte{1},
		Header:  []byte{2},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 2,
			ViewId:         0,
		}),
		VerificationSequence: 1,
	}

	viewData := proto.Clone(vd).(*protos.ViewData)
	viewData.InFlightProposal = &protos.Proposal{
		Payload:              inFlightProposal.Payload,
		Header:               inFlightProposal.Header,
		Metadata:             inFlightProposal.Metadata,
		VerificationSequence: uint64(inFlightProposal.VerificationSequence),
	}
	viewData.InFlightPrepared = true

	msg := proto.Clone(viewDataMsg1).(*protos.Message)
	msg.GetViewData().RawViewData = bft.MarshalOrPanic(viewData)

	signWG.Add(1)

	vc.HandleMessage(0, msg)
	msg2 := proto.Clone(msg).(*protos.Message)
	msg2.GetViewData().Signer = 2
	vc.HandleMessage(2, msg2)
	m = <-msgChan
	assert.NotNil(t, m.GetNewView())

	go func() {
		sched <- time.Now()
		sched <- time.Now()
	}()

	signWG.Wait()

	commitMsg := &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   0,
				Seq:    2,
				Digest: inFlightProposal.Digest(),
				Signature: &protos.Signature{
					Signer: 0,
					Value:  []byte{4},
				},
			},
		},
	}
	vc.HandleViewMessage(0, commitMsg)
	commitMsg2 := proto.Clone(commitMsg).(*protos.Message)
	commitMsg2.GetCommit().Signature.Signer = 2
	vc.HandleViewMessage(2, commitMsg2)

	m = <-msgChan
	assert.NotNil(t, m.GetCommit())

	delivered := <-appChan
	assert.Equal(t, inFlightProposal, delivered)

	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(3), num)

	controller.AssertNumberOfCalls(t, "AbortView", 2)
	pruner.AssertNumberOfCalls(t, "MaybePruneRevokedRequests", 1)

	vc.Stop()
}

func TestCommitWhileHavingInFlight(t *testing.T) {
	comm := &mocks.CommMock{}
	msgChan := make(chan *protos.Message, 1)
	comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
		m := args.Get(0).(*protos.Message)
		msgChan <- m
	})

	var reasonAboutInFlightLowerThanLatestDecision sync.WaitGroup
	reasonAboutInFlightLowerThanLatestDecision.Add(1)

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Node 1's in flight proposal sequence is 1 while already committed decision 2, but that is because it committed it during the view change") {
			reasonAboutInFlightLowerThanLatestDecision.Done()
		}
		return nil
	})).Sugar()

	signer := &mocks.SignerMock{}
	signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
	signWG := sync.WaitGroup{}
	signer.On("SignProposal", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		signWG.Done()
	}).Return(&types.Signature{
		ID:    1,
		Value: []byte{4},
	})
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
	controller := &mocks.ViewController{}
	controller.On("ViewChanged", mock.Anything, mock.Anything)
	controller.On("AbortView", mock.Anything)
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("StopTimers")
	reqTimer.On("RestartTimers")
	checkpoint := types.Checkpoint{}
	checkpoint.Set(lastDecision, lastDecisionSignatures)
	app := &mocks.ApplicationMock{}
	appChan := make(chan types.Proposal, 1)
	app.On("Deliver", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		prop := args.Get(0).(types.Proposal)
		sign := args.Get(1).([]types.Signature)
		checkpoint.Set(prop, sign)
		appChan <- prop
	}).Return(types.Reconfig{InLatestDecision: false})
	pruner := &mocks.Pruner{}
	pruner.On("MaybePruneRevokedRequests")

	sched := make(chan time.Time)
	vc := &bft.ViewChanger{
		SelfID:        1,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Comm:          comm,
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Signer:        signer,
		RequestsTimer: reqTimer,
		Ticker:        sched,
		InFlight:      &bft.InFlightData{},
		Checkpoint:    &checkpoint,
		ViewSequences: &atomic.Value{},
		State:         &bft.StateRecorder{},
		InMsqQSize:    int(types.DefaultConfig.IncomingMessageBufferSize),
		Application:   app,
		Pruner:        pruner,
	}

	vc.InFlight.StoreProposal(types.Proposal{
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 1,
		}),
	})

	defer vc.Stop()
	vc.Start(0)

	vc.HandleMessage(2, viewChangeMsg)
	vc.HandleMessage(3, viewChangeMsg)
	m := <-msgChan
	assert.NotNil(t, m.GetViewChange())

	viewData := proto.Clone(vd).(*protos.ViewData)
	viewData.LastDecision.Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		LatestSequence: 2,
	})

	msg := proto.Clone(viewDataMsg1).(*protos.Message)
	msg.GetViewData().RawViewData = bft.MarshalOrPanic(viewData)

	signWG.Add(1)

	vc.HandleMessage(0, msg)

	// Wait for committing the block
	commit := <-appChan
	assert.Equal(t, viewData.LastDecision.Metadata, commit.Metadata)

	// Get the second view data message, this time our latest decision is higher by 1

	msg2 := proto.Clone(msg).(*protos.Message)
	msg2.GetViewData().Signer = 2
	vc.HandleMessage(2, msg2)

	reasonAboutInFlightLowerThanLatestDecision.Wait()

}

func TestDontCommitInFlight(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	verifier := &mocks.VerifierMock{}
	verifier.On("VerifySignature", mock.Anything).Return(nil)
	verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
	controller := &mocks.ViewController{}
	viewNumChan := make(chan uint64)
	seqNumChan := make(chan uint64)
	controller.On("ViewChanged", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		num := args.Get(0).(uint64)
		viewNumChan <- num
		num = args.Get(1).(uint64)
		seqNumChan <- num
	}).Return(nil).Once()
	reqTimer := &mocks.RequestsTimer{}
	reqTimer.On("RestartTimers")
	app := &mocks.ApplicationMock{}
	app.On("Deliver", mock.Anything, mock.Anything)
	state := &mocks.State{}
	state.On("Save", mock.Anything).Return(nil)

	vc := &bft.ViewChanger{
		SelfID:        3,
		N:             4,
		NodesList:     []uint64{0, 1, 2, 3},
		Logger:        log,
		Verifier:      verifier,
		Controller:    controller,
		Ticker:        make(chan time.Time),
		RequestsTimer: reqTimer,
		Application:   app,
		State:         state,
	}

	inFlightProposal := types.Proposal{
		Payload: []byte{1},
		Header:  []byte{2},
		Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence: 2,
			ViewId:         0,
		}),
		VerificationSequence: 1,
	}

	viewData := proto.Clone(vd).(*protos.ViewData)
	viewData.InFlightProposal = &protos.Proposal{
		Payload:              inFlightProposal.Payload,
		Header:               inFlightProposal.Header,
		Metadata:             inFlightProposal.Metadata,
		VerificationSequence: uint64(inFlightProposal.VerificationSequence),
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

	checkpoint := types.Checkpoint{}
	checkpoint.Set(inFlightProposal, lastDecisionSignatures)
	vc.Checkpoint = &checkpoint

	vc.Start(1)

	vc.HandleMessage(1, msg)

	num := <-viewNumChan
	assert.Equal(t, uint64(1), num)
	num = <-seqNumChan
	assert.Equal(t, uint64(3), num)

	vc.Stop()

	app.AssertNotCalled(t, "Deliver")
}
