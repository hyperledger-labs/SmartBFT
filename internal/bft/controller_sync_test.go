// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// stubSynchronizer returns a fixed SyncResponse.
type stubSynchronizer struct {
	response types.SyncResponse
}

func (s *stubSynchronizer) Sync() types.SyncResponse { return s.response }

// stubComm is a no-op implementation of api.Comm.
type stubComm struct{}

func (s *stubComm) SendConsensus(targetID uint64, m *protos.Message) {}
func (s *stubComm) SendTransaction(targetID uint64, request []byte)  {}
func (s *stubComm) Nodes() []uint64                                  { return []uint64{1, 2, 3, 4} }
func (s *stubComm) BroadcastConsensus(m *protos.Message)             {}

// stateRecorder records the last message saved to the state.
type stateRecorder struct {
	saved *protos.SavedMessage
}

func (s *stateRecorder) Save(m *protos.SavedMessage) error {
	s.saved = m
	return nil
}

func (s *stateRecorder) Restore(v *View) error { return nil }

// stubRequestsTimer is a no-op implementation of RequestsTimer.
type stubRequestsTimer struct{}

func (s *stubRequestsTimer) StopTimers()                             {}
func (s *stubRequestsTimer) RestartTimers()                          {}
func (s *stubRequestsTimer) RemoveRequest(r types.RequestInfo) error { return nil }

// peerStateComm answers state transfer requests with a fixed view and sequence, the way the
// other nodes of the cluster do.
type peerStateComm struct {
	stubComm
	view       uint64
	seq        uint64
	onResponse func(sender uint64, m *protos.Message)
}

func (c *peerStateComm) SendConsensus(targetID uint64, m *protos.Message) {
	if m.GetStateTransferRequest() == nil || c.onResponse == nil {
		return
	}
	// the peers answer asynchronously, they are not inline with the request
	go c.onResponse(targetID, &protos.Message{
		Content: &protos.Message_StateTransferResponse{
			StateTransferResponse: &protos.StateTransferResponse{
				ViewNum:  c.view,
				Sequence: c.seq,
			},
		},
	})
}

// TestSyncDecisionsInView tests that the sync() method returns the correct
// DecisionsInView value in two scenarios:
//   - same height: sync returns the same sequence as the controller, so
//     newDecisionsInView must come from the controller's in-memory state
//     (getCurrentDecisionsInView), not remain zero-initialized.
//   - higher height: sync returns a higher sequence, so newDecisionsInView
//     is overridden from the sync response metadata (+1).
func TestSyncDecisionsInView(t *testing.T) {
	const (
		controllerSeq       = uint64(2365908)
		controllerView      = uint64(782)
		controllerDecisions = uint64(9578)
	)

	// newController creates a minimal Controller with only the fields
	// needed by sync(). The checkpoint metadata sets the controller's
	// current sequence, and the synchronizer controls what sync returns.
	newController := func(t *testing.T, syncResponse types.SyncResponse) *Controller {
		t.Helper()

		basicLog, err := zap.NewDevelopment()
		assert.NoError(t, err)
		log := basicLog.Sugar()

		// Checkpoint metadata determines latestSeq() return value.
		checkpoint := &types.Checkpoint{}
		checkpoint.Set(types.Proposal{
			Metadata: MarshalOrPanic(&protos.ViewMetadata{
				LatestSequence:  controllerSeq,
				ViewId:          controllerView,
				DecisionsInView: controllerDecisions,
			}),
		}, nil)

		// StateCollector with very short timeout so fetchState() returns nil
		// without needing real consensus broadcast responses.
		collector := &StateCollector{
			SelfID:         1,
			N:              4,
			Logger:         log,
			CollectTimeout: time.Millisecond,
		}
		collector.Start()
		t.Cleanup(collector.Stop)

		c := &Controller{
			ID:                  1,
			N:                   4,
			Logger:              log,
			Comm:                &stubComm{},
			Synchronizer:        &stubSynchronizer{response: syncResponse},
			Checkpoint:          checkpoint,
			Collector:           collector,
			InFlight:            &InFlightData{},
			ViewChanger:         &ViewChanger{},
			currViewNumber:      controllerView,
			currDecisionsInView: controllerDecisions + 1, // checkpoint stores N, decide() increments to N+1
		}
		// sync() uses grabSyncToken/relinquishSyncToken which need syncChan.
		c.syncChan = make(chan struct{}, 1)

		return c
	}

	t.Run("same_height_must_preserve_decisions", func(t *testing.T) {
		// Synchronizer returns the same sequence as the controller.
		// This simulates the "already at target height" scenario where
		// the orderer is not behind but sync was triggered anyway.
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq,
						ViewId:          controllerView,
						DecisionsInView: controllerDecisions,
					}),
					VerificationSequence: 0,
				},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		})

		viewNum, seq, decisions := c.sync()

		assert.Equal(t, controllerView, viewNum, "view number should be preserved")
		assert.Equal(t, controllerSeq+1, seq, "proposal sequence should be controllerSeq+1")
		// newDecisionsInView should be initialized from the controller's in-memory
		// state (getCurrentDecisionsInView), which is controllerDecisions+1.
		assert.Equal(t, controllerDecisions+1, decisions,
			"DecisionsInView must be preserved when sync returns same height")
	})

	t.Run("higher_height_overrides_decisions", func(t *testing.T) {
		// Synchronizer returns a higher sequence than the controller.
		// In this case newDecisionsInView is overridden from the sync response (+1).
		syncDecisions := controllerDecisions + 1
		c := newController(t, types.SyncResponse{
			Latest: types.Decision{
				Proposal: types.Proposal{
					Metadata: MarshalOrPanic(&protos.ViewMetadata{
						LatestSequence:  controllerSeq + 1,
						ViewId:          controllerView,
						DecisionsInView: syncDecisions,
					}),
					VerificationSequence: 0,
				},
			},
			Reconfig: types.ReconfigSync{InReplicatedDecisions: false},
		})

		viewNum, seq, decisions := c.sync()

		assert.Equal(t, controllerView, viewNum, "view number should be preserved")
		assert.Equal(t, controllerSeq+2, seq, "proposal sequence should be syncSeq+1")
		assert.Equal(t, syncDecisions+1, decisions,
			"DecisionsInView should be latestDecisionDecisions+1 when sync returns higher height")
	})
}

// the sequence the lagging node knows about, and the view it is stuck in
const (
	controllerSeq       = uint64(100)
	controllerView      = uint64(67)
	controllerDecisions = uint64(3)
	clusterView         = uint64(69)
)

// TestSyncLearnsViewFromPeers covers the case where the cluster moved to a higher view without
// making a decision, so the latest decision says nothing about that view. The only source of
// the current view is then the state the other nodes report, and the node has to be able to
// move to it, otherwise it stays in a view the rest of the cluster left and no quorum can be
// formed for transactions again.
func TestSyncLearnsViewFromPeers(t *testing.T) {
	// the peers report the sequence they are at. It is higher than the one the lagging node
	// knows, which is what happens when the cluster also decided transactions while the node
	// was stuck, and it is what the node has to catch up with before it can rejoin.
	for _, test := range []struct {
		description string
		peerSeq     uint64
	}{
		{description: "the cluster did not decide anything while the node was stuck", peerSeq: controllerSeq + 1},
		{description: "the cluster decided transactions while the node was stuck", peerSeq: controllerSeq + 10},
	} {
		t.Run(test.description, func(t *testing.T) {
			runSyncAndAssertViewLearned(t, test.peerSeq)
		})
	}
}

func runSyncAndAssertViewLearned(t *testing.T, peerSeq uint64) {
	t.Helper()

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	// the node is in view 67 and has not decided anything since, so its checkpoint knows
	// nothing about the view the cluster moved to
	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{
		Metadata: MarshalOrPanic(&protos.ViewMetadata{
			LatestSequence:  controllerSeq,
			ViewId:          controllerView,
			DecisionsInView: controllerDecisions,
		}),
	}, nil)

	// the synchronizer returns the same decision the node already has, since the cluster made
	// no progress on transactions the node can see while the view was changing
	syncResponse := types.SyncResponse{
		Latest: types.Decision{
			Proposal: types.Proposal{
				Metadata: MarshalOrPanic(&protos.ViewMetadata{
					LatestSequence:  controllerSeq,
					ViewId:          controllerView,
					DecisionsInView: controllerDecisions,
				}),
			},
		},
	}

	// the peers report that the cluster is now in view 69
	collector := &StateCollector{
		SelfID:         1,
		N:              4,
		Logger:         log,
		CollectTimeout: time.Second,
	}
	collector.Start()
	t.Cleanup(collector.Stop)

	comm := &peerStateComm{view: clusterView, seq: peerSeq}
	comm.onResponse = collector.HandleMessage

	state := &stateRecorder{}
	// the view changer is started so that it can be told about the view it learned
	viewChanger := &ViewChanger{
		N:             4,
		NodesList:     []uint64{1, 2, 3, 4},
		InMsqQSize:    100,
		Logger:        log,
		RequestsTimer: &stubRequestsTimer{},
	}
	viewChanger.Start(controllerView)
	t.Cleanup(viewChanger.Stop)

	c := &Controller{
		ID:                  1,
		N:                   4,
		NodesList:           []uint64{1, 2, 3, 4},
		Logger:              log,
		Comm:                comm,
		Synchronizer:        &stubSynchronizer{response: syncResponse},
		Checkpoint:          checkpoint,
		Collector:           collector,
		InFlight:            &InFlightData{},
		ViewChanger:         viewChanger,
		State:               state,
		currViewNumber:      controllerView,
		currDecisionsInView: controllerDecisions + 1,
	}
	c.syncChan = make(chan struct{}, 1)

	viewNum, _, _ := c.sync()

	assert.Equal(t, clusterView, viewNum,
		"the node did not learn from its peers that the cluster moved to a higher view")

	// and the node is told about it, so the view changer can move it there
	assert.NotNil(t, state.saved, "the node did not persist the view it learned from its peers")
	assert.Equal(t, clusterView, state.saved.GetNewView().ViewId)
}
