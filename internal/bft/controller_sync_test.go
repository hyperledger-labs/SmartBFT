// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"
	"time"

	"github.com/hyperledger-labs/SmartBFT/pkg/types"
	protos "github.com/hyperledger-labs/SmartBFT/smartbftprotos"
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

// TestSyncDecisionsInView tests that the sync() method correctly preserves
// the DecisionsInView counter. This test reproduces a bug where sync()
// resets DecisionsInView to 0 when the synchronizer returns the same block
// height as the controller (the "already at target height" case).
//
// Bug location: controller.go, sync() function.
// Line 610: newDecisionsInView is zero-initialized.
// Line 635: only updated when latestDecisionSeq > controllerSequence.
// When latestDecisionSeq == controllerSequence, newDecisionsInView stays 0.
//
// Real-world impact: after sync returns same height, changeView() resets
// DecisionsInView from ~9578 to 0. The next proposal carries the correct
// DecisionsInView=9578, which fails validation at view.go:577, causing the
// orderer to reject valid proposals and enter a recovery sync loop.
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
			ID:             1,
			N:              4,
			Logger:         log,
			Comm:           &stubComm{},
			Synchronizer:   &stubSynchronizer{response: syncResponse},
			Checkpoint:     checkpoint,
			Collector:      collector,
			InFlight:       &InFlightData{},
			ViewChanger:    &ViewChanger{},
			currViewNumber: controllerView,
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
		// BUG: sync() returns decisions=0 because the condition at line 635
		// (latestDecisionSeq > controllerSequence) is false when they're equal,
		// so newDecisionsInView is never set from the zero-initialized value.
		// The correct value should be controllerDecisions+1 (9579).
		assert.Equal(t, controllerDecisions+1, decisions,
			"DecisionsInView must be preserved when sync returns same height; "+
				"got 0 means the bug at controller.go:635 is still present")
	})

	t.Run("higher_height_preserves_decisions", func(t *testing.T) {
		// Synchronizer returns a higher sequence than the controller.
		// This is the working code path (line 641).
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
