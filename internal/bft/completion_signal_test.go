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
	"google.golang.org/protobuf/proto"
)

func TestControllerDecideDoesNotBlockIfDeliveryWaiterLeft(t *testing.T) {
	metadata, err := proto.Marshal(&protos.ViewMetadata{})
	assert.NoError(t, err)

	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{Metadata: metadata}, nil)

	controller := &Controller{
		ID:         2,
		N:          4,
		NodesList:  []uint64{1, 2, 3, 4},
		Logger:     zap.NewNop().Sugar(),
		Deliver:    applicationFunc(func(types.Proposal, []types.Signature) types.Reconfig { return types.Reconfig{} }),
		Verifier:   noopVerifier{},
		Checkpoint: checkpoint,
		stopChan:   make(chan struct{}),
	}

	requireReturns(t, func() {
		controller.decide(decision{
			proposal:  types.Proposal{Metadata: metadata},
			delivered: make(chan struct{}),
		})
	})
}

func requireReturns(t *testing.T, f func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("function blocked")
	}
}

// These tests live in package bft to reach unexported completion paths, so they
// cannot use internal/bft/mocks without creating an import cycle.

type applicationFunc func(types.Proposal, []types.Signature) types.Reconfig

func (f applicationFunc) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	return f(proposal, signatures)
}

type noopVerifier struct{}

func (noopVerifier) VerificationSequence() uint64 {
	return 0
}

func (noopVerifier) VerifyProposal(types.Proposal) ([]types.RequestInfo, error) {
	panic("unexpected VerifyProposal call")
}

func (noopVerifier) VerifyRequest([]byte) (types.RequestInfo, error) {
	panic("unexpected VerifyRequest call")
}

func (noopVerifier) VerifyConsenterSig(types.Signature, types.Proposal) ([]byte, error) {
	panic("unexpected VerifyConsenterSig call")
}

func (noopVerifier) VerifySignature(types.Signature) error {
	panic("unexpected VerifySignature call")
}

func (noopVerifier) RequestsFromProposal(types.Proposal) []types.RequestInfo {
	panic("unexpected RequestsFromProposal call")
}

func (noopVerifier) AuxiliaryData([]byte) []byte {
	panic("unexpected AuxiliaryData call")
}
