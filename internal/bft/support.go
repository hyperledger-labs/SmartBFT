// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

// A collection of interfaces that are also defined in api/dependencies.go

//go:generate mockery -dir . -name Verifier -case underscore -output ./mocks/
type Verifier interface {
	VerifyProposal(proposal types.Proposal, prevHeader []byte) ([]types.RequestInfo, error)
	VerifyRequest(val []byte) (types.RequestInfo, error)
	VerifyConsenterSig(signature types.Signature, prop types.Proposal) error
	VerificationSequence() uint64
}

//go:generate mockery -dir . -name Assembler -case underscore -output ./mocks/
type Assembler interface {
	AssembleProposal(metadata []byte, requests [][]byte) (nextProp types.Proposal, remainder [][]byte)
}

//go:generate mockery -dir . -name Application -case underscore -output ./mocks/
type Application interface {
	Deliver(proposal types.Proposal, signature []types.Signature)
}

//go:generate mockery -dir . -name Comm -case underscore -output ./mocks/
type Comm interface {
	BroadcastConsensus(m *protos.Message)
	SendConsensus(targetID uint64, m *protos.Message)
	SendTransaction(targetID uint64, request []byte)
}

//go:generate mockery -dir . -name Synchronizer -case underscore -output ./mocks/
type Synchronizer interface {
	Sync() (protos.ViewMetadata, uint64)
}

//go:generate mockery -dir . -name Signer -case underscore -output ./mocks/
type Signer interface {
	Sign([]byte) []byte
	SignProposal(types.Proposal) *types.Signature
}
