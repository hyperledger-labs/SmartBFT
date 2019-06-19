// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package api

import (
	bft "github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type Application interface {
	Deliver(proposal bft.Proposal, signature []bft.Signature)
}

type Comm interface {
	Broadcast(m *protos.Message) // broadcast message to others (not including yourself)
	Send(targetID uint64, message *protos.Message)
}

type Assembler interface {
	AssembleProposal(metadata []byte, requests [][]byte) (nextProp bft.Proposal, remainder [][]byte)
}

type WriteAheadLog interface {
	Append(entry []byte)
	Read() [][]byte
	Truncate()
}

type Signer interface {
	Sign([]byte) []byte
	SignProposal(bft.Proposal) *bft.Signature
}

type Verifier interface {
	VerifyProposal(proposal bft.Proposal, prevHeader []byte) error
	VerifyRequest(val []byte) error
	VerifyConsenterSig(signer uint64, signature []byte, prop bft.Proposal) error
	VerificationSequence() uint64
}

type RequestInspector interface {
	RequestID(req []byte) bft.RequestInfo
}

type Synchronizer interface {
	Sync() (protos.BlockMetadata, uint64)
}

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}
