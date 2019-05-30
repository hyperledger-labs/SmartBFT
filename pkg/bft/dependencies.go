// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import "github.com/SmartBFT-Go/consensus/protos"

type Signature struct {
	Id    uint64
	Value []byte
}

type Proposal struct {
	Payload              []byte
	Header               []byte
	Metadata             []byte
	VerificationSequence uint64
}

type Comm interface {
	Broadcast(m *protos.Message)
	Send(targetID uint64, message *protos.Message)
}

type Assembler interface {
	AssembleProposal(metadata []byte, requests [][]byte) (nextProp Proposal, remainder [][]byte)
}

type WriteAheadLog interface {
	Append(entry []byte)
	Read() [][]byte
	Truncate()
}

type Signer interface {
	Sign([]byte) []byte
	SignProposal(Proposal) []byte
}

type Verifier interface {
	VerifyProposal(proposal Proposal, prevHeader []byte) error
	VerifyRequest(val []byte) error
	VerifyConsenterSig(signer uint, signature []byte, m []byte) error
	VerificationSequence() uint64
}

type RequestInfo struct {
	ID       string
	ClientID string
}

type RequestInspector interface {
	RequestID(req []byte) RequestInfo
}

type Synchronizer interface {
	Sync() uint64
}
