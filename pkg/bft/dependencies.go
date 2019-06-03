// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"encoding/asn1"
	"fmt"

	"github.com/SmartBFT-Go/consensus/protos"
)

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

func (p Proposal) Digest() string {
	rawBytes, err := asn1.Marshal(Proposal{
		VerificationSequence: p.VerificationSequence,
		Metadata:             p.Metadata,
		Payload:              p.Payload,
		Header:               p.Header,
	})

	if err != nil {
		panic(fmt.Sprintf("failed marshaling proposal: %v", err))
	}

	return ComputeDigest(rawBytes)
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
	SignProposal(Proposal) *Signature
}

type Verifier interface {
	VerifyProposal(proposal Proposal, prevHeader []byte) error
	VerifyRequest(val []byte) error
	VerifyConsenterSig(signer uint64, signature []byte, prop Proposal) error
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

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}
