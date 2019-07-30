// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"encoding/asn1"

	"github.com/SmartBFT-Go/consensus/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type App struct {
	ID        uint64
	Delivered chan *Batch
	Consensus *consensus.Consensus
}

func (a *App) Submit(req Request) {
	a.Consensus.SubmitRequest(req.ToBytes())
}

func (a *App) Sync() (smartbftprotos.ViewMetadata, uint64) {
	panic("implement me")
}

func (a *App) RequestID(req []byte) types.RequestInfo {
	txn := requestFromBytes(req)
	return types.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

func (a *App) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	blockData := BatchFromBytes(proposal.Payload)
	requests := make([]types.RequestInfo, 0)
	for _, t := range blockData.Requests {
		req := requestFromBytes(t)
		reqInfo := types.RequestInfo{ID: req.ID, ClientID: req.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

func (a *App) VerifyRequest(val []byte) (types.RequestInfo, error) {
	req := requestFromBytes(val)
	return types.RequestInfo{ID: req.ID, ClientID: req.ClientID}, nil
}

func (a *App) VerifyConsenterSig(signature types.Signature, prop types.Proposal) error {
	return nil
}

func (a *App) VerificationSequence() uint64 {
	return 0
}

func (a *App) Sign([]byte) []byte {
	return nil
}

func (a *App) SignProposal(types.Proposal) *types.Signature {
	return &types.Signature{Id: a.ID}
}

func (a *App) AssembleProposal(metadata []byte, requests [][]byte) (nextProp types.Proposal, remainder [][]byte) {
	return types.Proposal{
		Payload:  Batch{Requests: requests}.ToBytes(),
		Metadata: metadata,
	}, nil
}

func (a *App) Deliver(proposal types.Proposal, signature []types.Signature) {
	a.Delivered <- BatchFromBytes(proposal.Payload)
}

type Request struct {
	ClientID string
	ID       string
}

func (txn Request) ToBytes() []byte {
	rawTxn, err := asn1.Marshal(txn)
	if err != nil {
		panic(err)
	}
	return rawTxn
}

func requestFromBytes(req []byte) *Request {
	var r Request
	asn1.Unmarshal(req, &r)
	return &r
}

type Batch struct {
	Requests [][]byte
}

func (b Batch) ToBytes() []byte {
	rawBlock, err := asn1.Marshal(b)
	if err != nil {
		panic(err)
	}
	return rawBlock
}

func BatchFromBytes(rawBlock []byte) *Batch {
	var block Batch
	asn1.Unmarshal(rawBlock, &block)
	return &block
}
