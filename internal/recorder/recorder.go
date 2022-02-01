// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
)

const (
	TypeSyncResponse        types.EventType = "SyncResponse"
	TypeDecisionAndResponse types.EventType = "DecisionAndResponse"
	TypeSignResponse        types.EventType = "SignResponse"
	TypeSignedProposal      types.EventType = "SignedProposal"
	TypeProposal            types.EventType = "Proposal"
	TypeMembershipChange    types.EventType = "MembershipChange"
)

func RegisterTypes() {
	types.RegisterDecoder(TypeSyncResponse, decodeSanitizedResponse)
	types.RegisterSanitizer(TypeSyncResponse, sanitizeSync)
	types.RegisterDecoder(TypeDecisionAndResponse, decodeSanitizedDecision)
	types.RegisterSanitizer(TypeDecisionAndResponse, sanitizeDecision)
	types.RegisterSanitizer(TypeSignResponse, sanitizeToNil)
	types.RegisterDecoder(TypeSignResponse, decodeFromNil)
	types.RegisterSanitizer(TypeSignedProposal, sanitizeSignedProposal)
	types.RegisterDecoder(TypeSignedProposal, decodeSanitizedSignedProposal)
	types.RegisterSanitizer(TypeProposal, sanitizeProposal)
	types.RegisterDecoder(TypeProposal, decodeSanitizedProposal)
	types.RegisterSanitizer(TypeMembershipChange, nothingToSanitize)
	types.RegisterDecoder(TypeMembershipChange, decodeBool)
}

type Proxy struct {
	Logger             api.Logger
	once               sync.Once
	in                 *bufio.Scanner
	Synchronizer       api.Synchronizer
	Application        api.Application
	Signer             api.Signer
	Assembler          api.Assembler
	MembershipNotifier api.MembershipNotifier
	Out                io.Writer
	In                 io.Reader
}

func (p *Proxy) getOrCreateInput() *bufio.Scanner {
	p.once.Do(func() {
		p.in = bufio.NewScanner(p.In)
	})
	return p.in
}

func (p *Proxy) nextRecord() interface{} {
	in := p.getOrCreateInput()
	if !in.Scan() {
		panic("reached end of file")
	}
	re := types.RecordedEvent{}
	re.FromString(in.Text())
	return re.Decode()
}

func (p *Proxy) write(re types.RecordedEvent) {
	fmt.Fprintln(p.Out, re)
}

func (p *Proxy) Sync() types.SyncResponse {
	if p.Out != nil {
		res := p.Synchronizer.Sync()
		re := types.NewRecordedEvent(TypeSyncResponse, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		res := p.nextRecord().(types.SyncResponse)
		return res
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) Deliver(proposal types.Proposal, signature []types.Signature) types.Reconfig {
	if p.Out != nil {
		res := p.Application.Deliver(proposal, signature)
		re := types.NewRecordedEvent(TypeDecisionAndResponse, DecisionAndResponse{
			Reconfig: res,
			Decision: types.Decision{Proposal: proposal, Signatures: signature},
		})
		p.write(re)
		return res
	}

	if p.In != nil {
		res := p.nextRecord().(DecisionAndResponse)
		return res.Reconfig
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) Sign(b []byte) []byte {
	if p.Out != nil {
		res := p.Signer.Sign(b)
		re := types.NewRecordedEvent(TypeSignResponse, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		p.nextRecord()
		return nil
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) SignProposal(proposal types.Proposal, auxiliaryInput []byte) *types.Signature {
	if p.Out != nil {
		res := p.Signer.SignProposal(proposal, auxiliaryInput)
		re := types.NewRecordedEvent(TypeSignedProposal, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		return p.nextRecord().(*types.Signature)
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	if p.Out != nil {
		res := p.Assembler.AssembleProposal(metadata, requests)
		re := types.NewRecordedEvent(TypeProposal, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		return p.nextRecord().(types.Proposal)
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) MembershipChange() bool {
	if p.Out != nil {
		res := p.MembershipNotifier.MembershipChange()
		re := types.NewRecordedEvent(TypeMembershipChange, res)
		p.write(re)
		return res
	}

	if p.In != nil {
		return p.nextRecord().(bool)
	}

	panic("programming error: in recording mode but no input nor output initialized")
}
