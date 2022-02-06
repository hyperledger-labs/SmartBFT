// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
)

const (
	TypeSyncResponse         types.EventType = "SyncResponse"
	TypeDecisionAndResponse  types.EventType = "DecisionAndResponse"
	TypeSignResponse         types.EventType = "SignResponse"
	TypeSignedProposal       types.EventType = "SignedProposal"
	TypeProposal             types.EventType = "Proposal"
	TypeMembershipChange     types.EventType = "MembershipChange"
	TypeVerifyProposal       types.EventType = "VerifyProposal"
	TypeVerifyRequest        types.EventType = "VerifyRequest"
	TypeVerifyConsenterSig   types.EventType = "VerifyConsenterSig"
	TypeVerifySignature      types.EventType = "VerifySignature"
	TypeVerificationSequence types.EventType = "VerificationSequence"
	TypeRequestsFromProposal types.EventType = "RequestsFromProposal"
	TypeAuxData              types.EventType = "AuxData"
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
	types.RegisterSanitizer(TypeVerifyProposal, nothingToSanitize)
	types.RegisterDecoder(TypeVerifyProposal, decodeVerifierResponses)
	types.RegisterSanitizer(TypeVerifyRequest, nothingToSanitize)
	types.RegisterDecoder(TypeVerifyRequest, decodeVerifierResponses)
	types.RegisterSanitizer(TypeVerifyConsenterSig, nothingToSanitize)
	types.RegisterDecoder(TypeVerifyConsenterSig, decodeVerifierResponses)
	types.RegisterSanitizer(TypeVerifySignature, nothingToSanitize)
	types.RegisterDecoder(TypeVerifySignature, decodeVerifierResponses)
	types.RegisterSanitizer(TypeVerificationSequence, nothingToSanitize)
	types.RegisterDecoder(TypeVerificationSequence, decodeVerifierResponses)
	types.RegisterSanitizer(TypeRequestsFromProposal, nothingToSanitize)
	types.RegisterDecoder(TypeRequestsFromProposal, decodeVerifierResponses)
	types.RegisterSanitizer(TypeAuxData, nothingToSanitize)
	types.RegisterDecoder(TypeAuxData, decodeVerifierResponses)
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
	Verifier           api.Verifier
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

func (p *Proxy) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	if p.Out != nil {
		ris, err := p.Verifier.VerifyProposal(proposal)
		errS := ""
		if err != nil {
			errS = err.Error()
		}
		re := types.NewRecordedEvent(TypeVerifyProposal, VerifierResponses{Ris: ris, Err: errS})
		p.write(re)
		return ris, err
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		if rec.Err != "" {
			return rec.Ris, errors.New(rec.Err)
		}
		return rec.Ris, nil
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) VerifyRequest(val []byte) (types.RequestInfo, error) {
	if p.Out != nil {
		ri, err := p.Verifier.VerifyRequest(val)
		ris := []types.RequestInfo{ri}
		errS := ""
		if err != nil {
			errS = err.Error()
		}
		re := types.NewRecordedEvent(TypeVerifyRequest, VerifierResponses{Ris: ris, Err: errS})
		p.write(re)
		return ri, err
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		if rec.Err != "" {
			return rec.Ris[0], errors.New(rec.Err)
		}
		return rec.Ris[0], nil
	}

	panic("programming error: in recording mode but no input nor output initialized")

}

func (p *Proxy) VerifyConsenterSig(signature types.Signature, prop types.Proposal) ([]byte, error) {
	if p.Out != nil {
		aux, err := p.Verifier.VerifyConsenterSig(signature, prop)
		errS := ""
		if err != nil {
			errS = err.Error()
		}
		re := types.NewRecordedEvent(TypeVerifyConsenterSig, VerifierResponses{Aux: aux, Err: errS})
		p.write(re)
		return aux, err
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		if rec.Err != "" {
			return rec.Aux, errors.New(rec.Err)
		}
		return rec.Aux, nil
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) VerifySignature(signature types.Signature) error {
	if p.Out != nil {
		err := p.Verifier.VerifySignature(signature)
		errS := ""
		if err != nil {
			errS = err.Error()
		}
		re := types.NewRecordedEvent(TypeVerifySignature, VerifierResponses{Err: errS})
		p.write(re)
		return err
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		if rec.Err != "" {
			return errors.New(rec.Err)
		}
		return nil
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) VerificationSequence() uint64 {
	if p.Out != nil {
		seq := p.Verifier.VerificationSequence()
		re := types.NewRecordedEvent(TypeVerificationSequence, VerifierResponses{Seq: seq})
		p.write(re)
		return seq
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		return rec.Seq
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) RequestsFromProposal(proposal types.Proposal) []types.RequestInfo {
	if p.Out != nil {
		ris := p.Verifier.RequestsFromProposal(proposal)
		re := types.NewRecordedEvent(TypeRequestsFromProposal, VerifierResponses{Ris: ris})
		p.write(re)
		return ris
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		return rec.Ris
	}

	panic("programming error: in recording mode but no input nor output initialized")
}

func (p *Proxy) AuxiliaryData(sig []byte) []byte {
	if p.Out != nil {
		aux := p.Verifier.AuxiliaryData(sig)
		re := types.NewRecordedEvent(TypeAuxData, VerifierResponses{Aux: aux})
		p.write(re)
		return aux
	}

	if p.In != nil {
		rec := p.nextRecord().(VerifierResponses)
		return rec.Aux
	}

	panic("programming error: in recording mode but no input nor output initialized")
}
