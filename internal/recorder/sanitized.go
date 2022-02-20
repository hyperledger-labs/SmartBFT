// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

func decodeSanitizedResponse(in []byte) interface{} {
	var ssr SanitizedSyncResponse
	if err := json.Unmarshal(in, &ssr); err != nil {
		panic(fmt.Sprintf("failed unmarshaling %s to SanitizedSyncResponse: %v", base64.StdEncoding.EncodeToString(in), err))
	}

	var sr types.SyncResponse
	sr.Reconfig = ssr.Reconfig
	sr.Latest.Signatures = ssr.Latest.Signatures
	sr.Latest.Proposal.VerificationSequence = ssr.Latest.Proposal.VerificationSequence
	sr.Latest.Proposal.Header = ssr.Latest.Proposal.Header
	sr.Latest.Proposal.Metadata = ssr.Latest.Proposal.Metadata

	return sr
}

func sanitizeSync(in interface{}) interface{} {
	sr, isSyncResponse := in.(types.SyncResponse)
	if !isSyncResponse {
		panic(fmt.Sprintf("expected object of type SyncResponse but got %s", reflect.TypeOf(in)))
	}

	var ssr SanitizedSyncResponse
	ssr.Reconfig = sr.Reconfig
	// Copy by reference everything but Payload
	ssr.Latest.Signatures = sr.Latest.Signatures
	ssr.Latest.Proposal.Header = sr.Latest.Proposal.Header
	ssr.Latest.Proposal.Metadata = sr.Latest.Proposal.Metadata
	ssr.Latest.Proposal.VerificationSequence = sr.Latest.Proposal.VerificationSequence
	return ssr
}

type SanitizedSyncResponse struct {
	Reconfig types.ReconfigSync
	Latest   SanitizedDecision
}

type SanitizedDecision struct {
	Proposal   SanitizedProposal
	Signatures []types.Signature
}

type SanitizedProposal struct {
	Header               []byte
	Metadata             []byte
	VerificationSequence int64 // int64 for asn1 marshaling
}

func sanitizeProposal(in interface{}) interface{} {
	p, isProposal := in.(types.Proposal)
	if !isProposal {
		panic(fmt.Sprintf("expected object of type Proposal but got %s", reflect.TypeOf(in)))
	}

	var sp SanitizedProposal
	sp.Header = p.Header
	sp.Metadata = p.Metadata
	sp.VerificationSequence = p.VerificationSequence

	return sp
}

func decodeSanitizedProposal(in []byte) interface{} {
	var sp SanitizedProposal
	if err := json.Unmarshal(in, &sp); err != nil {
		panic(fmt.Sprintf("failed unmarshaling %s to SanitizedProposal: %v", base64.StdEncoding.EncodeToString(in), err))
	}

	var p types.Proposal
	p.Header = sp.Header
	p.Metadata = sp.Metadata
	p.VerificationSequence = sp.VerificationSequence

	return p
}

type DecisionAndResponse struct {
	Reconfig types.Reconfig
	Decision types.Decision
}

type SanitizeDecisionResponse struct {
	Reconfig types.Reconfig
	Decision SanitizedDecision
}

func sanitizeDecision(in interface{}) interface{} {
	dr, isDeliver := in.(DecisionAndResponse)
	if !isDeliver {
		panic(fmt.Sprintf("expected object of type DecisionAndResponse but got %s", reflect.TypeOf(in)))
	}

	var sdr SanitizeDecisionResponse
	sdr.Reconfig = dr.Reconfig
	// Copy by reference everything but Payload
	sdr.Decision.Signatures = dr.Decision.Signatures
	sdr.Decision.Proposal.Header = dr.Decision.Proposal.Header
	sdr.Decision.Proposal.Metadata = dr.Decision.Proposal.Metadata
	sdr.Decision.Proposal.VerificationSequence = dr.Decision.Proposal.VerificationSequence
	return sdr
}

func decodeSanitizedDecision(in []byte) interface{} {
	var sdr SanitizeDecisionResponse
	if err := json.Unmarshal(in, &sdr); err != nil {
		panic(fmt.Sprintf("failed unmarshaling %s to SanitizeDecisionResponse: %v", base64.StdEncoding.EncodeToString(in), err))
	}

	var dr DecisionAndResponse
	dr.Reconfig = sdr.Reconfig
	dr.Decision.Signatures = sdr.Decision.Signatures
	dr.Decision.Proposal.VerificationSequence = sdr.Decision.Proposal.VerificationSequence
	dr.Decision.Proposal.Header = sdr.Decision.Proposal.Header
	dr.Decision.Proposal.Metadata = sdr.Decision.Proposal.Metadata

	return dr
}

type SanitizedSignature struct {
	ID uint64
}

func sanitizeSignedProposal(in interface{}) interface{} {
	sp, isSignedProposal := in.(*types.Signature)
	if !isSignedProposal {
		panic(fmt.Sprintf("expected object of type Signature but got %s", reflect.TypeOf(in)))
	}

	return SanitizedSignature{ID: sp.ID}
}

func decodeSanitizedSignedProposal(in []byte) interface{} {
	var sSign SanitizedSignature
	if err := json.Unmarshal(in, &sSign); err != nil {
		panic(fmt.Sprintf("failed unmarshaling %s to SanitizedSignature: %v", base64.StdEncoding.EncodeToString(in), err))
	}

	return &types.Signature{ID: sSign.ID}
}

func sanitizeToNil(in interface{}) interface{} {
	return nil
}

func decodeFromNil(in []byte) interface{} {
	return nil
}

func nothingToSanitize(in interface{}) interface{} {
	return in
}

func decodeBool(in []byte) interface{} {
	var b bool
	if err := json.Unmarshal(in, &b); err != nil {
		panic(fmt.Sprintf("failed unmarshaling %s to bool: %v", base64.StdEncoding.EncodeToString(in), err))
	}
	return b
}

type RecordedMessage struct {
	Sender uint64
	M      *protos.Message
}

func sanitizePrePrepare(in interface{}) interface{} {
	rm, isRecordedMessage := in.(RecordedMessage)
	if !isRecordedMessage {
		panic(fmt.Sprintf("expected object of type RecordedMessage but got %s", reflect.TypeOf(in)))
	}
	srm := RecordedMessage{}
	srm.Sender = rm.Sender
	pp := rm.M.GetPrePrepare()
	if pp == nil {
		panic("expected message of type PrePrepare")
	}
	var sanitizedPrevCommitSignatures []*protos.Signature
	for _, s := range pp.PrevCommitSignatures {
		ss := &protos.Signature{Signer: s.Signer}
		sanitizedPrevCommitSignatures = append(sanitizedPrevCommitSignatures, ss)
	}
	srm.M = &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: pp.View,
				Seq:  pp.Seq,
				Proposal: &protos.Proposal{
					Header:               pp.Proposal.Header,
					Payload:              nil, // sanitized
					Metadata:             pp.Proposal.Metadata,
					VerificationSequence: pp.Proposal.VerificationSequence,
				},
				PrevCommitSignatures: sanitizedPrevCommitSignatures, // sanitized
			},
		},
	}
	return srm
}

func sanitizeCommit(in interface{}) interface{} {
	rm, isRecordedMessage := in.(RecordedMessage)
	if !isRecordedMessage {
		panic(fmt.Sprintf("expected object of type RecordedMessage but got %s", reflect.TypeOf(in)))
	}
	srm := RecordedMessage{}
	srm.Sender = rm.Sender
	cmt := rm.M.GetCommit()
	if cmt == nil {
		panic("expected message of type Commit")
	}
	srm.M = &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:      cmt.View,
				Seq:       cmt.Seq,
				Digest:    cmt.Digest,
				Signature: &protos.Signature{Signer: cmt.Signature.Signer}, // sanitized
				Assist:    cmt.Assist,
			},
		},
	}
	return srm
}

func sanitizeViewData(in interface{}) interface{} {
	rm, isRecordedMessage := in.(RecordedMessage)
	if !isRecordedMessage {
		panic(fmt.Sprintf("expected object of type RecordedMessage but got %s", reflect.TypeOf(in)))
	}
	srm := RecordedMessage{}
	srm.Sender = rm.Sender
	svd := rm.M.GetViewData()
	if svd == nil {
		panic("expected message of type ViewData")
	}
	srm.M = &protos.Message{
		Content: &protos.Message_ViewData{
			ViewData: sanitizeSignedViewData(svd),
		},
	}
	return srm
}

func sanitizeSignedViewData(svd *protos.SignedViewData) *protos.SignedViewData {
	vd := &protos.ViewData{}
	if err := proto.Unmarshal(svd.RawViewData, vd); err != nil {
		panic("unable to unmarshal ViewData message")
	}
	var sanitizedLastDecisionSignatures []*protos.Signature
	for _, s := range vd.LastDecisionSignatures {
		ss := &protos.Signature{Signer: s.Signer}
		sanitizedLastDecisionSignatures = append(sanitizedLastDecisionSignatures, ss)
	}
	sanitizedVD := &protos.ViewData{
		NextView: vd.NextView,
		LastDecision: &protos.Proposal{
			Header:               vd.LastDecision.Header,
			Payload:              nil,
			Metadata:             vd.LastDecision.Metadata,
			VerificationSequence: vd.LastDecision.VerificationSequence,
		},
		LastDecisionSignatures: sanitizedLastDecisionSignatures,
		InFlightProposal:       nil,
		InFlightPrepared:       vd.InFlightPrepared,
	}
	if vd.InFlightProposal != nil {
		sanitizedVD.InFlightProposal = &protos.Proposal{
			Header:               vd.InFlightProposal.Header,
			Payload:              nil,
			Metadata:             vd.InFlightProposal.Metadata,
			VerificationSequence: vd.InFlightProposal.VerificationSequence,
		}
	}
	vdBytes, err := proto.Marshal(sanitizedVD)
	if err != nil {
		panic(err)
	}
	return &protos.SignedViewData{
		RawViewData: vdBytes,
		Signer:      svd.Signer,
		Signature:   svd.Signature,
	}
}

func sanitizeNewView(in interface{}) interface{} {
	rm, isRecordedMessage := in.(RecordedMessage)
	if !isRecordedMessage {
		panic(fmt.Sprintf("expected object of type RecordedMessage but got %s", reflect.TypeOf(in)))
	}
	srm := RecordedMessage{}
	srm.Sender = rm.Sender
	nv := rm.M.GetNewView()
	if nv == nil {
		panic("expected message of type NewView")
	}
	var sanitizedSignedViewDataMessages []*protos.SignedViewData
	for _, svd := range nv.SignedViewData {
		sanitized := sanitizeSignedViewData(svd)
		sanitizedSignedViewDataMessages = append(sanitizedSignedViewDataMessages, sanitized)
	}
	srm.M = &protos.Message{
		Content: &protos.Message_NewView{
			NewView: &protos.NewView{
				SignedViewData: sanitizedSignedViewDataMessages,
			},
		},
	}
	return srm
}
