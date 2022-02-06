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

type VerifierResponses struct {
	Ris []types.RequestInfo
	Err string
	Seq uint64
	Aux []byte
}

func decodeVerifierResponses(in []byte) interface{} {
	var vr VerifierResponses
	if err := json.Unmarshal(in, &vr); err != nil {
		panic(fmt.Sprintf("failed unmarshaling %s to VerifierResponses: %v", base64.StdEncoding.EncodeToString(in), err))
	}
	return vr
}
