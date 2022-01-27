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
