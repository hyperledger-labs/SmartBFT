// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"testing"

	. "github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestSync(t *testing.T) {
	syncResponse := SyncResponse{
		Reconfig: ReconfigSync{
			CurrentConfig: Configuration{
				RequestBatchMaxBytes: 1,
				RequestMaxBytes:      1,
			},
			InReplicatedDecisions: true,
			CurrentNodes:          []uint64{1, 2, 3, 4},
		},
		Latest: Decision{
			Signatures: []Signature{{
				ID:    1,
				Msg:   []byte{1, 2, 3},
				Value: []byte{4, 5, 6},
			}},
			Proposal: Proposal{
				Metadata: []byte{1, 2, 3},
				Header:   []byte{4, 5, 6},
				Payload:  []byte{7, 8, 9},
			},
		},
	}

	RegisterDecoder(TypeSyncResponse, decodeSanitizedResponse)
	RegisterSanitizer(TypeSyncResponse, sanitizeSync)

	re := NewRecordedEvent(TypeSyncResponse, syncResponse)
	// Make sure we can encode to string and decode from string without losing information
	re2 := RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	syncResponse2 := re2.Decode().(SyncResponse)
	// Nullify payload since we sanitized
	syncResponse.Latest.Proposal.Payload = nil
	// Ensure the original sync response was decoded from the string properly
	assert.Equal(t, syncResponse, syncResponse2)
}

func TestDeliver(t *testing.T) {
	decisionAndResponse := DecisionAndResponse{
		Reconfig: Reconfig{
			CurrentConfig: Configuration{
				RequestBatchMaxBytes: 1,
				RequestMaxBytes:      1,
			},
			InLatestDecision: true,
			CurrentNodes:     []uint64{1, 2, 3, 4},
		},
		Decision: Decision{
			Signatures: []Signature{{
				ID:    1,
				Msg:   []byte{1, 2, 3},
				Value: []byte{4, 5, 6},
			}},
			Proposal: Proposal{
				Metadata: []byte{1, 2, 3},
				Header:   []byte{4, 5, 6},
				Payload:  []byte{7, 8, 9},
			},
		},
	}

	RegisterDecoder(TypeDecisionAndResponse, decodeSanitizedDecision)
	RegisterSanitizer(TypeDecisionAndResponse, sanitizeDecision)

	re := NewRecordedEvent(TypeDecisionAndResponse, decisionAndResponse)
	// Make sure we can encode to string and decode from string without losing information
	re2 := RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	decisionAndResponse2 := re2.Decode().(DecisionAndResponse)
	// Nullify payload since we sanitized
	decisionAndResponse.Decision.Proposal.Payload = nil
	// Ensure the original response was decoded from the string properly
	assert.Equal(t, decisionAndResponse, decisionAndResponse2)
}

func TestMembershipChange(t *testing.T) {
	RegisterDecoder(TypeMembershipChange, decodeBool)
	RegisterSanitizer(TypeMembershipChange, nothingToSanitize)
	re := NewRecordedEvent(TypeMembershipChange, true)
	re2 := RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	true2 := re2.Decode().(bool)
	assert.True(t, true2)
}

func TestVerifier(t *testing.T) {
	RegisterSanitizer(TypeVerifyProposal, nothingToSanitize)
	RegisterDecoder(TypeVerifyProposal, decodeVerifierResponses)
	ris := []RequestInfo{{ClientID: "1", ID: "1"}, {ClientID: "2", ID: "2"}, {ClientID: "3", ID: "3"}}
	err := errors.Errorf("verifier error")
	re := NewRecordedEvent(TypeVerifyProposal, VerifierResponses{Ris: ris, Err: err.Error()})
	re2 := RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	vr := re2.Decode().(VerifierResponses)
	assert.Equal(t, vr.Err, "verifier error")
	assert.Len(t, vr.Ris, 3)

	RegisterSanitizer(TypeVerifyRequest, nothingToSanitize)
	RegisterDecoder(TypeVerifyRequest, decodeVerifierResponses)
	ri := RequestInfo{ClientID: "10", ID: "10"}
	re = NewRecordedEvent(TypeVerifyRequest, VerifierResponses{Ris: []RequestInfo{ri}, Err: err.Error()})
	re2 = RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	vr = re2.Decode().(VerifierResponses)
	assert.Equal(t, vr.Err, "verifier error")
	assert.Len(t, vr.Ris, 1)

	RegisterSanitizer(TypeVerifyConsenterSig, nothingToSanitize)
	RegisterDecoder(TypeVerifyConsenterSig, decodeVerifierResponses)
	re = NewRecordedEvent(TypeVerifyConsenterSig, VerifierResponses{Aux: []byte{1, 2}, Err: err.Error()})
	re2 = RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	vr = re2.Decode().(VerifierResponses)
	assert.Equal(t, vr.Err, "verifier error")
	assert.Len(t, vr.Aux, 2)

	RegisterSanitizer(TypeVerifySignature, nothingToSanitize)
	RegisterDecoder(TypeVerifySignature, decodeVerifierResponses)
	re = NewRecordedEvent(TypeVerifySignature, VerifierResponses{Err: err.Error()})
	re2 = RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	vr = re2.Decode().(VerifierResponses)
	assert.Errorf(t, errors.New(vr.Err), "verifier error")

	RegisterSanitizer(TypeVerificationSequence, nothingToSanitize)
	RegisterDecoder(TypeVerificationSequence, decodeVerifierResponses)
	re = NewRecordedEvent(TypeVerificationSequence, VerifierResponses{Seq: 100})
	re2 = RecordedEvent{}
	re2.FromString(re.String())
	assert.Equal(t, re, re2)
	vr = re2.Decode().(VerifierResponses)
	assert.Equal(t, uint64(100), vr.Seq)

}
