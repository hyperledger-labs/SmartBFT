// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package recorder

import (
	"testing"

	. "github.com/SmartBFT-Go/consensus/pkg/types"
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
