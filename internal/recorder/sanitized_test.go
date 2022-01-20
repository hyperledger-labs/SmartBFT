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

func TestSample(t *testing.T) {
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
