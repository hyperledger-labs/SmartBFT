// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestInFlightProposalLatest(t *testing.T) {
	prop := types.Proposal{
		VerificationSequence: 1,
		Metadata:             []byte{1},
		Payload:              []byte{2},
		Header:               []byte{3},
	}

	ifp := &InFlightData{}
	assert.Nil(t, ifp.InFlightProposal())

	ifp.StoreProposal(prop)
	assert.Equal(t, prop, *ifp.InFlightProposal())
}

func TestQuorum(t *testing.T) {
	// Ensure that quorum size is as expected.

	type quorum struct {
		N uint64
		F int
		Q int
	}

	quorums := []quorum{{4, 1, 3}, {5, 1, 4}, {6, 1, 4}, {7, 2, 5}, {8, 2, 6},
		{9, 2, 6}, {10, 3, 7}, {11, 3, 8}, {12, 3, 8}}

	for _, testCase := range quorums {
		t.Run(fmt.Sprintf("%d nodes", testCase.N), func(t *testing.T) {
			Q, F := computeQuorum(testCase.N)
			assert.Equal(t, testCase.Q, Q)
			assert.Equal(t, testCase.F, F)
		})
	}

}
