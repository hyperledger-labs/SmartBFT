// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/internal/bft"
)

func TestViewBasic(t *testing.T) {
	view := &bft.View{
		N:                4,
		LeaderID:         1,
		Number:           1,
		ProposalSequence: new(uint64),
	}
	view.Start()
	view.Abort()
}
