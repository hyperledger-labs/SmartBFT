// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"go.uber.org/zap"
	"github.com/stretchr/testify/assert"
)

func TestViewBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	view := &bft.View{
		Logger: log,
		N:                4,
		LeaderID:         1,
		Number:           1,
		ProposalSequence: new(uint64),
	}
	view.Start()
	view.Abort()
}
