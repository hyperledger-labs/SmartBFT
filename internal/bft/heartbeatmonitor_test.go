// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestHeartbeatMonitor_New(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	comm := &mocks.CommMock{}
	handler := &mocks.HeartbeatTimeoutHandler{}

	hm := bft.NewHeartbeatMonitor(log, time.Hour, comm)
	assert.NotNil(t, hm)
	hm.SetTimeoutHandler(handler)
	hm.Close()
}
