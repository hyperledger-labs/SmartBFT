// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
)

func TestSchedule(t *testing.T) {
	timeChan := make(chan time.Time)
	s := bft.NewScheduler(timeChan)
	s.Start()
	s.Stop()
}
