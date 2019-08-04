// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

var (
	heartbeat = &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: 1,
				Seq:  0,
			},
		},
	}
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

func TestHeartbeatMonitor_StartLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	comm := &mocks.CommMock{}
	handler := &mocks.HeartbeatTimeoutHandler{}
	hm := bft.NewHeartbeatMonitor(log, time.Millisecond, comm)
	hm.SetTimeoutHandler(handler)

	count1 := 10
	count2 := 10
	toWG1 := &sync.WaitGroup{}
	toWG1.Add(1)
	toWG2 := &sync.WaitGroup{}
	toWG2.Add(1)
	comm.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		view := msg.GetHeartBeat().View
		if uint64(10) == view {
			count1--
			if count1 == 0 {
				toWG1.Done()
			}
		} else if uint64(20) == view {
			count2--
			if count2 == 0 {
				toWG2.Done()
			}
		}
		log.Debugf("On1: view: %d", view)
	}).Return()

	hm.StartLeader(10, 12)
	toWG1.Wait()
	hm.Close()
	hm.StartLeader(20, 12)
	toWG2.Wait()
	hm.Close()
}
