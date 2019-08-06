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

	hm := bft.NewHeartbeatMonitor(log, time.Hour, comm, handler)
	assert.NotNil(t, hm)
	hm.Close()
}

func TestHeartbeatMonitor_StartLeader(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	comm := &mocks.CommMock{}
	handler := &mocks.HeartbeatTimeoutHandler{}
	hm := bft.NewHeartbeatMonitor(log, time.Millisecond, comm, handler)

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
		log.Debugf("On: view: %d", view)
	}).Return()

	hm.StartLeader(10, 12)
	toWG1.Wait()
	hm.StartLeader(20, 12)
	toWG2.Wait()
	hm.Close()
}

func TestHeartbeatMonitor_Follower(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	t.Run("timeout expires", func(t *testing.T) {
		comm := &mocks.CommMock{}
		handler := &mocks.HeartbeatTimeoutHandler{}

		hm := bft.NewHeartbeatMonitor(log, 10*time.Millisecond, comm, handler)

		toWG := &sync.WaitGroup{}
		toWG.Add(1)
		handler.On("OnHeartbeatTimeout", uint64(10), uint64(12)).Run(func(args mock.Arguments) {
			toWG.Done()
		}).Return()

		hm.StartFollower(10, 12)
		toWG.Wait()

		hm.Close()
	})

	t.Run("heartbeats prevent timeout", func(t *testing.T) {
		comm := &mocks.CommMock{}
		handler := &mocks.HeartbeatTimeoutHandler{}

		hbTO := time.Second
		hm := bft.NewHeartbeatMonitor(log, hbTO, comm, handler)

		handler.On("OnHeartbeatTimeout", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			t.Fail()
		}).Return()

		done := make(chan bool)
		go func() {
			for i := 0; i < 30; i++ {
				hb := heartbeat.GetHeartBeat()
				hb.View = 10
				hm.ProcessMsg(12, hb)
				time.Sleep(hbTO / 10)
			}
			close(done)
		}()

		hm.StartFollower(10, 12)
		<-done
		hm.Close()
	})

	t.Run("close prevents timeout", func(t *testing.T) {
		comm := &mocks.CommMock{}
		handler := &mocks.HeartbeatTimeoutHandler{}

		hbTO := 500 * time.Millisecond
		hm := bft.NewHeartbeatMonitor(log, hbTO, comm, handler)

		handler.On("OnHeartbeatTimeout", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			t.Fail()
		}).Return()

		hm.StartFollower(10, 12)
		hm.Close()
		time.Sleep(hbTO * 2)
	})

	t.Run("bad heartbeats do not prevent timeout", func(t *testing.T) {
		comm := &mocks.CommMock{}
		handler := &mocks.HeartbeatTimeoutHandler{}

		hbTO := time.Second
		hm := bft.NewHeartbeatMonitor(log, hbTO, comm, handler)

		toWG := &sync.WaitGroup{}
		toWG.Add(1)
		handler.On("OnHeartbeatTimeout", uint64(10), uint64(12)).Run(func(args mock.Arguments) {
			toWG.Done()
		}).Return()

		done := make(chan bool)
		go func() {
			for i := 0; i < 20; i++ {
				time.Sleep(hbTO / 10)
				// either 9,12 or 10,13, both are wrong.
				hb := heartbeat.GetHeartBeat()
				hb.View = uint64(9 + i%2)
				hm.ProcessMsg(uint64(12+i%2), hb)
			}
			close(done)
		}()

		hm.StartFollower(10, 12)
		<-done
		toWG.Wait()

		hm.Close()
	})

	t.Run("follow new view", func(t *testing.T) {
		comm := &mocks.CommMock{}
		handler := &mocks.HeartbeatTimeoutHandler{}

		hbTO := time.Second
		hm := bft.NewHeartbeatMonitor(log, hbTO, comm, handler)

		toWG := &sync.WaitGroup{}
		toWG.Add(1)
		handler.On("OnHeartbeatTimeout", uint64(11), uint64(13)).Run(func(args mock.Arguments) {
			toWG.Done()
		}).Return()

		done := make(chan bool)
		go func() {
			for i := 0; i < 20; i++ {
				time.Sleep(hbTO / 10)
				hb := heartbeat.GetHeartBeat()
				hb.View = uint64(10)
				hm.ProcessMsg(uint64(12), hb)
			}
			close(done)
		}()

		hm.StartFollower(10, 12)
		time.Sleep(hbTO / 5)
		hm.StartFollower(11, 13)
		<-done
		toWG.Wait()

		hm.Close()
	})
}

func TestHeartbeatMonitor_LeaderAndFollower(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	hbTO := time.Second

	comm1 := &mocks.CommMock{}
	handler1 := &mocks.HeartbeatTimeoutHandler{}
	hm1 := bft.NewHeartbeatMonitor(log, hbTO, comm1, handler1)

	comm2 := &mocks.CommMock{}
	handler2 := &mocks.HeartbeatTimeoutHandler{}
	hm2 := bft.NewHeartbeatMonitor(log, hbTO, comm2, handler2)

	comm1.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		go func() { hm2.ProcessMsg(1, msg.GetHeartBeat()) }()
	}).Return()

	comm2.On("BroadcastConsensus", mock.AnythingOfType("*smartbftprotos.Message")).Run(func(args mock.Arguments) {
		msg := args[0].(*smartbftprotos.Message)
		go func() { hm1.ProcessMsg(2, msg.GetHeartBeat()) }()
	}).Return()

	toWG := &sync.WaitGroup{}
	toWG.Add(1)
	handler1.On("OnHeartbeatTimeout", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		view := args[0].(uint64)
		if view != 12 {
			t.Fail()
		} else {
			toWG.Done()
		}
	}).Return()
	handler2.On("OnHeartbeatTimeout", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		t.Fail()
	}).Return()

	hm1.StartLeader(10, 1)
	hm2.StartFollower(10, 1)
	time.Sleep(2 * hbTO)

	hm1.StartFollower(11, 2)
	hm2.StartLeader(11, 2)
	time.Sleep(2 * hbTO)

	hm1.StartFollower(12, 2)
	hm2.StartLeader(12, 2)
	time.Sleep(hbTO / 5)
	hm2.Close()
	toWG.Wait()
	hm1.Close()
}
