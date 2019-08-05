// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

const (
	DefaultHeartbeatTimeout = 60 * time.Second
)

//go:generate mockery -dir . -name HeartbeatTimeoutHandler -case underscore -output ./mocks/

// HeartbeatTimeoutHandler defines who to call when a heartbeat timeout expires.
type HeartbeatTimeoutHandler interface {
	OnHeartbeatTimeout(view uint64, leaderID uint64)
}

type HeartbeatMonitor struct {
	logger     api.Logger
	hbTimeout  time.Duration
	hbInterval time.Duration
	comm       Comm
	handler    HeartbeatTimeoutHandler
	mutex      sync.Mutex
	timer      *time.Timer
	view       uint64
	leaderID   uint64
	follower   bool
}

func NewHeartbeatMonitor(
	logger api.Logger,
	heartbeatTimeout time.Duration,
	comm Comm,
) *HeartbeatMonitor {
	if heartbeatTimeout/10 < time.Nanosecond {
		return nil
	}

	hm := &HeartbeatMonitor{
		logger:     logger,
		hbTimeout:  heartbeatTimeout,
		hbInterval: heartbeatTimeout / 10,
		comm:       comm,
	}
	return hm
}

func (hm *HeartbeatMonitor) SetTimeoutHandler(handler HeartbeatTimeoutHandler) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	hm.handler = handler
}

// StartFollower will start following the heartbeats of the leader of the view.
func (hm *HeartbeatMonitor) StartFollower(view uint64, leaderID uint64) {
	//TODO
}

func (hm *HeartbeatMonitor) onHeartbeatTimeout(view uint64, leaderID uint64) {
	//TODO check for extension due to MsgFromLeader
	hm.handler.OnHeartbeatTimeout(view, leaderID)
}

// StartLeader will start sending heartbeats to all followers.
func (hm *HeartbeatMonitor) StartLeader(view uint64, leaderID uint64) {
	hm.logger.Debugf("Starting heartbeat transmission; leader: %d, view: %d", leaderID, view)

	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.timer != nil {
		hm.timer.Stop()
	}
	hm.view = view
	hm.leaderID = leaderID
	hm.follower = false
	hm.timer = time.AfterFunc(hm.hbInterval, hm.sendHeartbeat)
}

func (hm *HeartbeatMonitor) sendHeartbeat() {
	//TODO check for extension due to MsgFromLeader
	hm.mutex.Lock()
	heartbeat := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: hm.view,
				Seq:  0,
			},
		},
	}
	hm.mutex.Unlock()

	hm.comm.BroadcastConsensus(heartbeat)

	hm.mutex.Lock()
	hm.timer.Reset(hm.hbInterval)
	hm.mutex.Unlock()
}

// ProcessMsg handles an incoming heartbeat.
func (hm *HeartbeatMonitor) ProcessMsg(sender uint64, msg *smartbftprotos.HeartBeat) {
	// TODO
}

// Close stops following or sending heartbeats.
func (hm *HeartbeatMonitor) Close() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()
	if hm.timer != nil {
		hm.timer.Stop()
	}
}
