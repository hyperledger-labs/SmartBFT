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
	logger          api.Logger
	hbTimeout       time.Duration
	hbInterval      time.Duration
	comm            Comm
	handler         HeartbeatTimeoutHandler
	mutex           sync.Mutex
	timer           *time.Timer
	view            uint64
	leaderID        uint64
	follower        bool
	lastInHeartbeat time.Time
}

func NewHeartbeatMonitor(
	logger api.Logger,
	heartbeatTimeout time.Duration,
	comm Comm,
	handler HeartbeatTimeoutHandler,
) *HeartbeatMonitor {
	if heartbeatTimeout/10 < time.Nanosecond {
		return nil
	}

	hm := &HeartbeatMonitor{
		logger:     logger,
		hbTimeout:  heartbeatTimeout,
		hbInterval: heartbeatTimeout / 10,
		comm:       comm,
		handler:    handler,
	}
	return hm
}

// StartFollower will start following the heartbeats of the leader of the view.
func (hm *HeartbeatMonitor) StartFollower(view uint64, leaderID uint64) {
	hm.logger.Debugf("Starting heartbeat timeout on follower, duration: %s; leader: %d, view: %d",
		hm.hbTimeout.String(), leaderID, view)

	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.timer != nil {
		hm.timer.Stop()
	}
	hm.view = view
	hm.leaderID = leaderID
	hm.follower = true
	hm.lastInHeartbeat = time.Now()

	hm.timer = time.AfterFunc(hm.hbTimeout, func() { hm.onHeartbeatTimeout(view, leaderID) })
}

func (hm *HeartbeatMonitor) onHeartbeatTimeout(view uint64, leaderID uint64) {
	// TODO Optimisation: check for extension due to data messages from leader (backlog)
	hm.logger.Debugf("Heartbeat timer event, checking expiration")
	expired := hm.checkExpiration()
	if expired {
		hm.handler.OnHeartbeatTimeout(view, leaderID)
	}
}

func (hm *HeartbeatMonitor) checkExpiration() bool {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	delta := time.Since(hm.lastInHeartbeat)
	if delta >= hm.hbTimeout {
		hm.logger.Debugf("Heartbeat timeout expired; last HB: %s, delta: %s", hm.lastInHeartbeat, delta)
		return true
	}

	extension := hm.hbTimeout - delta
	hm.timer.Reset(extension)
	hm.logger.Debugf("Heartbeat timeout extended; duration: %s", extension)
	return false
}

// ProcessMsg handles an incoming heartbeat.
// If the sender and msg.View equal what we expect, and the timeout had not expired yet, the timeout is extended.
func (hm *HeartbeatMonitor) ProcessMsg(sender uint64, msg *smartbftprotos.HeartBeat) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if !hm.follower {
		hm.logger.Debugf("Heartbeat monitor is not a follower, ignoring; sender: %d, msg: %v", sender, msg)
		return
	}

	if sender != hm.leaderID {
		hm.logger.Debugf("Heartbeat sender is not the leader, ignoring; leader: %d, sender: %d, msg: %v", hm.leaderID, sender, msg)
		return
	}

	if msg.View != hm.view {
		hm.logger.Debugf("Heartbeat view is different than monitor view, ignoring; view: %d, sender: %d, msg: %v", hm.leaderID, sender, msg)
		return
	}

	hm.lastInHeartbeat = time.Now()
	hm.logger.Debugf("Heartbeat arrival time recorded, timeout will be extended; msg: %v", msg)
}

// StartLeader will start sending heartbeats to all followers.
func (hm *HeartbeatMonitor) StartLeader(view uint64, leaderID uint64) {
	hm.logger.Debugf("Starting heartbeat transmission, interval: %s; leader: %d, view: %d",
		hm.hbInterval, leaderID, view)

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
	// TODO Optimisation: check for extension due to data messages sent by leader (backlog)

	view, follower, heartbeat, doSend := hm.prepareSend()
	if !doSend {
		return
	}

	hm.comm.BroadcastConsensus(heartbeat)

	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if view == hm.view && follower == hm.follower {
		hm.timer.Reset(hm.hbInterval)
	}
}

func (hm *HeartbeatMonitor) prepareSend() (view uint64, follower bool, heartbeat *smartbftprotos.Message, doSend bool) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	view = hm.view
	follower = hm.follower
	if follower {
		return view, follower, nil, false
	}

	heartbeat = &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: view,
			},
		},
	}
	doSend = true
	return
}

// Close stops following or sending heartbeats.
func (hm *HeartbeatMonitor) Close() {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	if hm.timer != nil {
		hm.timer.Stop()
	}
	hm.view = 0
}
