// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
)

const (
	Leader   Role = false
	Follower Role = true
)

//go:generate mockery -dir . -name HeartbeatTimeoutHandler -case underscore -output ./mocks/

// HeartbeatTimeoutHandler defines who to call when a heartbeat timeout expires.
type HeartbeatTimeoutHandler interface {
	OnHeartbeatTimeout(view uint64, leaderID uint64)
}

type Role bool

type roleChange struct {
	view     uint64
	leaderID uint64
	follower Role
}

// senderSet is a set of node IDs
type senderSet map[uint64]bool

// heartbeatResponseCollector is a map from view ID to a set of senders.
type heartbeatResponseCollector map[uint64]senderSet

type HeartbeatMonitor struct {
	scheduler       <-chan time.Time
	inc             chan incMsg
	stopChan        chan struct{}
	commandChan     chan roleChange
	logger          api.Logger
	hbTimeout       time.Duration
	hbCount         int
	comm            Comm
	handler         HeartbeatTimeoutHandler
	view            uint64
	leaderID        uint64
	follower        Role
	lastHeartbeat   time.Time
	lastTick        time.Time
	hbRespCollector heartbeatResponseCollector
	running         sync.WaitGroup
	runOnce         sync.Once
	timedOut        bool
	syncReq         bool
	viewSequences   *atomic.Value
}

func NewHeartbeatMonitor(
	scheduler <-chan time.Time,
	logger api.Logger,
	heartbeatTimeout time.Duration,
	heartbeatCount int,
	comm Comm,
	handler HeartbeatTimeoutHandler,
	viewSequences *atomic.Value,
) *HeartbeatMonitor {
	hm := &HeartbeatMonitor{
		stopChan:        make(chan struct{}),
		inc:             make(chan incMsg),
		commandChan:     make(chan roleChange),
		scheduler:       scheduler,
		logger:          logger,
		hbTimeout:       heartbeatTimeout,
		hbCount:         heartbeatCount,
		comm:            comm,
		handler:         handler,
		hbRespCollector: make(heartbeatResponseCollector),
		viewSequences:   viewSequences,
	}
	return hm
}

func (hm *HeartbeatMonitor) start() {
	hm.running.Add(1)
	go hm.run()
}

// Close stops following or sending heartbeats.
func (hm *HeartbeatMonitor) Close() {
	if hm.closed() {
		return
	}

	defer hm.running.Wait()
	close(hm.stopChan)
}

func (hm *HeartbeatMonitor) run() {
	defer hm.running.Done()
	for {
		select {
		case <-hm.stopChan:
			return
		case now := <-hm.scheduler:
			hm.tick(now)
		case msg := <-hm.inc:
			hm.handleMsg(msg.sender, msg.Message)
		case cmd := <-hm.commandChan:
			hm.handleCommand(cmd)
		}
	}
}

// ProcessMsg handles an incoming heartbeat or heartbeat-response.
// If the sender and msg.View equal what we expect, and the timeout had not expired yet, the timeout is extended.
func (hm *HeartbeatMonitor) ProcessMsg(sender uint64, msg *smartbftprotos.Message) {
	hm.inc <- incMsg{
		sender:  sender,
		Message: msg,
	}
}

// ChangeRole will change the role of this HeartbeatMonitor
func (hm *HeartbeatMonitor) ChangeRole(follower Role, view uint64, leaderID uint64) {
	hm.runOnce.Do(func() {
		hm.follower = follower
		hm.start()
	})

	role := "leader"
	if follower {
		role = "follower"
	}

	hm.logger.Infof("Changing to %s role, current view: %d, current leader: %d", role, view, leaderID)
	select {
	case hm.commandChan <- roleChange{
		leaderID: leaderID,
		view:     view,
		follower: follower,
	}:
	case <-hm.stopChan:
		return
	}

}

func (hm *HeartbeatMonitor) handleMsg(sender uint64, msg *smartbftprotos.Message) {
	switch msg.GetContent().(type) {
	case *smartbftprotos.Message_HeartBeat:
		hm.handleHeartBeat(sender, msg.GetHeartBeat())
	case *smartbftprotos.Message_HeartBeatResponse:
		hm.handleHeartBeatResponse(sender, msg.GetHeartBeatResponse())
	default:
		hm.logger.Warnf("Unexpected message type, ignoring")
	}
}

func (hm *HeartbeatMonitor) handleHeartBeat(sender uint64, hb *smartbftprotos.HeartBeat) {
	if sender != hm.leaderID || hb.View != hm.view {
		hm.logger.Infof("Heartbeat is unexpected, ignoring; Monitor: leader=%d, view=%d; HB: sender: %d, view: %d, seq: %d",
			hm.leaderID, hm.view, sender, hb.View, hb.Seq)
		if hb.View < hm.view {
			hm.sendHeartBeatResponse(sender)
		}
		return
	}

	if !hm.follower {
		hm.logger.Infof("Heartbeat monitor is not a follower, ignoring; sender: %d, msg: %v", sender, hb)
		return
	}

	if hm.viewActiveButBehindLeader(hb) {
		return
	}

	hm.logger.Debugf("Received heartbeat from %d, last heartbeat was %v ago", sender, hm.lastTick.Sub(hm.lastHeartbeat))
	hm.lastHeartbeat = hm.lastTick
}

func (hm *HeartbeatMonitor) handleHeartBeatResponse(sender uint64, hbr *smartbftprotos.HeartBeatResponse) {
	if hm.follower {
		hm.logger.Infof("Monitor is not a leader, ignoring HeartBeatResponse; sender: %d, msg: %v", sender, hbr)
		return
	}

	if hm.view >= hbr.View {
		hm.logger.Debugf("Monitor view: %d >= HeartBeatResponse, ignoring; sender: %d, msg: %v", sender, hbr)
		return
	}

	hm.logger.Debugf("Received HeartBeatResponse, msg: %v; from %d", hbr, sender)
	senders, exists := hm.hbRespCollector[hbr.View]
	if !exists {
		senders = make(senderSet)
		hm.hbRespCollector[hbr.View] = senders
	}
	senders[sender] = true

	hm.logger.Debugf("HeartBeatResponse Collector size: %d, senderSet(%d) size: %d", len(hm.hbRespCollector), hbr.View, len(senders))

	//TODO keep track of these, and if we get f+1 identical, force a sync
}

func (hm *HeartbeatMonitor) sendHeartBeatResponse(target uint64) {
	heartbeatResponse := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeatResponse{
			HeartBeatResponse: &smartbftprotos.HeartBeatResponse{
				View: hm.view,
			},
		},
	}
	hm.comm.SendConsensus(target, heartbeatResponse)
	hm.logger.Debugf("Sent HeartBeatResponse view: %d; to %d", hm.view, target)
}

func (hm *HeartbeatMonitor) viewActiveButBehindLeader(hbMsg *smartbftprotos.HeartBeat) bool {
	vs := hm.viewSequences.Load()
	// View isn't initialized
	if vs == nil {
		return false
	}

	viewSeq := vs.(ViewSequence)
	if !viewSeq.ViewActive {
		return false
	}

	ourSeq := viewSeq.ProposalSeq

	areWeBehind := ourSeq+1 < hbMsg.Seq

	if areWeBehind {
		hm.logger.Infof("Leader's sequence is %d and ours is %d", hbMsg.Seq, ourSeq)
	}

	return areWeBehind
}

func (hm *HeartbeatMonitor) tick(now time.Time) {
	hm.lastTick = now
	if hm.lastHeartbeat.IsZero() {
		hm.lastHeartbeat = now
	}
	if hm.follower {
		hm.followerTick(now)
	} else {
		hm.leaderTick(now)
	}
}

func (hm *HeartbeatMonitor) closed() bool {
	select {
	case <-hm.stopChan:
		return true
	default:
		return false
	}
}

func (hm *HeartbeatMonitor) handleCommand(cmd roleChange) {
	hm.timedOut = false
	hm.view = cmd.view
	hm.leaderID = cmd.leaderID
	hm.follower = cmd.follower
	hm.lastHeartbeat = hm.lastTick
	hm.hbRespCollector = make(heartbeatResponseCollector)
	hm.syncReq = false
}

func (hm *HeartbeatMonitor) leaderTick(now time.Time) {
	if now.Sub(hm.lastHeartbeat)*time.Duration(hm.hbCount) < hm.hbTimeout {
		return
	}

	var sequence uint64
	vs := hm.viewSequences.Load()
	if vs != nil && vs.(ViewSequence).ViewActive {
		sequence = vs.(ViewSequence).ProposalSeq
	} else {
		hm.logger.Infof("ViewSequence uninitialized or view inactive")
		return
	}
	hm.logger.Debugf("Sending heartbeat with view %d, sequence %d", hm.view, sequence)
	heartbeat := &smartbftprotos.Message{
		Content: &smartbftprotos.Message_HeartBeat{
			HeartBeat: &smartbftprotos.HeartBeat{
				View: hm.view,
				Seq:  sequence,
			},
		},
	}
	hm.comm.BroadcastConsensus(heartbeat)
	hm.lastHeartbeat = now
}

func (hm *HeartbeatMonitor) followerTick(now time.Time) {
	if hm.timedOut || hm.lastHeartbeat.IsZero() {
		hm.lastHeartbeat = now
		return
	}

	delta := now.Sub(hm.lastHeartbeat)
	if delta >= hm.hbTimeout {
		hm.logger.Warnf("Heartbeat timeout (%v) from %d expired; last heartbeat was observed %s ago",
			hm.hbTimeout, hm.leaderID, delta)
		hm.handler.OnHeartbeatTimeout(hm.view, hm.leaderID)
		hm.timedOut = true
		return
	}

	hm.logger.Debugf("Last heartbeat from %d was %v ago", hm.leaderID, delta)
}
