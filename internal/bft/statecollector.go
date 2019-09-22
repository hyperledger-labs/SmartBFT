// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type StateCollector struct {
	SelfID uint64
	N      uint64
	f      int
	quorum int

	Logger api.Logger

	incMsgs chan *incMsg

	CollectTimeout time.Duration

	responses *voteSet

	stopOnce sync.Once
	stopChan chan struct{}
}

// Start starts the state collector
func (s *StateCollector) Start() {
	s.incMsgs = make(chan *incMsg, s.N)
	s.quorum, s.f = computeQuorum(s.N)
	s.stopChan = make(chan struct{})
	s.stopOnce = sync.Once{}

	acceptResponse := func(_ uint64, message *protos.Message) bool {
		return message.GetStateTransferResponse() != nil
	}
	s.responses = &voteSet{
		validVote: acceptResponse,
	}
	s.responses.clear(s.N)
}

// HandleMessage handle messages addressed to the state collector
func (s *StateCollector) HandleMessage(sender uint64, m *protos.Message) {
	msg := &incMsg{sender: sender, Message: m}
	s.Logger.Debugf("Node %d handling state response: %v", s.SelfID, msg)
	select {
	case <-s.stopChan:
		return
	case s.incMsgs <- msg:
	default: // if incMsgs is full do nothing
		s.Logger.Debugf("Node %d reached default in handling state response: %v", s.SelfID, msg)
	}
}

// CollectStateResponses return a valid response or nil if reached timeout
func (s *StateCollector) CollectStateResponses() *types.ViewAndSeq {
	// drain message channel
	for len(s.incMsgs) > 0 {
		<-s.incMsgs
	}

	s.responses.clear(s.N)

	timer := time.NewTimer(s.CollectTimeout)

	s.Logger.Debugf("Node %d started collecting state responses", s.SelfID)

	for {
		select {
		case <-s.stopChan:
			timer.Stop()
			return nil
		case <-timer.C:
			s.Logger.Infof("Node %d reached the state collector timeout", s.SelfID)
			return nil
		case msg := <-s.incMsgs:
			s.Logger.Debugf("Node %d collected a response: %v", s.SelfID, msg)
			s.responses.registerVote(msg.sender, msg.Message)
			if viewAndSeq := s.collectLogic(); viewAndSeq != nil {
				s.Logger.Infof("Node %d collected a valid state: view - %d and seq - %d", s.SelfID, viewAndSeq.View, viewAndSeq.Seq)
				timer.Stop()
				return viewAndSeq
			}
		}
	}

}

func (s *StateCollector) collectLogic() *types.ViewAndSeq {
	if len(s.responses.voted) > s.f {
		votesMap := make(map[types.ViewAndSeq]uint64)
		num := len(s.responses.votes)
		for i := 0; i < num; i++ {
			vote := <-s.responses.votes
			viewAndSeq := types.ViewAndSeq{
				View: vote.GetStateTransferResponse().ViewNum,
				Seq:  vote.GetStateTransferResponse().Sequence,
			}
			s.Logger.Debugf("Node %d collected a responses with view - %d and seq - %d", s.SelfID, viewAndSeq.View, viewAndSeq.Seq)
			s.responses.votes <- vote
			if count, exist := votesMap[viewAndSeq]; exist {
				count++
				votesMap[viewAndSeq] = count
			} else {
				votesMap[viewAndSeq] = 1
			}
		}
		for viewAndSeq, count := range votesMap {
			if count > uint64(s.f) {
				return &viewAndSeq
			}
		}
	}
	return nil
}

func (s *StateCollector) close() {
	s.stopOnce.Do(
		func() {
			select {
			case <-s.stopChan:
				return
			default:
				close(s.stopChan)
			}
		},
	)
}

// Stop the state collector
func (s *StateCollector) Stop() {
	s.close()
}
