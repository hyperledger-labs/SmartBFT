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
	"github.com/golang/protobuf/proto"
)

//go:generate mockery -dir . -name ViewController -case underscore -output ./mocks/
type ViewController interface {
	ViewChanged(newViewNumber uint64, newProposalSequence uint64)
}

//go:generate mockery -dir . -name RequestsTimer -case underscore -output ./mocks/
type RequestsTimer interface {
	StopTimers()
	RestartTimers()
}

type ViewChanger struct {
	// Configuration
	SelfID uint64
	nodes  []uint64
	N      uint64
	f      int
	quorum int

	Logger   api.Logger
	Comm     Comm
	Signer   api.Signer
	Verifier api.Verifier

	Checkpoint *types.Checkpoint
	InFlight   *InFlightData

	Controller    ViewController
	RequestsTimer RequestsTimer

	ResendTicker <-chan time.Time

	// Runtime
	incMsgs        chan *incMsg
	viewChangeMsgs *voteSet
	viewDataMsgs   *voteSet
	currView       uint64
	nextView       uint64
	leader         uint64
	viewLock       sync.RWMutex // lock currView & nextView

	stopOnce sync.Once
	stopChan chan struct{}
	vcDone   sync.WaitGroup
}

// Start the view changer
func (v *ViewChanger) Start(startViewNumber uint64) {
	v.incMsgs = make(chan *incMsg, 10*v.N) // TODO channel size should be configured

	v.nodes = v.Comm.Nodes()

	v.quorum, v.f = computeQuorum(v.N)

	v.stopChan = make(chan struct{})
	v.stopOnce = sync.Once{}
	v.vcDone.Add(1)

	v.setupVotes()

	// set without locking
	v.currView = startViewNumber
	v.nextView = v.currView
	v.leader = getLeaderID(v.currView, v.N, v.nodes)

	go func() {
		defer v.vcDone.Done()
		v.run()
	}()

}

func (v *ViewChanger) setupVotes() {
	// view change
	acceptViewChange := func(_ uint64, message *protos.Message) bool {
		return message.GetViewChange() != nil
	}
	v.viewChangeMsgs = &voteSet{
		validVote: acceptViewChange,
	}
	v.viewChangeMsgs.clear(v.N)

	// view data
	acceptViewData := func(_ uint64, message *protos.Message) bool {
		return message.GetViewData() != nil
	}
	v.viewDataMsgs = &voteSet{
		validVote: acceptViewData,
	}
	v.viewDataMsgs.clear(v.N)
}

func (v *ViewChanger) close() {
	v.stopOnce.Do(
		func() {
			select {
			case <-v.stopChan:
				return
			default:
				close(v.stopChan)
			}
		},
	)
}

// Stop the view changer
func (v *ViewChanger) Stop() {
	v.close()
	v.vcDone.Wait()
}

// HandleMessage passes a message to the view changer
func (v *ViewChanger) HandleMessage(sender uint64, m *protos.Message) {
	msg := &incMsg{sender: sender, Message: m}
	select {
	case <-v.stopChan:
		return
	case v.incMsgs <- msg:
	}
}

func (v *ViewChanger) run() {
	for {
		select {
		case <-v.stopChan:
			return
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case <-v.ResendTicker:
			v.resend()
		}
	}
}

func (v *ViewChanger) resend() {
	v.viewLock.RLock()
	defer v.viewLock.RUnlock()
	if v.nextView == v.currView+1 { // started view change already but didn't get quorum yet
		msg := &protos.Message{
			Content: &protos.Message_ViewChange{
				ViewChange: &protos.ViewChange{
					NextView: v.nextView,
					Reason:   "", // TODO add reason
				},
			},
		}
		v.Comm.BroadcastConsensus(msg)
	}
}

func (v *ViewChanger) processMsg(sender uint64, m *protos.Message) {
	// viewChange message
	if vc := m.GetViewChange(); vc != nil {
		// check view number
		v.viewLock.RLock()
		if vc.NextView != v.currView+1 { // accept view change only to immediate next view number
			v.Logger.Warnf("%d got viewChange message %v from %d with view %d, expected view %d", v.SelfID, m, sender, vc.NextView, v.currView+1)
			v.viewLock.RUnlock()
			return
		}
		v.viewLock.RUnlock()
		v.viewChangeMsgs.registerVote(sender, m)
		v.processViewChangeMsg()
		return
	}

	v.viewLock.RLock()
	defer v.viewLock.RUnlock()

	//viewData message
	if vd := m.GetViewData(); vd != nil {
		if !v.validateViewDataMsg(vd, sender) {
			return
		}
		// TODO check data validity
		v.viewDataMsgs.registerVote(sender, m)
		v.processViewDataMsg()
		return
	}

	// newView message
	if nv := m.GetNewView(); nv != nil {
		if sender != v.leader {
			v.Logger.Warnf("%d got newView message %v from %d, expected sender to be %d the next leader", v.SelfID, m, sender, v.leader)
			return
		}
		// TODO check view number here?
		v.processNewViewMsg(nv)
	}
}

// StartViewChange stops current view and timeouts, and broadcasts a view change message to all
func (v *ViewChanger) StartViewChange() {
	v.viewLock.Lock()
	defer v.viewLock.Unlock()
	v.nextView = v.currView + 1
	v.RequestsTimer.StopTimers()
	msg := &protos.Message{
		Content: &protos.Message_ViewChange{
			ViewChange: &protos.ViewChange{
				NextView: v.nextView,
				Reason:   "", // TODO add reason
			},
		},
	}
	v.Comm.BroadcastConsensus(msg)
	v.Logger.Debugf("%d started view change, last view is %d", v.SelfID, v.currView)
}

func (v *ViewChanger) processViewChangeMsg() {
	if uint64(len(v.viewChangeMsgs.voted)) == uint64(v.f+1) { // join view change
		v.Logger.Debugf("%d is joining view change, last view is %d", v.SelfID, v.currView)
		v.StartViewChange()
	}
	v.viewLock.Lock()
	defer v.viewLock.Unlock()
	if len(v.viewChangeMsgs.voted) >= v.quorum-1 && v.nextView > v.currView { // send view data
		v.currView = v.nextView
		v.RequestsTimer.RestartTimers()
		v.leader = getLeaderID(v.currView, v.N, v.nodes)
		msg := v.prepareViewDataMsg()
		// TODO write to log
		if v.leader == v.SelfID {
			v.HandleMessage(v.SelfID, msg)
		} else {
			v.Comm.SendConsensus(v.leader, msg)
		}
		v.viewChangeMsgs.clear(v.N) // TODO make sure clear is in the right place
		v.viewDataMsgs.clear(v.N)   // clear because currView changed
		v.Logger.Debugf("%d sent view data msg, with next view %d, to the new leader %d", v.SelfID, v.currView, v.leader)
	}
}

func (v *ViewChanger) prepareViewDataMsg() *protos.Message {
	lastDecision, lastDecisionSignatures := v.Checkpoint.Get()
	inFlight := v.InFlight.InFlightProposal()
	var inFlightProposal *protos.Proposal
	if inFlight != nil {
		inFlightProposal = &protos.Proposal{
			Header:               inFlight.Header,
			Metadata:             inFlight.Metadata,
			Payload:              inFlight.Payload,
			VerificationSequence: uint64(inFlight.VerificationSequence),
		}
	}
	prepared := false
	if v.InFlight.InFlightPrepares() != nil {
		prepared = true
	}
	vd := &protos.ViewData{
		NextView:               v.currView,
		LastDecision:           &lastDecision,
		LastDecisionSignatures: lastDecisionSignatures,
		InFlightProposal:       inFlightProposal,
		InFlightPrepared:       prepared,
	}
	vdBytes := MarshalOrPanic(vd)
	sig := v.Signer.Sign(vdBytes)
	msg := &protos.Message{
		Content: &protos.Message_ViewData{
			ViewData: &protos.SignedViewData{
				RawViewData: vdBytes,
				Signer:      v.SelfID,
				Signature:   sig,
			},
		},
	}
	return msg
}

func (v *ViewChanger) validateViewDataMsg(vd *protos.SignedViewData, sender uint64) bool {
	if vd.Signer != sender {
		v.Logger.Warnf("%d got viewData message %v from %d, but signer %d is not the sender %d", v.SelfID, vd, sender, vd.Signer, sender)
		return false
	}
	if err := v.Verifier.VerifySignature(types.Signature{Id: vd.Signer, Value: vd.Signature, Msg: vd.RawViewData}); err != nil {
		v.Logger.Warnf("%d got viewData message %v from %d, but signature is invalid, error: %v", v.SelfID, vd, sender, err)
		return false
	}
	rvd := &protos.ViewData{}
	if err := proto.Unmarshal(vd.RawViewData, rvd); err != nil {
		v.Logger.Errorf("%d was unable to unmarshal viewData message from %d, error: %v", v.SelfID, sender, err)
		return false
	}
	if rvd.NextView != v.currView {
		v.Logger.Warnf("%d got viewData message %v from %d, but %d is in view %d", v.SelfID, rvd, sender, v.SelfID, v.currView)
		return false
	}
	if getLeaderID(rvd.NextView, v.N, v.nodes) != v.SelfID { // check if I am the next leader
		v.Logger.Warnf("%d got viewData message %v from %d, but %d is not the next leader", v.SelfID, rvd, sender, v.SelfID)
		return false
	}
	return v.validateLastDecision(rvd, sender)
}

func (v *ViewChanger) validateLastDecision(vd *protos.ViewData, sender uint64) bool {
	if vd.LastDecision == nil {
		v.Logger.Warnf("%d got viewData message %v from %d, but the last decision is not set", v.SelfID, vd, sender)
		return false
	}
	if vd.LastDecision.Metadata == nil { // this is a genesis block, no signatures
		v.Logger.Debugf("%d got viewData message %v from %d, last decision metadata is nil", v.SelfID, vd, sender)
		// TODO handle genesis block
		return true
	}
	if vd.LastDecisionSignatures == nil {
		v.Logger.Warnf("%d got viewData message %v from %d, but the last decision signatures is not set", v.SelfID, vd, sender)
		return false
	}
	numSigs := len(vd.LastDecisionSignatures)
	if numSigs < v.quorum {
		v.Logger.Warnf("%d got viewData message %v from %d, but there are only %d last decision signatures", v.SelfID, vd, sender, numSigs)
		return false
	}
	for _, sig := range vd.LastDecisionSignatures {
		signature := types.Signature{
			Id:    sig.Signer,
			Value: sig.Value,
			Msg:   sig.Msg,
		}
		proposal := types.Proposal{
			Header:               vd.LastDecision.Header,
			Payload:              vd.LastDecision.Payload,
			Metadata:             vd.LastDecision.Metadata,
			VerificationSequence: int64(vd.LastDecision.VerificationSequence),
		}
		if err := v.Verifier.VerifyConsenterSig(signature, proposal); err != nil {
			v.Logger.Warnf("%d got viewData message %v from %d, but last decision signature is invalid, error: %v", v.SelfID, vd, sender, err)
			return false
		}
	}
	return true
}

func (v *ViewChanger) processViewDataMsg() {
	if len(v.viewDataMsgs.voted) >= v.quorum { // need enough (quorum) data to continue
		signedMsgs := make([]*protos.SignedViewData, 0)
		close(v.viewDataMsgs.votes)
		for vote := range v.viewDataMsgs.votes {
			signedMsgs = append(signedMsgs, vote.GetViewData())
		}
		msg := &protos.Message{
			Content: &protos.Message_NewView{
				NewView: &protos.NewView{
					SignedViewData: signedMsgs,
				},
			},
		}
		v.Comm.BroadcastConsensus(msg)
		v.HandleMessage(v.SelfID, msg) // also send to myself
		v.viewDataMsgs.clear(v.N)
	}
}

func (v *ViewChanger) processNewViewMsg(msg *protos.NewView) {
	signed := msg.GetSignedViewData()
	nodesMap := make(map[uint64]struct{}, v.N)
	valid := 0
	for _, svd := range signed {
		if _, exist := nodesMap[svd.Signer]; exist {
			continue // seen data from this node already
		}
		nodesMap[svd.Signer] = struct{}{}

		if err := v.Verifier.VerifySignature(types.Signature{Id: svd.Signer, Value: svd.Signature, Msg: svd.RawViewData}); err != nil {
			v.Logger.Warnf("%d is processing newView message %v, but signature of viewData %v is invalid, error: %v", v.SelfID, msg, svd, err)
			continue
		}

		vd := &protos.ViewData{}
		if err := proto.Unmarshal(svd.RawViewData, vd); err != nil {
			v.Logger.Errorf("%d was unable to unmarshal a viewData from the newView message, error: %v", v.SelfID, err)
			continue
		}

		if vd.NextView != v.currView {
			v.Logger.Warnf("%d is processing newView message %v, but nextView of viewData %v is %d, while the currView is %d", v.SelfID, msg, svd, vd.NextView, v.currView)
			continue
		}

		if !v.validateLastDecision(vd, svd.Signer) {
			v.Logger.Warnf("%d is processing newView message %v, but the last decision in viewData %v is invalid", v.SelfID, msg, vd)
			continue
		}

		// TODO validate data

		valid++
	}
	if valid >= v.quorum {
		// TODO handle data
		v.Controller.ViewChanged(v.currView, 0) // TODO change seq 0
	}
}
