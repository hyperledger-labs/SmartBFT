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
	AbortView()
}

//go:generate mockery -dir . -name RequestsTimer -case underscore -output ./mocks/
type RequestsTimer interface {
	StopTimers()
	RestartTimers()
	RemoveRequest(request types.RequestInfo) error
}

type ViewChanger struct {
	// Configuration
	SelfID uint64
	nodes  []uint64
	N      uint64
	f      int
	quorum int

	Logger       api.Logger
	Comm         Comm
	Signer       api.Signer
	Verifier     api.Verifier
	Application  api.Application
	Synchronizer api.Synchronizer

	Checkpoint *types.Checkpoint
	InFlight   *InFlightData

	Controller    ViewController
	RequestsTimer RequestsTimer

	ResendTicker <-chan time.Time

	// Runtime
	incMsgs         chan *incMsg
	viewChangeMsgs  *voteSet
	viewDataMsgs    *voteSet
	currView        uint64
	nextView        uint64
	leader          uint64
	startChangeChan chan struct{}

	stopOnce sync.Once
	stopChan chan struct{}
	vcDone   sync.WaitGroup
}

// Start the view changer
func (v *ViewChanger) Start(startViewNumber uint64) {
	v.incMsgs = make(chan *incMsg, 10*v.N) // TODO channel size should be configured
	v.startChangeChan = make(chan struct{}, 1)

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
		case <-v.startChangeChan:
			v.startViewChange()
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case <-v.ResendTicker:
			v.resend()
		}
	}
}

func (v *ViewChanger) resend() {
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
		v.Logger.Debugf("Node %d is processing a view change message from %d", v.SelfID, sender)
		// check view number
		if vc.NextView != v.currView+1 { // accept view change only to immediate next view number
			v.Logger.Warnf("Node %d got viewChange message %v from %d with view %d, expected view %d", v.SelfID, m, sender, vc.NextView, v.currView+1)
			return
		}
		v.viewChangeMsgs.registerVote(sender, m)
		v.processViewChangeMsg()
		return
	}

	//viewData message
	if vd := m.GetViewData(); vd != nil {
		v.Logger.Debugf("Node %d is processing a view data message from %d", v.SelfID, sender)
		if !v.validateViewDataMsg(vd, sender) {
			return
		}
		v.viewDataMsgs.registerVote(sender, m)
		v.processViewDataMsg()
		return
	}

	// newView message
	if nv := m.GetNewView(); nv != nil {
		v.Logger.Debugf("Node %d is processing a new view message from %d", v.SelfID, sender)
		if sender != v.leader {
			v.Logger.Warnf("Node %d got newView message %v from %d, expected sender to be %d the next leader", v.SelfID, m, sender, v.leader)
			return
		}
		v.processNewViewMsg(nv)
	}
}

// StartViewChange initiates a view change
func (v *ViewChanger) StartViewChange() {
	select {
	case v.startChangeChan <- struct{}{}:
	default:
	}
}

// StartViewChange stops current view and timeouts, and broadcasts a view change message to all
func (v *ViewChanger) startViewChange() {
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
	v.Logger.Debugf("Node %d started view change, last view is %d", v.SelfID, v.currView)
	v.Controller.AbortView() // abort the current view when joining view change
}

func (v *ViewChanger) processViewChangeMsg() {
	if uint64(len(v.viewChangeMsgs.voted)) == uint64(v.f+1) { // join view change
		v.Logger.Debugf("Node %d is joining view change, last view is %d", v.SelfID, v.currView)
		v.startViewChange()
	}
	// TODO add view change try timeout
	if len(v.viewChangeMsgs.voted) >= v.quorum-1 && v.nextView > v.currView { // send view data
		v.currView = v.nextView
		v.leader = getLeaderID(v.currView, v.N, v.nodes)
		v.RequestsTimer.RestartTimers()
		v.viewChangeMsgs.clear(v.N)
		v.viewDataMsgs.clear(v.N) // clear because currView changed

		msg := v.prepareViewDataMsg()
		// TODO write to log
		if v.leader == v.SelfID {
			v.processMsg(v.SelfID, msg)
		} else {
			v.Comm.SendConsensus(v.leader, msg)
		}
		v.Logger.Debugf("Node %d sent view data msg, with next view %d, to the new leader %d", v.SelfID, v.currView, v.leader)
	}
}

func (v *ViewChanger) prepareViewDataMsg() *protos.Message {
	lastDecision, lastDecisionSignatures := v.Checkpoint.Get()
	inFlight := v.getInFlight(&lastDecision)
	prepared := false
	if v.InFlight.InFlightPrepares() != nil {
		prepared = true
	}
	vd := &protos.ViewData{
		NextView:               v.currView,
		LastDecision:           &lastDecision,
		LastDecisionSignatures: lastDecisionSignatures,
		InFlightProposal:       inFlight,
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

func (v *ViewChanger) getInFlight(lastDecision *protos.Proposal) *protos.Proposal {
	inFlight := v.InFlight.InFlightProposal()
	if inFlight == nil {
		v.Logger.Debugf("Node %d's in flight proposal is not set", v.SelfID)
		return nil
	}
	if inFlight.Metadata == nil {
		v.Logger.Panicf("Node %d's in flight proposal metadata is not set", v.SelfID)
	}
	inFlightMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(inFlight.Metadata, inFlightMetadata); err != nil {
		v.Logger.Panicf("Node %d is unable to unmarshal its own in flight metadata, err: %v", v.SelfID, err)
	}
	proposal := &protos.Proposal{
		Header:               inFlight.Header,
		Metadata:             inFlight.Metadata,
		Payload:              inFlight.Payload,
		VerificationSequence: uint64(inFlight.VerificationSequence),
	}
	if lastDecision == nil {
		v.Logger.Panicf("Node %d's checkpoint is not set with the last decision", v.SelfID)
	}
	if lastDecision.Metadata == nil { // this is the genesis proposal
		return proposal
	}
	lastDecisionMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(lastDecision.Metadata, lastDecisionMetadata); err != nil {
		v.Logger.Panicf("Node %d is unable to unmarshal its own last decision metadata from checkpoint, err: %v", v.SelfID, err)
	}
	if inFlightMetadata.LatestSequence == lastDecisionMetadata.LatestSequence {
		v.Logger.Debugf("Node %d's in flight proposal and the last decision has the same sequence: %d", v.SelfID, inFlightMetadata.LatestSequence)
		return nil // this is not an actual in flight proposal
	}
	if inFlightMetadata.LatestSequence != lastDecisionMetadata.LatestSequence+1 {
		v.Logger.Panicf("Node %d's in flight proposal sequence is %d while its last decision sequence is %d", v.SelfID, inFlightMetadata.LatestSequence, lastDecisionMetadata.LatestSequence)
	}
	return proposal
}

func (v *ViewChanger) validateViewDataMsg(vd *protos.SignedViewData, sender uint64) bool {
	if vd.Signer != sender {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but signer %d is not the sender %d", v.SelfID, vd, sender, vd.Signer, sender)
		return false
	}
	if err := v.Verifier.VerifySignature(types.Signature{Id: vd.Signer, Value: vd.Signature, Msg: vd.RawViewData}); err != nil {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but signature is invalid, error: %v", v.SelfID, vd, sender, err)
		return false
	}
	rvd := &protos.ViewData{}
	if err := proto.Unmarshal(vd.RawViewData, rvd); err != nil {
		v.Logger.Errorf("Node %d was unable to unmarshal viewData message from %d, error: %v", v.SelfID, sender, err)
		return false
	}
	if rvd.NextView != v.currView {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but %d is in view %d", v.SelfID, rvd, sender, v.SelfID, v.currView)
		return false
	}
	if getLeaderID(rvd.NextView, v.N, v.nodes) != v.SelfID { // check if I am the next leader
		v.Logger.Warnf("Node %d got viewData message %v from %d, but %d is not the next leader", v.SelfID, rvd, sender, v.SelfID)
		return false
	}
	validLastDecision, lastView, lastSequence := v.validateLastDecision(rvd, sender)
	if !validLastDecision {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but the last decision is invalid", v.SelfID, rvd, sender)
		return false
	}
	return v.validateInFlight(rvd, sender, lastView, lastSequence)
}

func (v *ViewChanger) validateLastDecision(vd *protos.ViewData, sender uint64) (valid bool, lastView uint64, lastSequence uint64) {
	if vd.LastDecision == nil {
		v.Logger.Warnf("Node %d is processing viewData message %v from %d, but the last decision is not set", v.SelfID, vd, sender)
		return false, 0, 0
	}
	if vd.LastDecision.Metadata == nil { // this is a genesis proposal, no signatures
		v.Logger.Debugf("Node %d is processing viewData message %v from %d, last decision metadata is nil", v.SelfID, vd, sender)
		// TODO handle genesis proposal
		return true, 0, 0
	}
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(vd.LastDecision.Metadata, md); err != nil {
		v.Logger.Warnf("Node %d is processing viewData message %v from %d, but was unable to unmarshal last decision metadata, err: %v", v.SelfID, vd, sender, err)
		return false, 0, 0
	}
	if md.ViewId >= vd.NextView {
		v.Logger.Warnf("Node %d is processing viewData message %v from %d, but last decision view %d is greater or equal to requested next view %d", v.SelfID, vd, sender, md.ViewId, vd.NextView)
		return false, 0, 0
	}
	numSigs := len(vd.LastDecisionSignatures)
	if numSigs < v.quorum {
		v.Logger.Warnf("Node %d is processing viewData message %v from %d, but there are only %d last decision signatures", v.SelfID, vd, sender, numSigs)
		return false, 0, 0
	}
	nodesMap := make(map[uint64]struct{}, v.N)
	validSig := 0
	for _, sig := range vd.LastDecisionSignatures {
		if _, exist := nodesMap[sig.Signer]; exist {
			continue // seen signature from this node already
		}
		nodesMap[sig.Signer] = struct{}{}
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
			v.Logger.Warnf("Node %d is processing viewData message %v from %d, but last decision signature is invalid, error: %v", v.SelfID, vd, sender, err)
			return false, 0, 0
		}
		validSig++
	}
	if validSig < v.quorum {
		v.Logger.Warnf("Node %d is processing viewData message %v from %d, but there are only %d valid last decision signatures", v.SelfID, vd, sender, validSig)
		return false, 0, 0
	}
	return true, md.ViewId, md.LatestSequence
}

func (v *ViewChanger) validateInFlight(vd *protos.ViewData, sender uint64, lastView uint64, lastSequence uint64) bool {
	// TODO validate in flight
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
		v.processMsg(v.SelfID, msg) // also send to myself
		v.viewDataMsgs.clear(v.N)
		v.Logger.Debugf("Node %d sent a new view msg", v.SelfID)
	}
}

func (v *ViewChanger) processNewViewMsg(msg *protos.NewView) {
	signed := msg.GetSignedViewData()
	nodesMap := make(map[uint64]struct{}, v.N)
	valid := 0
	var maxLastDecisionSequence uint64
	var maxLastDecision *protos.Proposal
	var maxLastDecisionSigs []*protos.Signature
	for _, svd := range signed {
		if _, exist := nodesMap[svd.Signer]; exist {
			continue // seen data from this node already
		}
		nodesMap[svd.Signer] = struct{}{}

		if err := v.Verifier.VerifySignature(types.Signature{Id: svd.Signer, Value: svd.Signature, Msg: svd.RawViewData}); err != nil {
			v.Logger.Warnf("Node %d is processing newView message, but signature of viewData %v is invalid, error: %v", v.SelfID, svd, err)
			continue
		}

		vd := &protos.ViewData{}
		if err := proto.Unmarshal(svd.RawViewData, vd); err != nil {
			v.Logger.Errorf("Node %d was unable to unmarshal viewData from the newView message, error: %v", v.SelfID, err)
			continue
		}

		if vd.NextView != v.currView {
			v.Logger.Warnf("Node %d is processing newView message, but nextView of viewData %v is %d, while the currView is %d", v.SelfID, vd, vd.NextView, v.currView)
			continue
		}

		validLastDecision, lastView, lastSequence := v.validateLastDecision(vd, svd.Signer)
		if !validLastDecision {
			v.Logger.Warnf("Node %d is processing newView message, but the last decision in viewData %v is invalid", v.SelfID, vd)
			continue
		}

		if !v.validateInFlight(vd, svd.Signer, lastView, lastSequence) {
			v.Logger.Warnf("Node %d is processing newView message, but the in flight in viewData %v is invalid", v.SelfID, vd)
			continue
		}

		if vd.LastDecision.Metadata == nil {
			// TODO handle genesis proposal
			v.Logger.Debugf("Node %d is processing newView message, but the last decision metadata in viewData %v is nil", v.SelfID, vd)
		} else {
			md := &protos.ViewMetadata{}
			if err := proto.Unmarshal(vd.LastDecision.Metadata, md); err != nil {
				v.Logger.Warnf("Node %d is processing newView message, but was unable to unmarshal last decision metadata in viewData %v, err: %v", v.SelfID, vd, err)
				continue
			}
			v.Logger.Debugf("Current max sequence is %d and this viewData %v last decision sequence is %d", maxLastDecisionSequence, vd, md.LatestSequence)
			if md.LatestSequence > maxLastDecisionSequence {
				maxLastDecisionSequence = md.LatestSequence
				maxLastDecision = vd.LastDecision
				maxLastDecisionSigs = vd.LastDecisionSignatures
			}
		}

		valid++
	}
	if valid >= v.quorum {
		// TODO handle in flight
		v.Logger.Debugf("Changing to view %d with sequence %d and last decision %v", v.currView, maxLastDecisionSequence+1, maxLastDecision)
		v.commitLastDecision(maxLastDecisionSequence, maxLastDecision, maxLastDecisionSigs)
		v.Controller.ViewChanged(v.currView, maxLastDecisionSequence+1)
	}
}

func (v *ViewChanger) commitLastDecision(lastDecisionSequence uint64, lastDecision *protos.Proposal, lastDecisionSigs []*protos.Signature) {
	myLastDecision, _ := v.Checkpoint.Get()
	if lastDecisionSequence == 0 {
		return
	}
	proposal := types.Proposal{
		Header:               lastDecision.Header,
		Metadata:             lastDecision.Metadata,
		Payload:              lastDecision.Payload,
		VerificationSequence: int64(lastDecision.VerificationSequence),
	}
	signatures := make([]types.Signature, 0)
	for _, sig := range lastDecisionSigs {
		signature := types.Signature{
			Id:    sig.Signer,
			Value: sig.Value,
			Msg:   sig.Msg,
		}
		signatures = append(signatures, signature)
	}
	if myLastDecision.Metadata == nil { // I am at genesis proposal
		if lastDecisionSequence == 1 { // and one decision behind
			v.deliverDecision(proposal, signatures)
			return
		}
		v.Synchronizer.Sync() // TODO make sure sync succeeds
		return
	}
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(myLastDecision.Metadata, md); err != nil {
		v.Logger.Panicf("Node %d is unable to unmarshal its own last decision metadata from checkpoint, err: %v", v.SelfID, err)
	}
	if md.LatestSequence == lastDecisionSequence-1 { // I am one decision behind
		v.deliverDecision(proposal, signatures)
		return
	}
	if md.LatestSequence < lastDecisionSequence { // I am far behind
		v.Synchronizer.Sync() // TODO make sure sync succeeds
		return
	}
	if md.LatestSequence > lastDecisionSequence+1 {
		v.Logger.Panicf("Node %d has a checkpoint for sequence %d which is much greater than the last decision sequence %d", v.SelfID, md.LatestSequence, lastDecisionSequence)
	}
}

func (v *ViewChanger) deliverDecision(proposal types.Proposal, signatures []types.Signature) {
	v.Logger.Debugf("Delivering to app the last decision proposal %v", proposal)
	v.Application.Deliver(proposal, signatures)
	v.Checkpoint.Set(proposal, signatures)
	requests, err := v.Verifier.VerifyProposal(proposal)
	if err != nil {
		v.Logger.Panicf("Node %d is unable to verify the last decision proposal, err: %v", v.SelfID, err)
	}
	for _, reqInfo := range requests {
		if err := v.RequestsTimer.RemoveRequest(reqInfo); err != nil {
			v.Logger.Warnf("Error during remove of request %s from the pool, err: %v", reqInfo, err)
		}
	}
}
