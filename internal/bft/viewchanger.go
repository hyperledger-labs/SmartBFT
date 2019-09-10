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
	"github.com/pkg/errors"
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

type change struct {
	view     uint64
	stopView bool
}

type viewAndSeq struct {
	view uint64
	seq  uint64
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
	Synchronizer Synchronizer

	Checkpoint *types.Checkpoint
	InFlight   *InFlightData
	State      State

	Controller    ViewController
	RequestsTimer RequestsTimer

	Ticker              <-chan time.Time
	lastTick            time.Time
	ResendTimeout       time.Duration
	lastResend          time.Time
	TimeoutViewChange   time.Duration
	startViewChangeTime time.Time
	checkTimeout        bool

	// Runtime
	InMsqQSize      int
	incMsgs         chan *incMsg
	viewChangeMsgs  *voteSet
	viewDataMsgs    *voteSet
	currView        uint64
	nextView        uint64
	leader          uint64
	startChangeChan chan *change
	informChan      chan *viewAndSeq

	stopOnce sync.Once
	stopChan chan struct{}
	vcDone   sync.WaitGroup
}

// Start the view changer
func (v *ViewChanger) Start(startViewNumber uint64) {
	v.incMsgs = make(chan *incMsg, v.InMsqQSize)
	v.startChangeChan = make(chan *change, 1)
	v.informChan = make(chan *viewAndSeq, 1)

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

	v.lastTick = time.Now()
	v.lastResend = v.lastTick

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
		case change := <-v.startChangeChan:
			v.startViewChange(change)
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case now := <-v.Ticker:
			v.lastTick = now
			v.checkIfResendViewChange(now)
			v.checkIfTimeout(now)
		case info := <-v.informChan:
			v.informNewView(info)
		}
	}
}

func (v *ViewChanger) checkIfResendViewChange(now time.Time) {
	nextTimeout := v.lastResend.Add(v.ResendTimeout)
	if nextTimeout.After(now) { // check if it is time to resend
		return
	}
	if v.checkTimeout { // during view change process
		msg := &protos.Message{
			Content: &protos.Message_ViewChange{
				ViewChange: &protos.ViewChange{
					NextView: v.nextView,
				},
			},
		}
		v.Comm.BroadcastConsensus(msg)
		v.Logger.Debugf("Node %d resent a view change message with next view %d", v.SelfID, v.nextView)
		v.lastResend = now // update last resend time, or at least last time we checked if we should resend
	}
}

func (v *ViewChanger) checkIfTimeout(now time.Time) {
	if !v.checkTimeout {
		return
	}
	nextTimeout := v.startViewChangeTime.Add(v.TimeoutViewChange)
	if nextTimeout.After(now) { // check if timeout has passed
		return
	}
	v.Logger.Debugf("Node %d got a view change timeout, the current view is %d", v.SelfID, v.currView)
	v.checkTimeout = false // stop timeout for now, a new one will start when a new view change begins
	// the timeout has passed, something went wrong, try sync and complain
	v.Synchronizer.Sync()
	v.StartViewChange(v.currView, false) // don't stop the view, the sync maybe created a good view
}

func (v *ViewChanger) processMsg(sender uint64, m *protos.Message) {
	// viewChange message
	if vc := m.GetViewChange(); vc != nil {
		v.Logger.Debugf("Node %d is processing a view change message %v from %d with next view %d", v.SelfID, m, sender, vc.NextView)
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
		v.Logger.Debugf("Node %d is processing a view data message %s from %d", v.SelfID, MsgToString(m), sender)
		if !v.validateViewDataMsg(vd, sender) {
			return
		}
		v.viewDataMsgs.registerVote(sender, m)
		v.processViewDataMsg()
		return
	}

	// newView message
	if nv := m.GetNewView(); nv != nil {
		v.Logger.Debugf("Node %d is processing a new view message %s from %d", v.SelfID, MsgToString(m), sender)
		if sender != v.leader {
			v.Logger.Warnf("Node %d got newView message %v from %d, expected sender to be %d the next leader", v.SelfID, MsgToString(m), sender, v.leader)
			return
		}
		v.processNewViewMsg(nv)
	}
}

// InformNewView tells the view changer to advance to a new view number
func (v *ViewChanger) InformNewView(view uint64, seq uint64) {
	select {
	case v.informChan <- &viewAndSeq{
		view: view,
		seq:  seq,
	}:
	case <-v.stopChan:
		return
	}
}

func (v *ViewChanger) informNewView(info *viewAndSeq) {
	view := info.view
	if view <= v.currView {
		v.Logger.Debugf("Node %d was informed of view %d, but the current view is %d", v.SelfID, view, v.currView)
		return
	}
	v.Logger.Debugf("Node %d was informed of a new view %d", v.SelfID, view)
	v.currView = view
	v.nextView = v.currView
	v.leader = getLeaderID(v.currView, v.N, v.nodes)
	v.viewChangeMsgs.clear(v.N)
	v.viewDataMsgs.clear(v.N)
	v.checkTimeout = false
	v.RequestsTimer.RestartTimers()
}

// StartViewChange initiates a view change
func (v *ViewChanger) StartViewChange(view uint64, stopView bool) {
	select {
	case v.startChangeChan <- &change{view: view, stopView: stopView}:
	default:
	}
}

// StartViewChange stops current view and timeouts, and broadcasts a view change message to all
func (v *ViewChanger) startViewChange(change *change) {
	if change.view < v.currView { // this is about an old view
		v.Logger.Debugf("Node %d has a view change request with an old view %d, while the current view is %d", v.SelfID, change.view, v.currView)
		return
	}
	v.nextView = v.currView + 1
	v.RequestsTimer.StopTimers()
	msg := &protos.Message{
		Content: &protos.Message_ViewChange{
			ViewChange: &protos.ViewChange{
				NextView: v.nextView,
			},
		},
	}
	v.Comm.BroadcastConsensus(msg)
	v.Logger.Debugf("Node %d started view change, last view is %d", v.SelfID, v.currView)
	if change.stopView {
		v.Controller.AbortView() // abort the current view when joining view change
	}
	v.startViewChangeTime = v.lastTick
	v.checkTimeout = true
}

func (v *ViewChanger) processViewChangeMsg() {
	if uint64(len(v.viewChangeMsgs.voted)) == uint64(v.f+1) { // join view change
		v.Logger.Debugf("Node %d is joining view change, last view is %d", v.SelfID, v.currView)
		v.startViewChange(&change{v.currView, true})
	}
	if len(v.viewChangeMsgs.voted) >= v.quorum-1 && v.nextView > v.currView { // send view data
		v.currView = v.nextView
		v.leader = getLeaderID(v.currView, v.N, v.nodes)
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
	prepared := v.InFlight.IsInFlightPrepared()
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
	if lastDecision.Metadata == nil {
		return proposal // this is the first proposal after genesis
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
	err, lastSequence := ValidateLastDecision(rvd, v.quorum, v.N, v.Verifier)
	if err != nil {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but the last decision is invalid, reason: %v", v.SelfID, rvd, sender, err)
		return false
	}
	if err := ValidateInFlight(rvd.InFlightProposal, lastSequence); err != nil {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but the in flight proposal is invalid, reason: %v", v.SelfID, rvd, sender, err)
		return false
	}
	return true
}

func ValidateLastDecision(vd *protos.ViewData, quorum int, N uint64, verifier api.Verifier) (err error, lastSequence uint64) {
	if vd.LastDecision == nil {
		return errors.Errorf("the last decision is not set"), 0
	}
	if vd.LastDecision.Metadata == nil {
		// This is a genesis proposal, there are no signatures to validate, so we return at this point
		return nil, 0
	}
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(vd.LastDecision.Metadata, md); err != nil {
		return errors.Errorf("unable to unmarshal last decision metadata, err: %v", err), 0
	}
	if md.ViewId >= vd.NextView {
		return errors.Errorf("last decision view %d is greater or equal to requested next view %d", md.ViewId, vd.NextView), 0
	}
	numSigs := len(vd.LastDecisionSignatures)
	if numSigs < quorum {
		return errors.Errorf("there are only %d last decision signatures", numSigs), 0
	}
	nodesMap := make(map[uint64]struct{}, N)
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
		if err := verifier.VerifyConsenterSig(signature, proposal); err != nil {
			return errors.Errorf("last decision signature is invalid, error: %v", err), 0
		}
		validSig++
	}
	if validSig < quorum {
		return errors.Errorf("there are only %d valid last decision signatures", validSig), 0
	}
	return nil, md.LatestSequence
}

func ValidateInFlight(inFlightProposal *protos.Proposal, lastSequence uint64) error {
	if inFlightProposal == nil {
		return nil
	}
	if inFlightProposal.Metadata == nil {
		return errors.Errorf("in flight proposal metadata is nil")
	}
	inFlightMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(inFlightProposal.Metadata, inFlightMetadata); err != nil {
		return errors.Errorf("unable to unmarshal the in flight proposal metadata, err: %v", err)
	}
	if inFlightMetadata.LatestSequence != lastSequence+1 {
		return errors.Errorf("the in flight proposal sequence is %d while the last decision sequence is %d", inFlightMetadata.LatestSequence, lastSequence)
	}
	return nil
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

		err, lastSequence := ValidateLastDecision(vd, v.quorum, v.N, v.Verifier)
		if err != nil {
			v.Logger.Warnf("Node %d is processing newView message, but the last decision in viewData %v is invalid, reason: %v", v.SelfID, vd, err)
			continue
		}

		if err := ValidateInFlight(vd.InFlightProposal, lastSequence); err != nil {
			v.Logger.Warnf("Node %d is processing newView message, but the in flight in viewData %v is invalid, reason: %v", v.SelfID, vd, err)
			continue
		}

		v.Logger.Debugf("Current max sequence is %d and this viewData %v last decision sequence is %d", maxLastDecisionSequence, vd, lastSequence)
		if lastSequence > maxLastDecisionSequence {
			maxLastDecisionSequence = lastSequence
			maxLastDecision = vd.LastDecision
			maxLastDecisionSigs = vd.LastDecisionSignatures
		}

		valid++
	}
	if valid >= v.quorum {
		viewToChange := v.currView
		v.Logger.Debugf("Changing to view %d with sequence %d and last decision %v", viewToChange, maxLastDecisionSequence+1, maxLastDecision)
		v.commitLastDecision(maxLastDecisionSequence, maxLastDecision, maxLastDecisionSigs)
		if viewToChange == v.currView { // commitLastDecision did not cause a sync that cause an increase in the view
			newViewToSave := &protos.SavedMessage{
				Content: &protos.SavedMessage_NewView{
					NewView: msg,
				},
			}
			if err := v.State.Save(newViewToSave); err != nil {
				v.Logger.Panicf("Failed to save message to state, error: %v", err)
			}
			v.Controller.ViewChanged(v.currView, maxLastDecisionSequence+1)
		}
		v.RequestsTimer.RestartTimers()
		v.checkTimeout = false
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
		v.sync(lastDecisionSequence)
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
		v.sync(lastDecisionSequence)
		return
	}
	if md.LatestSequence > lastDecisionSequence+1 {
		v.Logger.Panicf("Node %d has a checkpoint for sequence %d which is much greater than the last decision sequence %d", v.SelfID, md.LatestSequence, lastDecisionSequence)
	}
}

func (v *ViewChanger) sync(targetSeq uint64) {
	for {
		v.Synchronizer.Sync()
		select { // wait for sync to return with expected info
		case info := <-v.informChan:
			if info.seq >= targetSeq {
				v.informNewView(info)
				return
			}
		case <-v.stopChan:
		case now := <-v.Ticker:
			v.lastTick = now
			v.checkIfTimeout(now)
		case change := <-v.startChangeChan:
			v.startViewChange(change)
		}
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
