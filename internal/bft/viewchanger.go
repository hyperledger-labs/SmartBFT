// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// ViewController controls the view
//go:generate mockery -dir . -name ViewController -case underscore -output ./mocks/
type ViewController interface {
	ViewChanged(newViewNumber uint64, newProposalSequence uint64)
	AbortView(view uint64)
}

// Pruner prunes revoked requests
//go:generate mockery -dir . -name Pruner -case underscore -output ./mocks/
type Pruner interface {
	MaybePruneRevokedRequests()
}

// RequestsTimer controls requests
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

// ViewChanger is responsible for running the view change protocol
type ViewChanger struct {
	// Configuration
	SelfID            uint64
	NodesList         []uint64
	N                 uint64
	f                 int
	quorum            int
	SpeedUpViewChange bool

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
	Pruner        Pruner

	// for the in flight proposal view
	ViewSequences      *atomic.Value
	inFlightDecideChan chan struct{}
	inFlightSyncChan   chan struct{}
	inFlightView       *View
	inFlightViewLock   sync.RWMutex

	Ticker              <-chan time.Time
	lastTick            time.Time
	ResendTimeout       time.Duration
	lastResend          time.Time
	ViewChangeTimeout   time.Duration
	startViewChangeTime time.Time
	checkTimeout        bool
	backOffFactor       uint64

	// Runtime
	Restore         chan struct{}
	InMsqQSize      int
	incMsgs         chan *incMsg
	viewChangeMsgs  *voteSet
	viewDataMsgs    *voteSet
	currView        uint64
	nextView        uint64
	leader          uint64
	startChangeChan chan *change
	informChan      chan *types.ViewAndSeq

	stopOnce sync.Once
	stopChan chan struct{}
	vcDone   sync.WaitGroup
}

// Start the view changer
func (v *ViewChanger) Start(startViewNumber uint64) {
	v.incMsgs = make(chan *incMsg, v.InMsqQSize)
	v.startChangeChan = make(chan *change, 1)
	v.informChan = make(chan *types.ViewAndSeq, 1)

	v.quorum, v.f = computeQuorum(v.N)

	v.stopChan = make(chan struct{})
	v.stopOnce = sync.Once{}
	v.vcDone.Add(1)

	v.setupVotes()

	// set without locking
	v.currView = startViewNumber
	v.nextView = v.currView
	v.leader = getLeaderID(v.currView, v.N, v.NodesList)

	v.lastTick = time.Now()
	v.lastResend = v.lastTick

	v.backOffFactor = 1

	v.inFlightDecideChan = make(chan struct{})
	v.inFlightSyncChan = make(chan struct{})

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
		case <-v.Restore:
			v.processViewChangeMsg(true)
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
	nextTimeout := v.startViewChangeTime.Add(v.ViewChangeTimeout * time.Duration(v.backOffFactor))
	if nextTimeout.After(now) { // check if timeout has passed
		return
	}
	v.Logger.Debugf("Node %d got a view change timeout, the current view is %d", v.SelfID, v.currView)
	v.checkTimeout = false // stop timeout for now, a new one will start when a new view change begins
	v.backOffFactor++      // next timeout will be longer
	// the timeout has passed, something went wrong, try sync and complain
	v.Logger.Debugf("Node %d is calling sync because it got a view change timeout", v.SelfID)
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
		v.processViewChangeMsg(false)
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
	case v.informChan <- &types.ViewAndSeq{
		View: view,
		Seq:  seq,
	}:
	case <-v.stopChan:
		return
	}
}

func (v *ViewChanger) informNewView(info *types.ViewAndSeq) {
	view := info.View
	if view < v.currView {
		v.Logger.Debugf("Node %d was informed of view %d, but the current view is %d", v.SelfID, view, v.currView)
		return
	}
	v.Logger.Debugf("Node %d was informed of a new view %d", v.SelfID, view)
	v.currView = view
	v.nextView = v.currView
	v.leader = getLeaderID(v.currView, v.N, v.NodesList)
	v.viewChangeMsgs.clear(v.N)
	v.viewDataMsgs.clear(v.N)
	v.checkTimeout = false
	v.backOffFactor = 1 //reset
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
	if v.nextView == v.currView+1 {
		v.Logger.Debugf("Node %d has already started view change with last view %d", v.SelfID, v.currView)
		v.checkTimeout = true // make sure timeout is checked anyway
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
		v.Controller.AbortView(v.currView) // abort the current view when joining view change
	}
	v.startViewChangeTime = v.lastTick
	v.checkTimeout = true
}

func (v *ViewChanger) processViewChangeMsg(restore bool) {
	if ((uint64(len(v.viewChangeMsgs.voted)) == uint64(v.f+1)) && v.SpeedUpViewChange) || restore { // join view change
		v.Logger.Debugf("Node %d is joining view change, last view is %d", v.SelfID, v.currView)
		v.startViewChange(&change{v.currView, true})
	}
	if (len(v.viewChangeMsgs.voted) >= v.quorum-1) || restore { // send view data
		if !v.SpeedUpViewChange {
			v.Logger.Debugf("Node %d is joining view change, last view is %d", v.SelfID, v.currView)
			v.startViewChange(&change{v.currView, true})
		}
		if !restore {
			msgToSave := &protos.SavedMessage{
				Content: &protos.SavedMessage_ViewChange{
					ViewChange: &protos.ViewChange{
						NextView: v.currView,
					},
				},
			}
			if err := v.State.Save(msgToSave); err != nil {
				v.Logger.Panicf("Failed to save message to state, error: %v", err)
			}
		}
		v.currView = v.nextView
		v.leader = getLeaderID(v.currView, v.N, v.NodesList)
		v.viewChangeMsgs.clear(v.N)
		v.viewDataMsgs.clear(v.N) // clear because currView changed

		msg := v.prepareViewDataMsg()
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
	if err := v.Verifier.VerifySignature(types.Signature{ID: vd.Signer, Value: vd.Signature, Msg: vd.RawViewData}); err != nil {
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
	if getLeaderID(rvd.NextView, v.N, v.NodesList) != v.SelfID { // check if I am the next leader
		v.Logger.Warnf("Node %d got viewData message %v from %d, but %d is not the next leader", v.SelfID, rvd, sender, v.SelfID)
		return false
	}
	lastSequence, err := ValidateLastDecision(rvd, v.quorum, v.N, v.Verifier)
	if err != nil {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but the last decision is invalid, reason: %v", v.SelfID, rvd, sender, err)
		return false
	}
	v.Logger.Debugf("Node %d got viewData message %v from %d, the last decision with sequence %d is valid", v.SelfID, rvd, sender, lastSequence)
	if err := ValidateInFlight(rvd.InFlightProposal, lastSequence); err != nil {
		v.Logger.Warnf("Node %d got viewData message %v from %d, but the in flight proposal is invalid, reason: %v", v.SelfID, rvd, sender, err)
		return false
	}
	v.Logger.Debugf("Node %d got viewData message %v from %d, the in flight proposal is valid", v.SelfID, rvd, sender)
	return true
}

// ValidateLastDecision validates the given decision, and returns its sequence when valid
func ValidateLastDecision(vd *protos.ViewData, quorum int, N uint64, verifier api.Verifier) (lastSequence uint64, err error) {
	if vd.LastDecision == nil {
		return 0, errors.Errorf("the last decision is not set")
	}
	if vd.LastDecision.Metadata == nil {
		// This is a genesis proposal, there are no signatures to validate, so we return at this point
		return 0, nil
	}
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(vd.LastDecision.Metadata, md); err != nil {
		return 0, errors.Errorf("unable to unmarshal last decision metadata, err: %v", err)
	}
	if md.ViewId >= vd.NextView {
		return 0, errors.Errorf("last decision view %d is greater or equal to requested next view %d", md.ViewId, vd.NextView)
	}
	numSigs := len(vd.LastDecisionSignatures)
	if numSigs < quorum {
		return 0, errors.Errorf("there are only %d last decision signatures", numSigs)
	}
	nodesMap := make(map[uint64]struct{}, N)
	validSig := 0
	for _, sig := range vd.LastDecisionSignatures {
		if _, exist := nodesMap[sig.Signer]; exist {
			continue // seen signature from this node already
		}
		nodesMap[sig.Signer] = struct{}{}
		signature := types.Signature{
			ID:    sig.Signer,
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
			return 0, errors.Errorf("last decision signature is invalid, error: %v", err)
		}
		validSig++
	}
	if validSig < quorum {
		return 0, errors.Errorf("there are only %d valid last decision signatures", validSig)
	}
	return md.LatestSequence, nil
}

// ValidateInFlight validates the given in-flight proposal
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
		v.Logger.Debugf("Node %d got a quorum of viewData messages", v.SelfID)
		ok, _, _, err := CheckInFlight(v.getViewDataMessages(), v.f, v.quorum, v.N, v.Verifier)
		if err != nil {
			v.Logger.Panicf("Node %d checked the in flight and it got an error: %v", v.SelfID, err)
		}
		if !ok {
			v.Logger.Debugf("Node %d checked the in flight and it was invalid", v.SelfID)
			return
		}
		v.Logger.Debugf("Node %d checked the in flight and it was valid", v.SelfID)
		// create the new view message
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
		v.Logger.Debugf("Node %d is broadcasting a new view msg", v.SelfID)
		v.Comm.BroadcastConsensus(msg)
		v.Logger.Debugf("Node %d sent a new view msg to self", v.SelfID)
		v.processMsg(v.SelfID, msg) // also send to myself
		v.viewDataMsgs.clear(v.N)
		v.Logger.Debugf("Node %d sent a new view msg", v.SelfID)
	}
}

func (v *ViewChanger) getViewDataMessages() []*protos.ViewData {
	num := len(v.viewDataMsgs.votes)
	messages := make([]*protos.ViewData, 0)
	for i := 0; i < num; i++ {
		vote := <-v.viewDataMsgs.votes
		vd := &protos.ViewData{}
		if err := proto.Unmarshal(vote.GetViewData().RawViewData, vd); err != nil {
			v.Logger.Panicf("Node %d was unable to unmarshal viewData message, error: %v", v.SelfID, err)
		}
		messages = append(messages, vd)
		v.viewDataMsgs.votes <- vote
	}
	return messages
}

type possibleProposal struct {
	proposal    *protos.Proposal
	preprepared int
	noArgument  int
}

type proposalAndMetadata struct {
	proposal *protos.Proposal
	metadata *protos.ViewMetadata
}

// CheckInFlight checks if there is an in-flight proposal that needs to be decided on (because a node might decided on it already)
func CheckInFlight(messages []*protos.ViewData, f int, quorum int, N uint64, verifier api.Verifier) (ok, noInFlight bool, inFlightProposal *protos.Proposal, err error) {
	expectedSequence := maxLastDecisionSequence(messages, quorum, N, verifier) + 1
	possibleProposals := make([]*possibleProposal, 0)
	proposalsAndMetadata := make([]*proposalAndMetadata, 0)
	noInFlightCount := 0
	for _, vd := range messages {

		if vd.InFlightProposal == nil { // there is no in flight proposal here
			noInFlightCount++
			proposalsAndMetadata = append(proposalsAndMetadata, &proposalAndMetadata{nil, nil})
			continue
		}

		if vd.InFlightProposal.Metadata == nil { // should have been validated earlier
			return false, false, nil, errors.Errorf("Node has a view data message where the in flight proposal metadata is nil")
		}

		inFlightMetadata := &protos.ViewMetadata{}
		if err := proto.Unmarshal(vd.InFlightProposal.Metadata, inFlightMetadata); err != nil { // should have been validated earlier
			return false, false, nil, errors.Errorf("Node was unable to unmarshal the in flight proposal metadata, error: %v", err)
		}

		proposalsAndMetadata = append(proposalsAndMetadata, &proposalAndMetadata{vd.InFlightProposal, inFlightMetadata})

		if inFlightMetadata.LatestSequence != expectedSequence { // the in flight proposal sequence is not as expected
			noInFlightCount++
			continue
		}

		// now the in flight proposal is with the expected sequence
		// find possible proposals

		if !vd.InFlightPrepared { // no prepared so isn't a possible proposal
			continue
		}

		// this proposal is prepared and so it is possible
		alreadyExists := false
		for _, p := range possibleProposals {
			if proto.Equal(p.proposal, vd.InFlightProposal) {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			// this is not a proposal we have seen before
			possibleProposals = append(possibleProposals, &possibleProposal{proposal: vd.InFlightProposal})
		}
	}

	// condition B holds
	if noInFlightCount >= quorum { // there is a quorum of messages that support that there is no in flight proposal
		return true, true, nil, nil
	}

	// fill out info on all possible proposals
	for _, prop := range proposalsAndMetadata {
		for _, possible := range possibleProposals {

			if prop.proposal == nil {
				possible.noArgument++
				continue
			}

			if prop.metadata.LatestSequence != expectedSequence {
				possible.noArgument++
				continue
			}

			if proto.Equal(prop.proposal, possible.proposal) {
				possible.noArgument++
				possible.preprepared++
			}

		}
	}

	// see if there is an in flight proposal that is agreed on
	agreed := -1
	for i, possible := range possibleProposals {
		if possible.preprepared < f+1 { // condition A2 doesn't hold
			continue
		}
		if possible.noArgument < quorum { // condition A1 doesn't hold
			continue
		}
		agreed = i
		break
	}

	// condition A holds
	if agreed != -1 {
		return true, false, possibleProposals[agreed].proposal, nil
	}

	return false, false, nil, nil
}

func maxLastDecisionSequence(messages []*protos.ViewData, quorum int, N uint64, verifier api.Verifier) uint64 {
	max := uint64(0)
	for _, vd := range messages {
		seq, err := ValidateLastDecision(vd, quorum, N, verifier)
		if err != nil {
			panic(fmt.Sprintf("Node was unable to validate last decision in viewData message, error: %v", err))
		}

		if seq > max {
			max = seq
		}
	}
	return max
}

func (v *ViewChanger) processNewViewMsg(msg *protos.NewView) {
	signed := msg.GetSignedViewData()
	nodesMap := make(map[uint64]struct{}, v.N)
	valid := 0
	var maxLastDecisionSequence uint64
	var maxLastDecision *protos.Proposal
	var maxLastDecisionSigs []*protos.Signature
	var viewDataMessages []*protos.ViewData
	for _, svd := range signed {
		if _, exist := nodesMap[svd.Signer]; exist {
			continue // seen data from this node already
		}
		nodesMap[svd.Signer] = struct{}{}

		if err := v.Verifier.VerifySignature(types.Signature{ID: svd.Signer, Value: svd.Signature, Msg: svd.RawViewData}); err != nil {
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

		lastSequence, err := ValidateLastDecision(vd, v.quorum, v.N, v.Verifier)
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
		viewDataMessages = append(viewDataMessages, vd)
	}
	if valid >= v.quorum {

		v.Logger.Debugf("Node %d found a quorum of valid view data messages within the new view message", v.SelfID)

		ok, noInFlight, inFlightProposal, err := CheckInFlight(viewDataMessages, v.f, v.quorum, v.N, v.Verifier)
		if err != nil {
			v.Logger.Panicf("The check of the in flight proposal by node %d returned an error: %v", v.SelfID, err)
		}
		if !ok {
			v.Logger.Debugf("The check of the in flight proposal by node %d did not pass", v.SelfID)
			return
		}

		viewToChange := v.currView
		v.Logger.Debugf("Changing to view %d with sequence %d and last decision %v", v.currView, maxLastDecisionSequence+1, maxLastDecision)
		calledSync := v.commitLastDecision(maxLastDecisionSequence, maxLastDecision, maxLastDecisionSigs)

		if !noInFlight && !calledSync {
			v.commitInFlightProposal(inFlightProposal)
		}

		if viewToChange == v.currView { // commitLastDecision did not cause a sync that cause an increase in the view
			newViewToSave := &protos.SavedMessage{
				Content: &protos.SavedMessage_NewView{
					NewView: &protos.ViewMetadata{
						ViewId:         v.currView,
						LatestSequence: maxLastDecisionSequence,
					},
				},
			}
			if err := v.State.Save(newViewToSave); err != nil {
				v.Logger.Panicf("Failed to save message to state, error: %v", err)
			}
			v.Controller.ViewChanged(v.currView, maxLastDecisionSequence+1)
		}
		v.RequestsTimer.RestartTimers()
		v.checkTimeout = false
		v.backOffFactor = 1 // reset
	}
}

func (v *ViewChanger) commitLastDecision(lastDecisionSequence uint64, lastDecision *protos.Proposal, lastDecisionSigs []*protos.Signature) (calledSync bool) {
	myLastDecision, _ := v.Checkpoint.Get()
	if lastDecisionSequence == 0 {
		return false
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
			ID:    sig.Signer,
			Value: sig.Value,
			Msg:   sig.Msg,
		}
		signatures = append(signatures, signature)
	}
	if myLastDecision.Metadata == nil { // I am at genesis proposal
		if lastDecisionSequence == 1 { // and one decision behind
			v.deliverDecision(proposal, signatures)
			return false
		}
		v.Logger.Debugf("Node %d is calling sync because it is far behind, the last decision sequence is %d while the the node only has the genesis proposal ", v.SelfID, lastDecisionSequence)
		v.sync(lastDecisionSequence)
		return true
	}
	md := &protos.ViewMetadata{}
	if err := proto.Unmarshal(myLastDecision.Metadata, md); err != nil {
		v.Logger.Panicf("Node %d is unable to unmarshal its own last decision metadata from checkpoint, err: %v", v.SelfID, err)
	}
	if md.LatestSequence == lastDecisionSequence-1 { // I am one decision behind
		v.deliverDecision(proposal, signatures)
		return false
	}
	if md.LatestSequence < lastDecisionSequence { // I am far behind
		v.Logger.Debugf("Node %d is calling sync because it is far behind, the last decision sequence is %d while the the node's sequence is %d ", v.SelfID, lastDecisionSequence, md.LatestSequence)
		v.sync(lastDecisionSequence)
		return true
	}
	if md.LatestSequence > lastDecisionSequence+1 {
		v.Logger.Panicf("Node %d has a checkpoint for sequence %d which is much greater than the last decision sequence %d", v.SelfID, md.LatestSequence, lastDecisionSequence)
	}
	return false
}

func (v *ViewChanger) sync(targetSeq uint64) {
	for {
		v.Synchronizer.Sync()
		select { // wait for sync to return with expected info
		case info := <-v.informChan:
			if info.Seq >= targetSeq {
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
	v.Pruner.MaybePruneRevokedRequests()
}

func (v *ViewChanger) commitInFlightProposal(proposal *protos.Proposal) {
	myLastDecision, _ := v.Checkpoint.Get()
	if proposal == nil {
		v.Logger.Panicf("The in flight proposal is nil")
	}
	proposalMD := &protos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, proposalMD); err != nil {
		v.Logger.Panicf("Node %d is unable to unmarshal the in flight proposal metadata, err: %v", v.SelfID, err)
	}

	if myLastDecision.Metadata != nil { // if metadata is nil then I am at genesis proposal and I should commit the in flight proposal anyway
		lastDecisionMD := &protos.ViewMetadata{}
		if err := proto.Unmarshal(myLastDecision.Metadata, lastDecisionMD); err != nil {
			v.Logger.Panicf("Node %d is unable to unmarshal its own last decision metadata from checkpoint, err: %v", v.SelfID, err)
		}
		if lastDecisionMD.LatestSequence == proposalMD.LatestSequence {
			v.Logger.Debugf("Node %d already decided on sequence %d and so it will not commit the in flight proposal with the same sequence", v.SelfID, lastDecisionMD.LatestSequence)
			return // I already decided on the in flight proposal
		}
		if lastDecisionMD.LatestSequence != proposalMD.LatestSequence-1 {
			v.Logger.Panicf("Node %d got an in flight proposal with sequence %d while its last decision was on sequence %d", v.SelfID, proposalMD.LatestSequence, lastDecisionMD.LatestSequence)
		}
	}

	v.Logger.Debugf("Node %d is creating a view for the in flight proposal", v.SelfID)

	v.inFlightViewLock.Lock()
	v.inFlightView = &View{
		SelfID:           v.SelfID,
		N:                v.N,
		Number:           proposalMD.ViewId,
		LeaderID:         v.SelfID, // so that no byzantine leader will cause a complain
		Quorum:           v.quorum,
		Decider:          v,
		FailureDetector:  v,
		Sync:             v,
		Logger:           v.Logger,
		Comm:             v.Comm,
		Verifier:         v.Verifier,
		Signer:           v.Signer,
		ProposalSequence: proposalMD.LatestSequence,
		State:            v.State,
		InMsgQSize:       v.InMsqQSize,
		ViewSequences:    v.ViewSequences,
		Phase:            PREPARED,
	}

	v.inFlightView.inFlightProposal = &types.Proposal{
		VerificationSequence: int64(proposal.VerificationSequence),
		Metadata:             proposal.Metadata,
		Payload:              proposal.Payload,
		Header:               proposal.Header,
	}
	v.inFlightView.myProposalSig = v.Signer.SignProposal(*v.inFlightView.inFlightProposal)
	v.inFlightView.lastBroadcastSent = &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   v.inFlightView.Number,
				Digest: v.inFlightView.inFlightProposal.Digest(),
				Seq:    v.inFlightView.ProposalSequence,
				Signature: &protos.Signature{
					Signer: v.inFlightView.myProposalSig.ID,
					Value:  v.inFlightView.myProposalSig.Value,
					Msg:    v.inFlightView.myProposalSig.Msg,
				},
			},
		},
	}

	v.inFlightView.Start()
	v.inFlightViewLock.Unlock()

	v.Logger.Debugf("Node %d started a view for the in flight proposal", v.SelfID)

	select { // wait for view to finish
	case <-v.inFlightDecideChan:
	case <-v.inFlightSyncChan:
	case <-v.stopChan:
	case now := <-v.Ticker:
		v.lastTick = now
		v.checkIfTimeout(now)
	}

	v.inFlightView.Abort()
}

// Decide delivers to the application and informs the view changer after delivery
func (v *ViewChanger) Decide(proposal types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	v.inFlightView.stop()
	v.Logger.Debugf("Delivering to app the last decision proposal %v", proposal)
	v.Application.Deliver(proposal, signatures)
	v.Checkpoint.Set(proposal, signatures)
	for _, reqInfo := range requests {
		if err := v.RequestsTimer.RemoveRequest(reqInfo); err != nil {
			v.Logger.Warnf("Error during remove of request %s from the pool, err: %v", reqInfo, err)
		}
	}
	v.Pruner.MaybePruneRevokedRequests()
	v.inFlightDecideChan <- struct{}{}
}

// Complain panics when a view change is requested
func (v *ViewChanger) Complain(viewNum uint64, stopView bool) {
	v.Logger.Panicf("Node %d has complained while in the view for the in flight proposal", v.SelfID)
}

// Sync calls the synchronizer and informs the view changer of the sync
func (v *ViewChanger) Sync() {
	// the in flight proposal view asked to sync
	v.Logger.Debugf("Node %d is calling sync because the in flight proposal view has asked to sync", v.SelfID)
	v.Synchronizer.Sync()
	v.inFlightSyncChan <- struct{}{}
}

// HandleViewMessage passes a message to the in flight proposal view if applicable
func (v *ViewChanger) HandleViewMessage(sender uint64, m *protos.Message) {
	v.inFlightViewLock.RLock()
	defer v.inFlightViewLock.RUnlock()
	if view := v.inFlightView; view != nil {
		v.Logger.Debugf("Node %d is passing a message to the in flight view", v.SelfID)
		view.HandleMessage(sender, m)
	}
}
