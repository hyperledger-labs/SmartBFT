// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

//go:generate mockery -dir . -name ViewController -case underscore -output ./mocks/
type ViewController interface {
	ViewChanged(newViewNumber uint64, newProposalSequence uint64)
}

type ViewChanger struct {
	// Configuration
	SelfID     uint64
	nodes      []uint64
	N          uint64
	F          uint64
	Quorum     int
	Logger     api.Logger
	Comm       Comm
	Signer     api.Signer
	Controller ViewController
	Verifier   api.Verifier

	// Runtime
	incMsgs        chan *incMsg
	viewChangeMsgs *voteSet
	viewDataMsgs   *voteSet
	CurrView       uint64
	NextView       uint64
	Leader         uint64

	stopOnce sync.Once
	stopChan chan struct{}
	vcDone   sync.WaitGroup
}

// Start the view changer
func (v *ViewChanger) Start() {
	v.incMsgs = make(chan *incMsg, 10*v.N) // TODO channel size should be configured

	v.nodes = v.Comm.Nodes()

	v.stopChan = make(chan struct{})
	v.stopOnce = sync.Once{}
	v.vcDone.Add(1)

	v.setupVotes()

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
		}
	}
}

func (v *ViewChanger) processMsg(sender uint64, m *protos.Message) {
	// viewChange message
	if vc := m.GetViewChange(); vc != nil {
		// check view number
		if vc.NextView != v.CurrView+1 { // accept view change only to immediate next view number
			v.Logger.Warnf("%d got viewChange message %v from %d with view %d, expected view %d", v.SelfID, m, sender, vc.NextView, v.CurrView+1)
			return
		}
		v.viewChangeMsgs.registerVote(sender, m)
		v.processViewChangeMsg()
		return
	}

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
		if sender != v.Leader {
			v.Logger.Warnf("%d got newView message %v from %d, expected sender to be %d the next leader", v.SelfID, m, sender, v.Leader)
			return
		}
		// TODO check view number here?
		v.processNewViewMsg(nv)
	}
}

// StartViewChange stops current view and timeouts, and broadcasts a view change message to all
func (v *ViewChanger) StartViewChange() {
	v.NextView = v.CurrView + 1
	// TODO stop timeouts and submission of new requests
	msg := &protos.Message{
		Content: &protos.Message_ViewChange{
			ViewChange: &protos.ViewChange{
				NextView: v.NextView,
				Reason:   "", // TODO add reason
			},
		},
	}
	v.Comm.BroadcastConsensus(msg) // TODO periodically send msg
}

func (v *ViewChanger) processViewChangeMsg() {
	if uint64(len(v.viewChangeMsgs.voted)) == v.F+1 { // join view change
		v.StartViewChange()
	}
	if len(v.viewChangeMsgs.voted) >= v.Quorum-1 && v.NextView > v.CurrView { // send view data
		v.CurrView = v.NextView
		// TODO restart timeouts
		v.Leader = getLeaderID(v.CurrView, v.N, v.nodes)
		msg := v.prepareViewDataMsg()
		// TODO write to log
		if v.Leader == v.SelfID {
			v.HandleMessage(v.SelfID, msg)
		} else {
			v.Comm.SendConsensus(v.Leader, msg)
		}
		v.viewChangeMsgs.clear(v.N) // TODO make sure clear is in the right place
		v.viewDataMsgs.clear(v.N)   // clear because currView changed
	}
}

func (v *ViewChanger) prepareViewDataMsg() *protos.Message {
	vd := &protos.ViewData{
		NextView: v.CurrView,
		// TODO fill data
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
	if rvd.NextView != v.CurrView {
		v.Logger.Warnf("%d got viewData message %v from %d, but %d is in view %d", v.SelfID, rvd, sender, v.SelfID, v.CurrView)
		return false
	}
	if getLeaderID(rvd.NextView, v.N, v.nodes) != v.SelfID { // check if I am the next leader
		v.Logger.Warnf("%d got viewData message %v from %d, but %d is not the next leader", v.SelfID, rvd, sender, v.SelfID)
		return false
	}
	return true
}

func (v *ViewChanger) processViewDataMsg() {
	if len(v.viewDataMsgs.voted) >= v.Quorum { // need enough (quorum) data to continue
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

		if vd.NextView != v.CurrView {
			v.Logger.Warnf("%d is processing newView message %v, but nextView of viewData %v is %d, while the currView is %d", v.SelfID, msg, svd, vd.NextView, v.CurrView)
			continue
		}

		// TODO validate data

		valid++
	}
	if valid >= v.Quorum {
		// TODO handle data
		v.Controller.ViewChanged(v.CurrView, 0) // TODO change seq 0
	}
}
