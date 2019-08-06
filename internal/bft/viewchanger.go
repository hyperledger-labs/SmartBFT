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
	viewChangeMsgs *voteSet             // keep msg only if view number matches
	viewDataMsgs   map[uint64]*voteSet  // keep msg if I am the leader of that view
	newViewDataMsg chan struct{}        // inform a new viewData msg is available
	newViewMsg     chan *protos.NewView // keep msg only if view number matches and from the leader
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

	// view change
	acceptViewChange := func(_ uint64, message *protos.Message) bool {
		return message.GetViewChange() != nil
	}
	v.viewChangeMsgs = &voteSet{
		validVote: acceptViewChange,
	}
	v.viewChangeMsgs.clear(v.N)

	// view data
	v.viewDataMsgs = make(map[uint64]*voteSet)
	v.newViewDataMsg = make(chan struct{}, 1)

	// new view
	v.newViewMsg = make(chan *protos.NewView, 1)

	go func() {
		defer v.vcDone.Done()
		v.run()
	}()

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
		case vote := <-v.viewChangeMsgs.votes:
			v.processViewChangeMsg(vote)
		case <-v.newViewDataMsg:
			v.processViewDataMsg()
		case nv := <-v.newViewMsg:
			v.processNewViewMsg(nv)

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
		return
	}

	//viewData message
	if vd := m.GetViewData(); vd != nil {
		if vd.Signer != sender {
			v.Logger.Warnf("%d got viewData message %v from %d, but signer %d is not the sender %d", v.SelfID, m, sender, vd.Signer, sender)
			return
		}
		if err := v.Verifier.VerifySignature(types.Signature{Id: vd.Signer, Value: vd.Signature, Msg: vd.RawViewData}); err != nil {
			v.Logger.Warnf("%d got viewData message %v from %d, but signature is invalid, err: ", v.SelfID, m, sender, err)
			return
		}
		rvd := &protos.ViewData{}
		if err := proto.Unmarshal(vd.RawViewData, rvd); err != nil {
			v.Logger.Errorf("%d was unable to unmarshal viewData message from %d, error: ", v.SelfID, sender, err)
			return
		}
		if rvd.NextView < v.CurrView { // check if this data is not for an old view number
			v.Logger.Warnf("%d got viewData message %v from %d, but %d is in view %d", v.SelfID, rvd, sender, v.SelfID, v.CurrView)
			return
		}
		if getLeaderID(rvd.NextView, v.N, v.nodes) != v.SelfID { // check if I am the next leader
			v.Logger.Warnf("%d got viewData message %v from %d, but %d is not the next leader", v.SelfID, rvd, sender, v.SelfID)
			return
		}
		// TODO check data validity
		v.registerViewDataVote(sender, m)
		// notify a new view data is registered
		select {
		case v.newViewDataMsg <- struct{}{}:
		default: // notification already pending
		}
		return
	}

	// newView message
	if nv := m.GetNewView(); nv != nil {
		if sender != v.Leader {
			v.Logger.Warnf("%d got newView message %v from %d, expected sender to be %d the next leader", v.SelfID, m, sender, v.Leader)
			return
		}
		// TODO check view number here?
		v.newViewMsg <- nv
	}
}

func (v *ViewChanger) registerViewDataVote(sender uint64, m *protos.Message) {
	vd := m.GetViewData()
	rvd := &protos.ViewData{}
	if err := proto.Unmarshal(vd.RawViewData, rvd); err != nil {
		v.Logger.Errorf("%d was unable to unmarshal viewData message from %d, error: ", v.SelfID, sender, err)
		return
	}
	view := rvd.NextView
	votes, hasView := v.viewDataMsgs[view]
	if !hasView {
		acceptViewData := func(sender uint64, message *protos.Message) bool {
			return true
		}
		votes = &voteSet{
			validVote: acceptViewData,
		}
		votes.clear(v.N)
		v.viewDataMsgs[view] = votes
	}
	votes.registerVote(sender, m)
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

func (v *ViewChanger) processViewChangeMsg(vote *vote) {
	if uint64(len(v.viewChangeMsgs.voted)) > v.F { // join view change
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
		v.viewChangeMsgs.clear(v.N) // TODO make sure this is the right place to clean
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

func (v *ViewChanger) processViewDataMsg() {
	for viewNum, votes := range v.viewDataMsgs {
		if viewNum < v.CurrView { // clean old votes
			delete(v.viewDataMsgs, viewNum)
			continue
		}
		num := len(votes.voted)
		if num >= v.Quorum { // need enough (quorum) data to continue
			// TODO handle data
			repeated := make([]*protos.SignedViewData, 0)
			n := 0
			for n < num {
				v := <-votes.votes
				repeated = append(repeated, v.GetViewData())
				n++
			}
			msg := &protos.Message{
				Content: &protos.Message_NewView{
					NewView: &protos.NewView{
						SignedViewData: repeated,
					},
				},
			}
			v.Comm.BroadcastConsensus(msg)
			v.HandleMessage(v.SelfID, msg) // also send to myself

			delete(v.viewDataMsgs, viewNum) // don't need them anymore
			break
		}
	}
}

func (v *ViewChanger) processNewViewMsg(msg *protos.NewView) {
	repeated := msg.GetSignedViewData()
	nodesMap := make(map[uint64]struct{}, v.N)
	valid := 0
	for _, svd := range repeated {
		if _, exist := nodesMap[svd.Signer]; exist {
			continue // seen data from this node already
		}
		nodesMap[svd.Signer] = struct{}{}

		if err := v.Verifier.VerifySignature(types.Signature{Id: svd.Signer, Value: svd.Signature, Msg: svd.RawViewData}); err != nil {
			continue // invalid signature
		}

		vd := &protos.ViewData{}
		if err := proto.Unmarshal(svd.RawViewData, vd); err != nil {
			v.Logger.Errorf("%d was unable to unmarshal a viewData from the newView message, error: ", v.SelfID, err)
			continue
		}

		if vd.NextView != v.CurrView {
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
