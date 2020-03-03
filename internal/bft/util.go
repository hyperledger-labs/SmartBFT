// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

type proposalInfo struct {
	digest string
	view   uint64
	seq    uint64
}

func viewNumber(m *protos.Message) uint64 {
	if pp := m.GetPrePrepare(); pp != nil {
		return pp.GetView()
	}

	if prp := m.GetPrepare(); prp != nil {
		return prp.GetView()
	}

	if cmt := m.GetCommit(); cmt != nil {
		return cmt.GetView()
	}

	return math.MaxUint64
}

func proposalSequence(m *protos.Message) uint64 {
	if pp := m.GetPrePrepare(); pp != nil {
		return pp.Seq
	}

	if prp := m.GetPrepare(); prp != nil {
		return prp.Seq
	}

	if cmt := m.GetCommit(); cmt != nil {
		return cmt.Seq
	}

	return math.MaxUint64
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MarshalOrPanic marshals or panics when an error occurs
func MarshalOrPanic(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

func getLeaderID(view uint64, N uint64, nodes []uint64) uint64 {
	return nodes[view%N] // assuming this is sorted
}

type vote struct {
	*protos.Message
	sender uint64
}

type voteSet struct {
	validVote func(voter uint64, message *protos.Message) bool
	voted     map[uint64]struct{}
	votes     chan *vote
}

func (vs *voteSet) clear(n uint64) {
	// Drain the votes channel
	for len(vs.votes) > 0 {
		<-vs.votes
	}

	vs.voted = make(map[uint64]struct{}, n)
	vs.votes = make(chan *vote, n)
}

func (vs *voteSet) registerVote(voter uint64, message *protos.Message) {
	if !vs.validVote(voter, message) {
		return
	}

	_, hasVoted := vs.voted[voter]
	if hasVoted {
		// Received double vote
		return
	}

	vs.voted[voter] = struct{}{}
	vs.votes <- &vote{Message: message, sender: voter}
}

type incMsg struct {
	*protos.Message
	sender uint64
}

// computeQuorum calculates the quorums size Q, given a cluster size N.
//
// The calculation satisfies the following:
// Given a cluster size of N nodes, which tolerates f failures according to:
//    f = argmax ( N >= 3f+1 )
// Q is the size of the quorum such that:
//    any two subsets q1, q2 of size Q, intersect in at least f+1 nodes.
//
// Note that this is different from N-f (the number of correct nodes), when N=3f+3. That is, we have two extra nodes
// above the minimum required to tolerate f failures.
func computeQuorum(N uint64) (Q int, F int) {
	F = int((int(N) - 1) / 3)
	Q = int(math.Ceil((float64(N) + float64(F) + 1) / 2.0))
	return
}

// InFlightData records proposals that are in-flight,
// as well as their corresponding prepares.
type InFlightData struct {
	v atomic.Value
}

type inFlightProposalData struct {
	proposal *types.Proposal
	prepared bool
}

// InFlightProposal returns an in-flight proposal or nil if there is no such.
func (ifp *InFlightData) InFlightProposal() *types.Proposal {
	fetched := ifp.v.Load()
	if fetched == nil {
		return nil
	}

	data := fetched.(inFlightProposalData)
	return data.proposal
}

// IsInFlightPrepared returns true if the in-flight proposal is prepared.
func (ifp *InFlightData) IsInFlightPrepared() bool {
	fetched := ifp.v.Load()
	if fetched == nil {
		return false
	}
	data := fetched.(inFlightProposalData)
	return data.prepared
}

// StoreProposal stores an in-flight proposal.
func (ifp *InFlightData) StoreProposal(prop types.Proposal) {
	p := prop
	ifp.v.Store(inFlightProposalData{proposal: &p})
}

// StorePrepares stores alongside the already stored in-flight proposal that it is prepared.
func (ifp *InFlightData) StorePrepares(view, seq uint64) {
	prop := ifp.InFlightProposal()
	if prop == nil {
		panic("stored prepares but proposal is not initialized")
	}
	p := prop
	ifp.v.Store(inFlightProposalData{proposal: p, prepared: true})
}

// ProposalMaker implements ProposerBuilder
type ProposalMaker struct {
	N                  uint64
	SelfID             uint64
	Decider            Decider
	FailureDetector    FailureDetector
	Sync               Synchronizer
	Logger             api.Logger
	Comm               Comm
	Verifier           api.Verifier
	Signer             api.Signer
	State              State
	InMsqQSize         int
	ViewSequences      *atomic.Value
	restoreOnceFromWAL sync.Once
}

// NewProposer returns a new view
func (pm *ProposalMaker) NewProposer(leader, proposalSequence, viewNum uint64, quorumSize int) Proposer {
	view := &View{
		N:                pm.N,
		LeaderID:         leader,
		SelfID:           pm.SelfID,
		Quorum:           quorumSize,
		Number:           viewNum,
		Decider:          pm.Decider,
		FailureDetector:  pm.FailureDetector,
		Sync:             pm.Sync,
		Logger:           pm.Logger,
		Comm:             pm.Comm,
		Verifier:         pm.Verifier,
		Signer:           pm.Signer,
		ProposalSequence: proposalSequence,
		State:            pm.State,
		InMsgQSize:       pm.InMsqQSize,
		ViewSequences:    pm.ViewSequences,
	}

	view.ViewSequences.Store(ViewSequence{
		ViewActive:  true,
		ProposalSeq: proposalSequence,
	})

	pm.restoreOnceFromWAL.Do(func() {
		err := pm.State.Restore(view)
		if err != nil {
			pm.Logger.Panicf("Failed restoring view from WAL: %v", err)
		}
	})

	if proposalSequence > view.ProposalSequence {
		view.ProposalSequence = proposalSequence
	}

	if viewNum > view.Number {
		view.Number = viewNum
	}

	return view
}

// ViewSequence indicates if a view is currently active and its current proposal sequence
type ViewSequence struct {
	ViewActive  bool
	ProposalSeq uint64
}

// MsgToString converts a given message to a printable string
func MsgToString(m *protos.Message) string {
	if m == nil {
		return "empty message"
	}
	switch m.GetContent().(type) {
	case *protos.Message_PrePrepare:
		return prePrepareToString(m.GetPrePrepare())
	case *protos.Message_NewView:
		return newViewToString(m.GetNewView())
	case *protos.Message_ViewData:
		return signedViewDataToString(m.GetViewData())
	case *protos.Message_HeartBeat:
		return heartBeatToString(m.GetHeartBeat())
	case *protos.Message_HeartBeatResponse:
		return heartBeatResponseToString(m.GetHeartBeatResponse())
	default:
		return m.String()
	}
}

func prePrepareToString(prp *protos.PrePrepare) string {
	if prp == nil {
		return "<empty PrePrepare>"
	}
	if prp.Proposal == nil {
		return fmt.Sprintf("<PrePrepare with view: %d, seq: %d, empty proposal>", prp.View, prp.Seq)
	}
	return fmt.Sprintf("<PrePrepare with view: %d, seq: %d, payload of %d bytes, header: %s>",
		prp.View, prp.Seq, len(prp.Proposal.Payload), base64.StdEncoding.EncodeToString(prp.Proposal.Header))
}

func newViewToString(nv *protos.NewView) string {
	if nv == nil || nv.SignedViewData == nil {
		return "<empty NewView>"
	}
	buff := bytes.Buffer{}
	buff.WriteString("< NewView with ")
	for i, svd := range nv.SignedViewData {
		buff.WriteString(signedViewDataToString(svd))
		if i == len(nv.SignedViewData)-1 {
			break
		}
		buff.WriteString(", ")
	}
	buff.WriteString(">")
	return buff.String()
}

func signedViewDataToString(svd *protos.SignedViewData) string {
	if svd == nil {
		return "empty ViewData"
	}
	vd := &protos.ViewData{}
	if err := proto.Unmarshal(svd.RawViewData, vd); err != nil {
		return fmt.Sprintf("<malformed viewdata from %d>", svd.Signer)
	}

	return fmt.Sprintf("<ViewData signed by %d with NextView: %d>",
		svd.Signer, vd.NextView)
}

func heartBeatToString(hb *protos.HeartBeat) string {
	if hb == nil {
		return "empty HeartBeat"
	}

	return fmt.Sprintf("<HeartBeat with view: %d, seq: %d", hb.View, hb.Seq)
}

func heartBeatResponseToString(hbr *protos.HeartBeatResponse) string {
	if hbr == nil {
		return "empty HeartBeatResponse"
	}

	return fmt.Sprintf("<HeartBeatResponse with view: %d", hbr.View)
}
