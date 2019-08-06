// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math"
	"sort"

	"encoding/asn1"

	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

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

func MarshalOrPanic(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

func getLeaderID(view uint64, N uint64, nodes []uint64) uint64 {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i] < nodes[j]
	})
	return nodes[view%N]
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

type TBSPrepare struct {
	View   int64
	Seq    int64
	Digest string
}

func (tbsp TBSPrepare) ToBytes() []byte {
	bytes, err := asn1.Marshal(tbsp)
	if err != nil {
		panic(errors.Errorf("failed marshaling prepare %v: %v", tbsp, err))
	}
	return bytes
}
