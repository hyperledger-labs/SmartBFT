// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	incBuffSize = 1000
)

type handler interface {
	HandleMessage(sender uint64, m *smartbftprotos.Message)
	HandleRequest(sender uint64, req []byte)
}

type msgFrom struct {
	message proto.Message
	from    int
}

type Network map[uint64]*Node

func (n Network) AddNode(id uint64, h handler) {
	n[id] = &Node{
		in:           make(chan msgFrom, incBuffSize),
		h:            h,
		shutdownChan: make(chan struct{}),
		n:            n,
		id:           uint64(id),
	}
	go n[id].serve()
}

func (n Network) send(source, target uint64, msg proto.Message) {
	node, found := n[target]

	if !found {
		panic("node doesn't exist")
	}

	if rand.Float32() < node.lossProbability {
		return
	}

	select {
	case node.in <- msgFrom{from: int(source), message: msg}:
	default:
		fmt.Println("Dropped msg from", source, "to", target, "due to overflow")
		// drop
	}
}

type Node struct {
	id              uint64
	n               Network
	lossProbability float32
	shutdownChan    chan struct{}
	in              chan msgFrom
	h               handler
}

func (node *Node) SendConsensus(targetID uint64, m *smartbftprotos.Message) {
	node.n.send(node.id, targetID, m)
}

func (node *Node) SendTransaction(targetID uint64, request []byte) {
	node.n.send(node.id, targetID, &FwdMessage{Payload: request})
}

func (node *Node) Nodes() []uint64 {
	var res []uint64
	for _, n := range node.n {
		res = append(res, n.id)
	}
	return res
}

func (node *Node) serve() {
	for {
		select {
		case <-node.shutdownChan:
			return
		case m := <-node.in:
			switch msg := m.message.(type) {
			case *smartbftprotos.Message:
				node.h.HandleMessage(uint64(m.from), msg)
			default:
				node.h.HandleRequest(uint64(m.from), msg.(*FwdMessage).Payload)
			}

		}
	}
}

func (node *Node) stop() {
	close(node.shutdownChan)
}
