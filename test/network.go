// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"math/rand"
	"sync"
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
	Stop()
}

type msgFrom struct {
	message proto.Message
	from    int
}

// Network is a map of ids and nodes
type Network map[uint64]*Node

// AddOrUpdateNode adds or updates a node in the network
func (n Network) AddOrUpdateNode(id uint64, h handler, app *App) {
	node, exists := n[id]
	if exists {
		node.h = h
		return
	}

	node = &Node{
		in:                  make(chan msgFrom, incBuffSize),
		h:                   h,
		shutdownChan:        make(chan struct{}),
		n:                   n,
		id:                  id,
		peerLossProbability: make(map[uint64]float32),
		peerMutatingFunc:    make(map[uint64]func(uint64, *smartbftprotos.Message)),
		app:                 app,
	}
	n[id] = node
	node.running.Add(1)
	node.createCommittedBatches(n)
}

// StartServe calls serve on all nodes in the network
func (n Network) StartServe() {
	for _, node := range n {
		go node.serve()
	}
}

// Shutdown stops all nodes in the network
func (n Network) Shutdown() {
	for _, node := range n {
		close(node.shutdownChan)
		node.running.Wait()
	}
	for _, node := range n {
		node.h.Stop()
	}
}

func (n Network) send(source, target uint64, msg proto.Message) {
	dstNode, found := n[target]

	if !found {
		panic("node doesn't exist")
	}

	srcNode, found := n[source]
	if !found {
		panic("node doesn't exist")
	}

	dstNode.probabilityLock.RLock()
	p := dstNode.lossProbability
	dstNode.probabilityLock.RUnlock()

	srcNode.probabilityLock.RLock()
	q := srcNode.lossProbability
	w := srcNode.peerLossProbability[target]
	srcNode.probabilityLock.RUnlock()

	r := rand.Float32()
	if r < p || r < q || r < w {
		return
	}

	select {
	case dstNode.in <- msgFrom{from: int(source), message: msg}:
	default:
		fmt.Println("Dropped msg from", source, "to", target, "due to overflow")
	}
}

// Node represents a node in a network
type Node struct {
	sync.RWMutex
	running             sync.WaitGroup
	id                  uint64
	n                   Network
	lossProbability     float32
	peerLossProbability map[uint64]float32
	probabilityLock     sync.RWMutex
	peerMutatingFunc    map[uint64]func(uint64, *smartbftprotos.Message)
	mutatingFuncLock    sync.RWMutex
	shutdownChan        chan struct{}
	in                  chan msgFrom
	h                   handler
	cb                  *committedBatches
	app                 *App
}

// SendConsensus sends a consensus related message to a target node
func (node *Node) SendConsensus(targetID uint64, m *smartbftprotos.Message) {
	node.mutatingFuncLock.RLock()
	mutatingFunc := node.peerMutatingFunc[targetID]
	msg := m
	if mutatingFunc != nil {
		msg = proto.Clone(m).(*smartbftprotos.Message)
		mutatingFunc(targetID, msg)
	}
	node.mutatingFuncLock.RUnlock()
	node.n.send(node.id, targetID, msg)
}

// SendTransaction sends a client's request to a target node
func (node *Node) SendTransaction(targetID uint64, request []byte) {
	node.n.send(node.id, targetID, &FwdMessage{Payload: request})
}

// Nodes returns the ids of all nodes in the network
func (node *Node) Nodes() []uint64 {
	var res []uint64
	for _, n := range node.n {
		res = append(res, n.id)
	}
	return res
}

func (node *Node) serve() {
	defer node.running.Done()
	for {
		select {
		case <-node.shutdownChan:
			return
		case m := <-node.in:
			node.RLock()
			handler := node.h
			node.RUnlock()
			switch msg := m.message.(type) {
			case *smartbftprotos.Message:
				handler.HandleMessage(uint64(m.from), msg)
			default:
				handler.HandleRequest(uint64(m.from), msg.(*FwdMessage).Payload)
			}
		}
	}
}

func (node *Node) createCommittedBatches(network Network) {
	for _, n := range network {
		if n.cb != nil {
			node.cb = n.cb
			return
		}
	}
	node.cb = &committedBatches{}
}
