// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package network

import (
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/golang/protobuf/proto"
)

type labeledMessage struct {
	from    uint64
	message proto.Message
}

type Mesh struct {
	mutex       sync.Mutex
	inQueue     map[uint64]chan *labeledMessage
	sendTimeout time.Duration
	//TODO message loss/block matrix,
}

func NewMesh(sendTimeout time.Duration, nodeIDs ...uint64) *Mesh {
	m := &Mesh{
		inQueue:     make(map[uint64]chan *labeledMessage),
		sendTimeout: sendTimeout,
	}

	for _, id := range nodeIDs {
		m.inQueue[id] = make(chan *labeledMessage, 256)
	}

	return m
}

func (n *Mesh) MessageLoss(source, target uint64, rate float64) {
	//TODO
}

func (n *Mesh) NodeIDs() []uint64 {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	nodes := make([]uint64, 0, len(n.inQueue))
	for n, _ := range n.inQueue {
		nodes = append(nodes, n)
	}
	return nodes
}

// Heal restores the network to perfect conditions, i.e. no blocks, no message loss.
func (n *Mesh) Heal() {
	//TODO
}

func (n *Mesh) send(source, target uint64, msg proto.Message) error {
	n.mutex.Lock()
	targetQ, found := n.inQueue[target]
	// TODO check message loss / blocked connection, drop if applicable
	n.mutex.Unlock()

	if !found {
		return os.ErrNotExist
	}

	lm := &labeledMessage{from: source, message: msg}
	timeout := time.Tick(n.sendTimeout)
	for {
		select {
		case targetQ <- lm:
			return nil
		case <-timeout:
			return errors.New("send timeout")
		}
	}
}

func (n *Mesh) broadcast(source uint64, msg proto.Message, loop bool) error {
	var targets []uint64

	n.mutex.Lock()
	targets = make([]uint64, 0, len(n.inQueue))
	for target := range n.inQueue {
		if target == source && !loop {
			continue
		}
		targets = append(targets, target)
	}
	n.mutex.Unlock()

	for _, target := range targets {
		if err := n.send(source, target, msg); err != nil {
			return err
		}
	}

	return nil
}

func (n *Mesh) getInQ(target uint64) chan *labeledMessage {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	targetQ, found := n.inQueue[target]
	if !found {
		return nil
	}

	return targetQ
}
