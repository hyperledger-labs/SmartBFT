// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package network

import (
	"github.com/golang/protobuf/proto"
)

type Endpoint struct {
	network   *Mesh
	id        uint64
	onMessage func(from uint64, msg proto.Message)

	stopChan chan interface{}
	doneChan chan interface{}
}

func NewEndpoint(
	network *Mesh,
	id uint64,
	msgHandler func(from uint64, msg proto.Message),
) *Endpoint {
	q := network.getInQ(id)
	if q == nil {
		return nil
	}

	e := &Endpoint{
		network:   network,
		id:        id,
		onMessage: msgHandler,
		stopChan:  make(chan interface{}),
		doneChan:  make(chan interface{}),
	}
	return e
}

func (e *Endpoint) Send(target uint64, msg proto.Message) error {
	return e.network.send(e.id, target, msg)
}

func (e *Endpoint) Broadcast(msg proto.Message, loop bool) error {
	return e.network.broadcast(e.id, msg, loop)
}

func (e *Endpoint) Start() {
	go e.receive()
}

func (e *Endpoint) receive() {
	defer close(e.doneChan)

	inQ := e.network.getInQ(e.id)

	for {
		select {
		case <-e.stopChan:
			return
		case lm := <-inQ:
			e.onMessage(lm.from, lm.message)
		}
	}
}

func (e *Endpoint) Stop() {
	select {
	case <-e.stopChan:
		// already stopped
		return
	default:
		close(e.stopChan)
		<-e.doneChan
	}
}

// SetReceiveLimit limits the receive rate.
// rate: maximal number of message per second
// rate<0: no limit
func (e *Endpoint) SetReceiveLimit(rate float64) {
	//TODO
}
