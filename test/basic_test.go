// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	n1 := newNode(1, network)
	n2 := newNode(2, network)
	n3 := newNode(3, network)
	n4 := newNode(4, network)

	n1.Consensus.Start()
	n2.Consensus.Start()
	n3.Consensus.Start()
	n4.Consensus.Start()

	n1.Submit(Request{ID: "1", ClientID: "alice"})
	n1.Submit(Request{ID: "2", ClientID: "alice"})
	n1.Submit(Request{ID: "3", ClientID: "alice"})
	n1.Submit(Request{ID: "3", ClientID: "alice"})

	data1 := <-n1.Delivered
	data2 := <-n2.Delivered
	data3 := <-n3.Delivered
	data4 := <-n4.Delivered

	assert.Equal(t, data1, data2)
	assert.Equal(t, data3, data4)
	assert.Equal(t, data1, data4)
}

func TestRestartFollowers(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	n1 := newNode(1, network)
	n2 := newNode(2, network)
	n3 := newNode(3, network)
	n4 := newNode(4, network)

	n1.Consensus.Start()
	n2.Consensus.Start()
	n3.Consensus.Start()
	n4.Consensus.Start()

	n1.Submit(Request{ID: "1", ClientID: "alice"})

	n2.Restart()

	data1 := <-n1.Delivered
	data2 := <-n2.Delivered
	data3 := <-n3.Delivered
	data4 := <-n4.Delivered

	assert.Equal(t, data1, data2)
	assert.Equal(t, data3, data4)
	assert.Equal(t, data1, data4)

	n3.Restart()
	n1.Submit(Request{ID: "2", ClientID: "alice"})
	n4.Restart()

	data1 = <-n1.Delivered
	data2 = <-n2.Delivered
	data3 = <-n3.Delivered
	data4 = <-n4.Delivered
	assert.Equal(t, data1, data2)
	assert.Equal(t, data3, data4)
	assert.Equal(t, data1, data4)
}

func TestLeaderInPartition(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	n0 := newNode(0, network)
	n1 := newNode(1, network)
	n2 := newNode(2, network)
	n3 := newNode(3, network)

	n0.Consensus.Start()
	n1.Consensus.Start()
	n2.Consensus.Start()
	n3.Consensus.Start()

	n0.Disconnect() // leader in partition

	n1.Submit(Request{ID: "1", ClientID: "alice"}) // submit to other (f+1) nodes
	n2.Submit(Request{ID: "1", ClientID: "alice"})

	data1 := <-n1.Delivered
	data2 := <-n2.Delivered
	data3 := <-n3.Delivered
	assert.Equal(t, data1, data2)
	assert.Equal(t, data2, data3)
}
