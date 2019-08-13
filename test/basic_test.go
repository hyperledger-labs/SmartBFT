// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	n1 := newNode(1, network, t.Name())
	n2 := newNode(2, network, t.Name())
	n3 := newNode(3, network, t.Name())
	n4 := newNode(4, network, t.Name())

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

	n1 := newNode(1, network, t.Name())
	n2 := newNode(2, network, t.Name())
	n3 := newNode(3, network, t.Name())
	n4 := newNode(4, network, t.Name())

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

	n0 := newNode(0, network, t.Name())
	n1 := newNode(1, network, t.Name())
	n2 := newNode(2, network, t.Name())
	n3 := newNode(3, network, t.Name())

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

func TestLeaderForwarding(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	n0 := newNode(0, network, t.Name())
	n1 := newNode(1, network, t.Name())
	n2 := newNode(2, network, t.Name())
	n3 := newNode(3, network, t.Name())

	n0.Consensus.Start()
	n1.Consensus.Start()
	n2.Consensus.Start()
	n3.Consensus.Start()

	n1.Submit(Request{ID: "1", ClientID: "alice"})
	n2.Submit(Request{ID: "2", ClientID: "bob"})
	n3.Submit(Request{ID: "3", ClientID: "carol"})

	numBatchesCreated := countCommittedBatches(n0)

	committedBatches := make([][]AppRecord, 3)
	for nodeIndex, n := range []*App{n1, n2, n3} {
		committedBatches = append(committedBatches, make([]AppRecord, numBatchesCreated))
		for i := 0; i < numBatchesCreated; i++ {
			record := <-n.Delivered
			committedBatches[nodeIndex] = append(committedBatches[nodeIndex], *record)
		}
	}

	assert.Equal(t, committedBatches[0], committedBatches[1])
	assert.Equal(t, committedBatches[0], committedBatches[2])
}

func countCommittedBatches(n *App) int {
	var numBatchesCreated int
	for {
		select {
		case <-n.Delivered:
			numBatchesCreated++
		case <-time.After(time.Millisecond * 500):
			return numBatchesCreated
		}
	}
}
