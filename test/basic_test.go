// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestBasic(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	for i := 1; i < 5; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestRestartFollowers(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	startViewWG := sync.WaitGroup{}
	startViewWG.Add(1)
	baseLogger := nodes[2].logger.Desugar()
	nodes[2].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Starting view with number 0, sequence 2") {
			startViewWG.Done()
		}
		return nil
	})).Sugar()
	nodes[2].Setup()

	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	data := make([]*AppRecord, 0)
	d0 := <-nodes[0].Delivered
	d2 := <-nodes[2].Delivered
	d3 := <-nodes[3].Delivered

	nodes[1].Restart()
	d1 := <-nodes[1].Delivered

	data = append(data, d0, d1, d2, d3)
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[2].Restart()
	startViewWG.Wait()

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	d0 = <-nodes[0].Delivered
	d1 = <-nodes[1].Delivered
	d2 = <-nodes[2].Delivered

	nodes[3].Restart()
	d3 = <-nodes[3].Delivered

	data = append(data, d0, d1, d2, d3)
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderInPartition(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	assert.Equal(t, uint64(0), nodes[2].Consensus.GetLeaderID())
	startNodes(nodes, &network)
	assert.Equal(t, uint64(1), nodes[2].Consensus.GetLeaderID())

	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
	assert.Equal(t, uint64(2), nodes[2].Consensus.GetLeaderID())
}

func TestAfterDecisionLeaderInPartition(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "3", ClientID: "alice"}) // submit to other nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "4", ClientID: "alice"}) // submit to other nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderInPartitionWithHealing(t *testing.T) {
	t.Parallel()

	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition
	t.Log("Disconnected n0")

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "3", ClientID: "alice"}) // submit to other nodes
	}

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	assert.Len(t, nodes[0].Delivered, 0) // n0 did not receive it

	nodes[0].Connect() // partition heals, leader should eventually sync, become a follower, and deliver
	t.Log("Connected n0")

	data0 := <-nodes[0].Delivered
	assert.Equal(t, data[0], data0)
}

func TestMultiLeadersPartition(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}
	assert.Equal(t, uint64(0), nodes[0].Consensus.GetLeaderID())
	startNodes(nodes, &network)
	assert.Equal(t, uint64(1), nodes[0].Consensus.GetLeaderID())

	nodes[0].Disconnect() // leader in partition
	nodes[1].Disconnect() // next leader in partition

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	done := make(chan struct{})
	defer close(done)

	var counter uint64
	accelerateTime(nodes, done, false, true, &counter)

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-3; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := 2; i < numberOfNodes; i++ {
		assert.Equal(t, uint64(3), nodes[i].Consensus.GetLeaderID())
	}

}

func TestHeartbeatTimeoutCausesViewChange(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.Setup()
	}

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(numberOfNodes - 1)
	for _, n := range nodes {
		baseLogger := n.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		n.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "ViewChanged") {
				viewChangeWG.Done()
			}
			return nil
		})).Sugar()
	}

	startNodes(nodes, &network)

	nodes[0].Disconnect() // leader in partition

	// Accelerate the time until a view change because of heartbeat timeout
	var counter uint64
	accelerateTime(nodes, done, true, false, &counter)

	viewChangeWG.Wait()
	close(done)

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestMultiViewChangeWithNoRequestsTimeout(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	// wait for the new leader to finish the view change before submitting
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(5)
	for _, n := range network {
		baseLogger := n.app.Consensus.Logger.(*zap.SugaredLogger).Desugar()
		n.app.Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "ViewChanged") {
				viewChangeWG.Done()
			}
			return nil
		})).Sugar()
	}

	startNodes(nodes, &network)

	nodes[0].Disconnect() // leader in partition
	nodes[1].Disconnect() // next leader in partition

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)
	viewChangeWG.Wait()
	close(done)

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-3; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestCatchingUpWithViewChange(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	done := make(chan struct{})
	viewChangeFinishWG := sync.WaitGroup{}
	viewChangeFinishWG.Add(1)
	viewChangeFinishOnce := sync.Once{}
	baseLogger := nodes[3].logger.Desugar()
	nodes[3].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeFinishOnce.Do(func() {
				viewChangeFinishWG.Done()
			})
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, &network)

	nodes[3].Disconnect() // will need to catch up

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes-1; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[3].Connect()
	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "2", ClientID: "alice"}) // submit to other nodes
	}

	var counter uint64
	accelerateTime(nodes, done, false, true, &counter)

	data3 := <-nodes[3].Delivered // from catch up
	assert.Equal(t, data[0], data3)

	viewChangeFinishWG.Wait()
	close(done)

	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderCatchingUpAfterViewChange(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to leader

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Disconnect() // leader in partition

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "2", ClientID: "alice"}) // submit to other nodes
	}
	data = make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Connect() // old leader woke up

	// We create new batches until it catches up
	for reqID := 3; reqID < 100; reqID++ {
		nodes[1].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		nodes[2].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		<-nodes[1].Delivered // Wait for new leader to commit
		<-nodes[2].Delivered // Wait for follower to commit
		caughtUp := waitForCatchup(reqID, nodes[0].Delivered)
		if caughtUp {
			return
		}
	}
	t.Fatalf("Didn't catch up")
}

func TestRestartAfterViewChangeAndRestoreNewView(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.Setup()
	}

	// wait for a view change to occur
	done := make(chan struct{})
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(2)
	baseLogger1 := nodes[1].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[1].Consensus.Logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()
	baseLogger3 := nodes[3].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[3].Consensus.Logger = baseLogger3.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()

	startNodes(nodes, &network)

	nodes[0].Disconnect()

	var counter uint64
	accelerateTime(nodes, done, true, false, &counter)

	viewChangeWG.Wait()
	close(done)

	// restart new leader and a follower, they will restore from new view
	nodes[1].Restart()
	nodes[3].Restart()

	for i := 1; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"}) // submit to other nodes
	}
	data := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestRestoringViewChange(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	done := make(chan struct{})
	viewChangeFinishWG := sync.WaitGroup{}
	viewChangeFinishWG.Add(1)
	viewChangeFinishOnce := sync.Once{}
	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(1)
	viewChangeOnce := sync.Once{}
	baseLogger := nodes[6].logger.Desugar()
	nodes[6].logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Node 7 sent view data msg") {
			viewChangeOnce.Do(func() {
				viewChangeWG.Done()
			})
		}
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeFinishOnce.Do(func() {
				viewChangeFinishWG.Done()
			})
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, &network)

	nodes[0].Disconnect() // leader in partition
	nodes[1].Disconnect() // next leader in partition

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	viewChangeWG.Wait()
	nodes[6].Disconnect()
	nodes[6].Restart()
	nodes[6].Connect()

	viewChangeFinishWG.Wait()
	close(done)

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-3; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderForwarding(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	nodes[1].Submit(Request{ID: "1", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "2", ClientID: "bob"})
	nodes[3].Submit(Request{ID: "3", ClientID: "carol"})

	numBatchesCreated := countCommittedBatches(nodes[0])

	committedBatches := make([][]AppRecord, 3)
	for nodeIndex, n := range []*App{nodes[1], nodes[2], nodes[3]} {
		committedBatches = append(committedBatches, make([]AppRecord, numBatchesCreated))
		for i := 0; i < numBatchesCreated; i++ {
			record := <-n.Delivered
			committedBatches[nodeIndex] = append(committedBatches[nodeIndex], *record)
		}
	}

	assert.Equal(t, committedBatches[0], committedBatches[1])
	assert.Equal(t, committedBatches[0], committedBatches[2])
}

func TestLeaderExclusion(t *testing.T) {
	// Scenario: The leader doesn't send messages to n3,
	// but it should detect this and sync.
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	startNodes(nodes, &network)

	nodes[0].DisconnectFrom(3)

	// We create new batches until the disconnected node catches up the quorum.
	for reqID := 1; reqID < 100; reqID++ {
		nodes[1].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		<-nodes[1].Delivered // Wait for follower to commit
		caughtUp := waitForCatchup(reqID, nodes[3].Delivered)
		if caughtUp {
			return
		}
	}
	t.Fatalf("Didn't catch up")
}

func TestCatchingUpWithSyncAssisted(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	nodes[3].Disconnect() // will need to catch up

	for i := 1; i <= 10; i++ {
		for j := 0; j <= 2; j++ {
			nodes[j].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
		}
		for j := 0; j <= 2; j++ {
			<-nodes[j].Delivered
		}
	}

	nodes[3].Connect()

	// We create new batches until it catches up the quorum.
	for reqID := 11; reqID < 100; reqID++ {
		for j := 0; j <= 2; j++ {
			nodes[j].Submit(Request{ID: fmt.Sprintf("%d", reqID), ClientID: "alice"})
		}
		for j := 0; j <= 2; j++ {
			<-nodes[j].Delivered
		}
		caughtUp := waitForCatchup(reqID, nodes[3].Delivered)
		if caughtUp {
			return
		}
	}
	t.Fatalf("Didn't catch up")
}

func TestCatchingUpWithSyncAutonomous(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 3)
		nodes = append(nodes, n)
	}

	var detectedSequenceGap uint32

	baseLogger := nodes[3].Consensus.Logger.(*zap.SugaredLogger).Desugar()
	nodes[3].Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Leader's sequence is 10 and ours is 1") {
			atomic.StoreUint32(&detectedSequenceGap, 1)
		}
		return nil
	})).Sugar()

	start := time.Now()
	nodes[0].heartbeatTime = make(chan time.Time, 1)
	nodes[0].heartbeatTime <- start
	nodes[3].heartbeatTime = make(chan time.Time, 1)
	nodes[3].heartbeatTime <- start
	nodes[3].viewChangeTime = make(chan time.Time, 1)
	nodes[3].viewChangeTime <- start
	nodes[0].Setup()
	nodes[3].Setup()

	startNodes(nodes, &network)

	nodes[3].Disconnect() // will need to catch up

	for i := 1; i <= 5; i++ {
		for j := 0; j <= 2; j++ {
			nodes[j].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
		}
		for j := 0; j <= 2; j++ {
			<-nodes[j].Delivered
		}
	}

	nodes[3].Connect()

	done := make(chan struct{})
	// Accelerate the time for n3 so it will suspect the leader and view change.
	go func() {
		var i int
		for {
			i++
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 100):
				nodes[0].heartbeatTime <- time.Now().Add(time.Second * time.Duration(10*i))
				nodes[3].heartbeatTime <- time.Now().Add(time.Second * time.Duration(10*i))
				nodes[3].viewChangeTime <- time.Now().Add(time.Minute * time.Duration(10*i))
			}
		}
	}()

	for i := 1; i <= 5; i++ {
		select {
		case <-nodes[3].Delivered:
		case <-time.After(time.Second * 10):
			t.Fatalf("Didn't catch up within a timely period")
		}
	}

	close(done)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&detectedSequenceGap))
}

func TestFollowerStateTransfer(t *testing.T) {
	// Scenario: the leader (n0) is disconnected and so there is a view change
	// a follower (n6) is also disconnected and misses the view change
	// after the follower reconnects and gets a view change timeout is calls sync
	// where it collects state transfer requests and sees that there was a view change

	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	syncedWG := sync.WaitGroup{}
	syncedWG.Add(1)
	baseLogger6 := nodes[6].logger.Desugar()
	nodes[6].logger = baseLogger6.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "The collected state") {
			syncedWG.Done()
		}
		return nil
	})).Sugar()

	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(1)
	baseLogger1 := nodes[1].logger.Desugar()
	nodes[1].logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, &network)

	nodes[0].Disconnect() // leader in partition
	nodes[6].Disconnect() // follower in partition

	// Accelerate the time until a view change
	done := make(chan struct{})

	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	viewChangeWG.Wait()
	nodes[6].Connect()
	syncedWG.Wait()
	close(done)

	for i := 2; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "1", ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-3; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestLeaderModifiesPreprepare(t *testing.T) {
	for _, test := range []struct {
		description  string
		mutatingFunc func(target uint64, m *smartbftprotos.Message)
	}{
		{
			description: "wrong view",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().View = 1
			},
		},
		{
			description: "conflicting proposals",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				incReq := Request{ID: fmt.Sprintf("%d", target), ClientID: "alice"}
				incData := batch{
					Requests: [][]byte{incReq.ToBytes()},
				}
				m.GetPrePrepare().Proposal.Payload = incData.toBytes()
			},
		},
		{
			description: "next sequence",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().Seq = 2
			},
		},
		{
			description: "wrong verification sequence",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().Proposal.VerificationSequence = 3
			},
		},
		{
			description: "wrong number of decisions",
			mutatingFunc: func(target uint64, m *smartbftprotos.Message) {
				if m.GetPrePrepare() == nil {
					return
				}
				m.GetPrePrepare().Proposal.Metadata = bft.MarshalOrPanic(&smartbftprotos.ViewMetadata{
					DecisionsInView: 1, // instead of 0
					LatestSequence:  1,
					ViewId:          0,
				})
			},
		},
	} {
		test := test
		t.Run(test.description, func(t *testing.T) {
			t.Parallel()
			network := make(Network)
			defer network.Shutdown()

			testDir, err := ioutil.TempDir("", test.description)
			assert.NoErrorf(t, err, "generate temporary test dir")
			defer os.RemoveAll(testDir)

			numberOfNodes := 4
			nodes := make([]*App, 0)
			for i := 1; i <= numberOfNodes; i++ {
				n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
				nodes = append(nodes, n)
			}

			once := sync.Once{}
			viewChangeWG := sync.WaitGroup{}
			viewChangeWG.Add(1)
			baseLogger1 := nodes[1].logger.Desugar()
			nodes[1].logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				if strings.Contains(entry.Message, "ViewChanged") {
					once.Do(func() {
						viewChangeWG.Done()
					})
				}
				return nil
			})).Sugar()
			nodes[1].Setup()

			startNodes(nodes, &network)

			for i := 2; i <= numberOfNodes; i++ {
				nodes[0].MutateSend(uint64(i), test.mutatingFunc)
			}

			for i := 0; i < numberOfNodes; i++ {
				nodes[i].Submit(Request{ID: fmt.Sprintf("%d", 1), ClientID: "alice"})
			}

			viewChangeWG.Wait()

			data := make([]*AppRecord, 0)
			for i := 0; i < numberOfNodes; i++ {
				d := <-nodes[i].Delivered
				data = append(data, d)
			}
			for i := 0; i < numberOfNodes-1; i++ {
				assert.Equal(t, data[i], data[i+1])
			}

			md := &smartbftprotos.ViewMetadata{}
			if err := proto.Unmarshal(data[0].Metadata, md); err != nil {
				assert.NoError(t, err)
			}
			assert.Equal(t, uint64(1), md.LatestSequence)

			for i := 2; i <= numberOfNodes; i++ {
				nodes[0].ClearMutateSend(uint64(i))
			}
		})
	}
}

func TestGradualStart(t *testing.T) {
	// Scenario: initially the network has only one node
	// a transaction is submitted and committed with that node
	// then another node is introduced, with a reset to the original node
	// and another transaction is passed
	// lastly a third node is added and another transaction is submitted
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	// start with only one node
	n0 := newNode(uint64(1), network, t.Name(), testDir, true, 1)

	if err := n0.Consensus.Start(); err != nil {
		n0.logger.Panicf("Consensus returned an error : %v", err)
	}
	network.StartServe()

	n0.Submit(Request{ID: fmt.Sprintf("%d", 1), ClientID: "alice"})

	d0 := <-n0.Delivered
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d0.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(0), md.ViewId)
	assert.Equal(t, uint64(1), md.LatestSequence)
	first := d0

	network.StopServe()

	// add a second node
	n1 := newNode(uint64(2), network, t.Name(), testDir, true, 1)
	atomic.AddUint64(&n1.verificationSeq, 1)

	if err := n1.Consensus.Start(); err != nil {
		n1.logger.Panicf("Consensus returned an error : %v", err)
	}

	atomic.AddUint64(&n0.verificationSeq, 1)
	n0.Restart()

	network.StartServe()

	d1 := <-n1.Delivered
	assert.Equal(t, d0, d1)

	n0.Submit(Request{ID: fmt.Sprintf("%d", 2), ClientID: "alice"})

	d0 = <-n0.Delivered
	md = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d0.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(0), md.ViewId)
	assert.Equal(t, uint64(2), md.LatestSequence)
	second := d0

	d1 = <-n1.Delivered
	assert.Equal(t, d0, d1)

	network.StopServe()

	// add a third node
	n2 := newNode(uint64(3), network, t.Name(), testDir, true, 1)
	atomic.AddUint64(&n2.verificationSeq, 2)

	if err := n2.Consensus.Start(); err != nil {
		n2.logger.Panicf("Consensus returned an error : %v", err)
	}

	atomic.AddUint64(&n1.verificationSeq, 1)
	atomic.AddUint64(&n0.verificationSeq, 1)
	n0.Restart()
	n1.Restart()

	network.StartServe()

	d2 := <-n2.Delivered
	assert.Equal(t, first, d2)
	d2 = <-n2.Delivered
	assert.Equal(t, second, d2)

	n0.Submit(Request{ID: fmt.Sprintf("%d", 3), ClientID: "alice"})

	d0 = <-n0.Delivered
	md = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d0.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(0), md.ViewId)
	assert.Equal(t, uint64(3), md.LatestSequence)

	d1 = <-n1.Delivered
	assert.Equal(t, d0, d1)
	d2 = <-n2.Delivered
	assert.Equal(t, d0, d2)

	n0.Disconnect() // disconnect the leader

	n1.Submit(Request{ID: fmt.Sprintf("%d", 4), ClientID: "alice"})
	n2.Submit(Request{ID: fmt.Sprintf("%d", 4), ClientID: "alice"})

	d1 = <-n1.Delivered
	md = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(d1.Metadata, md); err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(1), md.ViewId) // view was changed
	assert.Equal(t, uint64(4), md.LatestSequence)

	d2 = <-n2.Delivered
	assert.Equal(t, d1, d2)
}

func TestReconfigAndViewChange(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 2; i <= numberOfNodes+1; i++ { // add 4 nodes with ids starting at 2
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	startNodes(nodes, &network)

	for i := 1; i < 5; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
	}

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := 0; i < numberOfNodes; i++ {
		nodes[i].Consensus.Stop() // stop all nodes
	}

	newNode := newNode(1, network, t.Name(), testDir, false, 0) // add node with id 1, should be the leader
	nodes = append(nodes, newNode)
	startNodes(nodes[4:], &network)

	for i := 0; i < numberOfNodes; i++ {
		nodes[i].Restart() // restart all stopped nodes
	}

	d := <-nodes[4].Delivered
	assert.Equal(t, d, data[0]) // make sure the new added node (leader) is synced

	nodes[4].Disconnect() // leader in partition

	for i := 0; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "10", ClientID: "alice"}) // submit while leader in partition will cause a view change
	}

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

}

func TestRotateAndViewChange(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
		nodes = append(nodes, n)
	}

	start := time.Now()
	for _, n := range nodes {
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
	}

	syncedWG := sync.WaitGroup{}
	syncedWG.Add(1)
	baseLogger3 := nodes[3].logger.Desugar()
	nodes[3].logger = baseLogger3.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "The collected state") {
			syncedWG.Done()
		}
		return nil
	})).Sugar()

	viewChangeWG := sync.WaitGroup{}
	viewChangeWG.Add(1)
	baseLogger1 := nodes[1].logger.Desugar()
	nodes[1].logger = baseLogger1.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "ViewChanged") {
			viewChangeWG.Done()
		}
		return nil
	})).Sugar()

	for _, n := range nodes {
		n.Setup()
	}

	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"}) // submit to first leader

	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[1].Submit(Request{ID: "2", ClientID: "alice"}) // submit to second leader

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[2].Submit(Request{ID: "3", ClientID: "alice"}) // submit to third leader

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[3].Disconnect() // fourth leader in partition

	// Accelerate the time until a view change
	done := make(chan struct{})
	var counter uint64
	accelerateTime(nodes, done, true, true, &counter)

	viewChangeWG.Wait()
	nodes[3].Connect()
	syncedWG.Wait()
	close(done)

	nodes[1].Submit(Request{ID: "4", ClientID: "alice"}) // submit to current leader

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

}

func TestMigrateToBlacklistAndBackAgain(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	var nodes []*App
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}

	var leaderRotationDisabled uint64
	var boundCommitSignatures uint64

	setupLogger := func(node *App) {
		node.logger = node.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Bound 3 commit signatures to proposal") {
				atomic.AddUint64(&boundCommitSignatures, 1)
			}
			if strings.Contains(entry.Message, "Leader rotation is disabled, will not bind signatures to proposals") {
				atomic.AddUint64(&leaderRotationDisabled, 1)
			}
			return nil
		})).Sugar()
	}

	for _, n := range nodes {
		setupLogger(n)
		n.Setup()
	}

	t.Run("No leader rotation", func(t *testing.T) {
		startNodes(nodes, &network)

		nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

		for i := 0; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(1), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(0), atomic.LoadUint64(&boundCommitSignatures))

		nodes[0].Submit(Request{ID: "2", ClientID: "alice"})

		for i := 0; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(0), atomic.LoadUint64(&boundCommitSignatures))
	})

	t.Run("Activate leader rotation", func(t *testing.T) {
		network.StopServe()

		for _, n := range nodes {
			n.Consensus.Config.DecisionsPerLeader = 1
			n.Consensus.Config.LeaderRotation = true
			n.Restart()
		}

		network.StartServe()

		nodes[0].Submit(Request{ID: "3", ClientID: "alice"})

		for i := 0; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(1), atomic.LoadUint64(&boundCommitSignatures))

		nodes[0].Submit(Request{ID: "4", ClientID: "alice"})

		for i := 0; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&leaderRotationDisabled))
		assert.Equal(t, uint64(2), atomic.LoadUint64(&boundCommitSignatures))
	})

	return

	t.Run("Deactivate leader rotation", func(t *testing.T) {
		network.StopServe()

		for _, n := range nodes {
			n.Consensus.Config.DecisionsPerLeader = 0
			n.Consensus.Config.LeaderRotation = false
			n.Restart()
		}

		network.StartServe()

		nodes[0].Submit(Request{ID: "5", ClientID: "alice"})

		for i := 0; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&boundCommitSignatures))

		nodes[0].Submit(Request{ID: "6", ClientID: "alice"})

		for i := 0; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}

		assert.Equal(t, uint64(2), atomic.LoadUint64(&boundCommitSignatures))

	})
}

func TestBlacklistAndRedemption(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 10
	var nodes []*App
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
		nodes = append(nodes, n)
	}

	var viewChangedWG sync.WaitGroup
	viewChangedWG.Add(len(nodes) - 1)

	var blacklistPrunedWG sync.WaitGroup
	blacklistPrunedWG.Add(len(nodes) + 1) // Once at proposing, n times at verification

	var redemptionWG sync.WaitGroup
	redemptionWG.Add(len(nodes))

	start := time.Now()
	for _, n := range nodes {
		l := n.logger.Desugar()
		n.logger = l.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number 1, sequence 1,") {
				viewChangedWG.Done()
			}
			if strings.Contains(entry.Message, "Blacklist changed: [1] --> []") {
				blacklistPrunedWG.Done()
			}
			if strings.Contains(entry.Message, "Rotating leader from 10 to 1") {
				redemptionWG.Done()
			}
			return nil
		})).Sugar()
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	startNodes(nodes, &network)

	nodes[0].Disconnect() // Leader is in partition

	// Accelerate the time until a view change
	done := make(chan struct{})
	var counter uint64
	accelerateTime(nodes[1:], done, true, true, &counter)

	// Wait for view change
	viewChangedWG.Wait()
	close(done)

	// Rotate the leader and ensure the view doesn't change, which proves
	// node 1 never became the leader again
	for j := 0; j < numberOfNodes; j++ {
		nodes[1].Submit(Request{ID: fmt.Sprintf("%d", j), ClientID: "alice"})
		for i := 1; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}
	}

	// Ensure we remain on view 1, and that node 1 is in the blacklist
	md := &smartbftprotos.ViewMetadata{}
	err = proto.Unmarshal(nodes[1].lastDecision.Proposal.Metadata, md)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), md.ViewId)
	assert.Equal(t, []uint64{1}, md.BlackList)

	// Next, re-connect node 1
	nodes[0].Connect()

	// Accelerate time to wait for it to synchronize
	done = make(chan struct{})
	accelerateTime(nodes, done, true, true, &counter)
	for j := 0; j < numberOfNodes; j++ {
		<-nodes[0].Delivered
	}

	// Rotate the leader and ensure the view doesn't change,
	// but this time we want to ensure that node 1 became the leader
	// again at some point, and that it has been deleted from the blacklist.
	stop := make(chan struct{})
	f := func() {
		txID := make([]byte, 15)
		rand.Read(txID)
		nodes[1].Submit(Request{ID: hex.EncodeToString(txID), ClientID: "alice"})
		for i := 0; i < len(nodes); i++ {
			<-nodes[i].Delivered
		}
		md := &smartbftprotos.ViewMetadata{}
		err = proto.Unmarshal(nodes[1].lastDecision.Proposal.Metadata, md)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), md.ViewId)
	}
	go doInBackground(f, stop)
	blacklistPrunedWG.Wait()
	redemptionWG.Wait()
	close(done)
	close(stop)
}

func TestBlacklistMultipleViewChanges(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 10
	var nodes []*App
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, true, 1)
		nodes = append(nodes, n)
	}

	var viewChangedWG sync.WaitGroup
	viewChangedWG.Add(len(nodes) - 2)

	var node2RemovedFromBlacklist sync.WaitGroup
	node2RemovedFromBlacklist.Add(len(nodes) + 1)

	var node3RemovedFromBlacklist sync.WaitGroup
	node3RemovedFromBlacklist.Add(len(nodes) + 1)

	start := time.Now()
	for _, n := range nodes {
		l := n.logger.Desugar()
		n.logger = l.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number 3, sequence 2,") {
				viewChangedWG.Done()
			}
			if strings.Contains(entry.Message, "Node 2 was observed sending a prepare") && strings.Contains(entry.Message, "removing it from blacklist") {
				node2RemovedFromBlacklist.Done()
			}
			if strings.Contains(entry.Message, "Node 3 was observed sending a prepare") && strings.Contains(entry.Message, "removing it from blacklist") {
				node3RemovedFromBlacklist.Done()
			}
			return nil
		})).Sugar()
		n.heartbeatTime = make(chan time.Time, 1)
		n.heartbeatTime <- start
		n.viewChangeTime = make(chan time.Time, 1)
		n.viewChangeTime <- start
		n.Setup()
	}

	startNodes(nodes, &network)

	// Put a single decision
	nodes[0].Submit(Request{ID: "genesis", ClientID: "alice"})
	for i := 0; i < numberOfNodes; i++ {
		<-nodes[i].Delivered
	}

	// Put the next and the next next nodes in partition
	nodes[1].Disconnect()
	nodes[2].Disconnect()

	// Accelerate the time until a view change
	done := make(chan struct{})
	var acceleratedNodes []*App
	acceleratedNodes = append(acceleratedNodes, nodes[0])
	acceleratedNodes = append(acceleratedNodes, nodes[2:]...)
	var counter uint64
	accelerateTime(acceleratedNodes, done, true, true, &counter)

	// Wait for two view changes
	viewChangedWG.Wait()
	close(done)

	// Rotate the leader and ensure the view doesn't change, which proves
	// nodes 2 and 3 never became the leader again
	for j := 0; j < numberOfNodes; j++ {
		nodes[5].Submit(Request{ID: fmt.Sprintf("%d", j), ClientID: "alice"})
		for i := 3; i < numberOfNodes; i++ {
			<-nodes[i].Delivered
		}
	}

	// Ensure we remain on view 2, and that nodes 2,3 are in the blacklist
	md := &smartbftprotos.ViewMetadata{}
	err = proto.Unmarshal(nodes[5].lastDecision.Proposal.Metadata, md)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), md.ViewId)
	assert.Equal(t, []uint64{2, 3}, md.BlackList)

	// Next, re-connect nodes 2,3
	nodes[1].Connect()
	nodes[2].Connect()

	// Accelerate time to wait for them to synchronize
	done = make(chan struct{})
	accelerateTime(nodes, done, true, true, &counter)
	for j := 0; j < numberOfNodes; j++ {
		<-nodes[1].Delivered
		<-nodes[2].Delivered
	}
	close(done)

	// Rotate the leader and ensure the view doesn't change,
	stop := make(chan struct{})
	f := func() {
		txID := make([]byte, 15)
		rand.Read(txID)
		nodes[3].Submit(Request{ID: hex.EncodeToString(txID), ClientID: "alice"})
		for i := 0; i < len(nodes); i++ {
			<-nodes[i].Delivered
		}
		md := &smartbftprotos.ViewMetadata{}
		err = proto.Unmarshal(nodes[3].lastDecision.Proposal.Metadata, md)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), md.ViewId)
	}
	go doInBackground(f, stop)
	node2RemovedFromBlacklist.Wait()
	node3RemovedFromBlacklist.Wait()
	close(stop)
}

func doInBackground(f func(), stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
			f()
		}
	}
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

func requestIDFromBatch(record *AppRecord) int {
	n, _ := strconv.ParseInt(requestFromBytes(record.Batch.Requests[0]).ID, 10, 32)
	return int(n)
}

func waitForCatchup(targetReqID int, out chan *AppRecord) bool {
	for {
		select {
		case record := <-out:
			if requestIDFromBatch(record) == targetReqID {
				return true
			}
		case <-time.After(time.Millisecond * 100):
			return false
		}
	}
}

func accelerateTime(nodes []*App, done chan struct{}, heartbeatTime, viewChangeTime bool, counter *uint64) {
	go func() {
		for {
			atomic.AddUint64(counter, 1)
			newTime := time.Now().Add(time.Second * time.Duration(2*atomic.LoadUint64(counter)))
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 100):
				for _, n := range nodes {
					if heartbeatTime {
						n.heartbeatTime <- newTime
					}
					if viewChangeTime {
						n.viewChangeTime <- newTime
					}
				}
			}
		}
	}()
}

func startNodes(nodes []*App, network *Network) {
	for _, n := range nodes {
		if err := n.Consensus.Start(); err != nil {
			n.logger.Panicf("Consensus returned an error : %v", err)
		}
	}
	network.StartServe()
}
