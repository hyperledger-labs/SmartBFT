// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/recorder"

	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestOnboardingRecording(t *testing.T) {
	t.Parallel()
	network := make(Network)

	recorder.RegisterDecoders(nil)

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	// Record last node
	recording := &bytes.Buffer{}
	nodes[3].Consensus.Recording = recording

	runSyncScenario := func(nodes []*App, network *Network, recording bool) {
		deliverWG := sync.WaitGroup{}
		deliverWG.Add(1)
		syncWG := sync.WaitGroup{}
		syncWG.Add(1)
		baseLogger := nodes[3].logger.Desugar()
		nodes[3].Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Starting view with number 0, sequence 7, and decisions 6") {
				syncWG.Done()
			}
			if strings.Contains(entry.Message, "Node 4 delivered proposal with view 0 and sequence 7") {
				deliverWG.Done()
			}
			return nil
		})).Sugar()

		startNodes(nodes, network)
		nodes[3].Disconnect()

		syncDelay := make(chan struct{})

		for i := 0; i < 5; i++ {
			nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
			// Wait for all nodes but the last one to commit
			for i := 0; i < numberOfNodes-1; i++ {
				<-nodes[i].Delivered
			}
		}

		nodes[3].DelaySync(syncDelay, true)
		nodes[3].Connect()

		// Commit some more transactions so last node will see it is behind
		for i := 5; i < 6; i++ {
			nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
			// Wait for all nodes but the last done to commit
			for i := 0; i < numberOfNodes-1; i++ {
				<-nodes[i].Delivered
			}
		}

		// Make sure sync is delayed until the rest of the nodes manage to commit
		close(syncDelay)
		syncWG.Wait()

		// Make one last transaction
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", 100), ClientID: "alice"})
		// Wait for all nodes to commit
		for i := 0; i < numberOfNodes-1; i++ {
			<-nodes[i].Delivered
		}
		if recording {
			for i := 0; i < 7; i++ {
				<-nodes[3].Delivered
			}
		} else {
			deliverWG.Wait()
		}
	}

	runSyncScenario(nodes, &network, true)

	// Now, shut down all nodes and re-create them with the last node receiving information from the recording
	network.Shutdown()
	os.RemoveAll(testDir)

	network = make(Network)
	defer network.Shutdown()

	testDir, err = ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	nodes = make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	// Setup recording transcript for last node
	nodes[3].Consensus.Transcript = recording
	nodes[3].Consensus.Recording = nil

	// Detach the committed batches of the node from the committed batches of the other nodes
	// so sync won't work, to force the sync output to come from the transcript
	nodes[3].Node.cb = &committedBatches{}

	// Start nodes and repeat the test
	runSyncScenario(nodes, &network, false)
	// Ensure the last node doesn't share committed entries with the rest,
	// which proves it could have not synced naturally.
	assert.Len(t, nodes[0].Node.cb.readAll(smartbftprotos.ViewMetadata{}), 7)
	assert.Len(t, nodes[1].Node.cb.readAll(smartbftprotos.ViewMetadata{}), 7)
	assert.Len(t, nodes[2].Node.cb.readAll(smartbftprotos.ViewMetadata{}), 7)
	assert.Len(t, nodes[3].Node.cb.readAll(smartbftprotos.ViewMetadata{}), 0)
}

func TestDecisionRecording(t *testing.T) {
	t.Parallel()
	network := make(Network)

	recorder.RegisterDecoders(nil)

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	// Record last node
	recording := &bytes.Buffer{}
	nodes[3].Consensus.Recording = recording

	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: fmt.Sprintf("%d", 100), ClientID: "alice"})
	// Wait for all nodes to commit
	for i := 0; i < numberOfNodes; i++ {
		<-nodes[i].Delivered
	}

	// Now, shut down all nodes and re-create them with the last node receiving information from the recording
	network.Shutdown()
	os.RemoveAll(testDir)

	network = make(Network)
	defer network.Shutdown()

	testDir, err = ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	nodes = make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	// Setup recording transcript for last node
	nodes[3].Consensus.Transcript = recording
	nodes[3].Consensus.Recording = nil

	deliverWG := sync.WaitGroup{}
	deliverWG.Add(1)
	baseLogger := nodes[3].logger.Desugar()
	nodes[3].Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Node 4 delivered proposal with view 0 and sequence 1") {
			deliverWG.Done()
		}
		return nil
	})).Sugar()

	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: fmt.Sprintf("%d", 100), ClientID: "alice"})
	// Wait for all nodes to commit
	for i := 0; i < numberOfNodes-1; i++ {
		<-nodes[i].Delivered
	}

	select {
	case <-nodes[3].Delivered:
		panic("Not using the recorder deliver")
	default:
		deliverWG.Wait()
	}
}

func TestAssemblerRecording(t *testing.T) {
	t.Parallel()

	recorder.RegisterDecoders(nil)

	network := make(Network)

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	// Record last nodex
	recording := &bytes.Buffer{}
	nodes[0].Consensus.Recording = recording // make sure the leader is recording

	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: fmt.Sprintf("%d", 100), ClientID: "alice"})
	// Wait for all nodes to commit
	var delivered *AppRecord
	for i := 0; i < numberOfNodes; i++ {
		delivered = <-nodes[i].Delivered
	}

	assert.NotNil(t, delivered.Batch.Requests[0])

	nodes[0].Submit(Request{ID: fmt.Sprintf("%d", 200), ClientID: "alice"})
	// Wait for all nodes to commit
	for i := 0; i < numberOfNodes; i++ {
		delivered = <-nodes[i].Delivered
	}

	assert.NotNil(t, delivered.Batch.Requests[0])

	// Now, shut down all nodes and re-create them with the leader node receiving information from the recording
	network.Shutdown()
	os.RemoveAll(testDir)

	network = make(Network)
	defer network.Shutdown()

	testDir, err = ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	nodes = make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.Consensus.Config.SyncOnStart = false
		nodes = append(nodes, n)
	}

	// Setup recording transcript for the leader node
	nodes[0].Consensus.Transcript = recording
	nodes[0].Consensus.Recording = nil

	baseLogger := nodes[0].logger.Desugar()
	nodes[0].Consensus.Logger = baseLogger.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Proposing proposal 2") {
			nodes[0].Disconnect()
		}
		return nil
	})).Sugar()

	startNodes(nodes, &network)

	nodes[0].Submit(Request{ID: fmt.Sprintf("%d", 100), ClientID: "alice"})
	// Wait for nodes to commit
	for i := 1; i < numberOfNodes; i++ {
		delivered = <-nodes[i].Delivered
	}

	assert.Nil(t, delivered.Batch.Requests)

}

type messageSeeker struct {
	handler        func(sender uint64, m *smartbftprotos.Message)
	transcript     *bufio.Scanner
	decodingEvents chan struct{}
	stop           chan struct{}
}

func newMessageSeeker(transcript io.Reader, h func(sender uint64, m *smartbftprotos.Message)) *messageSeeker {
	scanner := bufio.NewScanner(transcript)
	return &messageSeeker{
		transcript:     scanner,
		decodingEvents: make(chan struct{}, 1),
		stop:           make(chan struct{}),
		handler:        h,
	}
}

func (ms *messageSeeker) shutdown() {
	close(ms.decodingEvents)
}

func (ms *messageSeeker) seekMessages() {
	for {
		select {
		case <-ms.stop:
			return
		case <-ms.decodingEvents:
			time.Sleep(time.Millisecond * 100)
			ms.maybeMessageDispatch()
		}
	}
}

func (ms *messageSeeker) maybeMessageDispatch() {
	if !ms.transcript.Scan() {
		return
	}

	entry := ms.transcript.Text()
	// Figure out whether entry is a message
	sep := strings.Index(entry, " ")
	entryType := entry[:sep]
	_ = entryType
	if false {
		// Invoke correct decoder function
		ms.handler(0, nil)
	}
}

func (ms *messageSeeker) wrapDecoder(f func(in []byte) interface{}) func([]byte) interface{} {
	return func(in []byte) interface{} {
		defer func() {
			ms.decodingEvents <- struct{}{}
		}()
		return f(in)
	}
}
