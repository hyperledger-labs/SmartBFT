// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestOnboardingRecording(t *testing.T) {
	t.Parallel()
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
