package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/SmartBFT-Go/consensus/pkg/types"

	"github.com/stretchr/testify/assert"
)

func TestBasicReconfig(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir)
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

	newConfig := fastConfig
	newConfig.CollectTimeout = fastConfig.CollectTimeout * 2

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: newConfig}).CurrentConfig,
		},
	})

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestBasicAddNodes(t *testing.T) {
	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir)
		nodes = append(nodes, n)
	}
	startNodes(nodes, &network)

	for i := 1; i < 5; i++ {
		nodes[0].Submit(Request{ID: fmt.Sprintf("%d", i), ClientID: "alice"})
	}

	data1 := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data1 = append(data1, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data1[i], data1[i+1])
	}

	newNode1 := newNode(5, network, t.Name(), testDir)
	newNode2 := newNode(6, network, t.Name(), testDir)

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: fastConfig}).CurrentConfig,
		},
	})

	data2 := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data2 = append(data2, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data2[i], data2[i+1])
	}

	nodes = append(nodes, newNode1, newNode2)
	startNodes(nodes[4:], &network)

	d1 := <-nodes[4].Delivered
	d2 := <-nodes[5].Delivered
	assert.Equal(t, data1[0], d1)
	assert.Equal(t, data1[0], d2)
	d1 = <-nodes[4].Delivered
	d2 = <-nodes[5].Delivered
	assert.Equal(t, data2[0], d1)
	assert.Equal(t, data2[0], d2)

	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	numberOfNodes = 6
	data := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestBasicRemoveNodes(t *testing.T) {
	// In the beginning there are 7 nodes and the quorum size is 5,
	// after removing 3 nodes the new quorum size should be 4,
	// so transactions can be committed after the reconfiguration only if it was successful.

	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 7
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir)
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

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     []int64{1, 2, 3, 4},
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: fastConfig}).CurrentConfig,
		},
	})

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	nodes[4].Disconnect()
	nodes[5].Disconnect()
	nodes[6].Disconnect()

	numberOfNodes = 4
	nodes[0].Submit(Request{ID: "11", ClientID: "alice"})
	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}
}

func TestViewChangeAfterReconfig(t *testing.T) {
	// The initial nodes ids are {2,3,4,5}
	// After reconfiguration a fifth node is added with id 1
	// however this node is disconnected, and so a view change is needed

	t.Parallel()
	network := make(Network)
	defer network.Shutdown()

	testDir, err := ioutil.TempDir("", t.Name())
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 2; i <= numberOfNodes+1; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir)
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

	newNode := newNode(1, network, t.Name(), testDir)
	nodes = append(nodes, newNode)
	startNodes(nodes[4:], &network)
	nodes[4].Disconnect()

	nodes[0].Submit(Request{
		ClientID: "reconfig",
		ID:       "10",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     nodesToInt(nodes[0].Node.Nodes()),
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: fastConfig}).CurrentConfig,
		},
	})

	data = make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data = append(data, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data[i], data[i+1])
	}

	for i := 0; i < numberOfNodes; i++ {
		nodes[i].Submit(Request{ID: "11", ClientID: "alice"})
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
