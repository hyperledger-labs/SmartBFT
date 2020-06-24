package test

import (
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

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

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

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

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
	// after removing 3 nodes the new quorum size should be 3,
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

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

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

func TestAddRemoveNodes(t *testing.T) {
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

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

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
	newNode3 := newNode(7, network, t.Name(), testDir)
	newNode4 := newNode(8, network, t.Name(), testDir)
	newNode5 := newNode(9, network, t.Name(), testDir)
	newNode6 := newNode(10, network, t.Name(), testDir)

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

	nodes = append(nodes, newNode1, newNode2, newNode3, newNode4, newNode5, newNode6)
	startNodes(nodes[4:], &network)

	d4 := <-nodes[4].Delivered
	d5 := <-nodes[5].Delivered
	d6 := <-nodes[6].Delivered
	d7 := <-nodes[7].Delivered
	d8 := <-nodes[8].Delivered
	d9 := <-nodes[9].Delivered
	assert.Equal(t, data1[0], d4)
	assert.Equal(t, data1[0], d5)
	assert.Equal(t, data1[0], d6)
	assert.Equal(t, data1[0], d7)
	assert.Equal(t, data1[0], d8)
	assert.Equal(t, data1[0], d9)
	d4 = <-nodes[4].Delivered
	d5 = <-nodes[5].Delivered
	d6 = <-nodes[6].Delivered
	d7 = <-nodes[7].Delivered
	d8 = <-nodes[8].Delivered
	d9 = <-nodes[9].Delivered
	assert.Equal(t, data2[0], d4)
	assert.Equal(t, data2[0], d5)
	assert.Equal(t, data2[0], d6)
	assert.Equal(t, data2[0], d7)
	assert.Equal(t, data2[0], d8)
	assert.Equal(t, data2[0], d9)

	nodes[9].Submit(Request{ID: "11", ClientID: "alice"})
	numberOfNodes = 10
	data3 := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data3 = append(data3, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data3[i], data3[i+1])
	}

	nodes[9].Submit(Request{
		ClientID: "reconfig",
		ID:       "20",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     []int64{2, 3, 4, 5, 6, 7, 8, 9, 10},
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: fastConfig}).CurrentConfig,
		},
	})

	data4 := make([]*AppRecord, 0)
	for i := 0; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data4 = append(data4, d)
	}
	for i := 0; i < numberOfNodes-1; i++ {
		assert.Equal(t, data4[i], data4[i+1])
	}

	nodes[0].Disconnect()

	nodes[9].Submit(Request{ID: "21", ClientID: "alice"})
	data5 := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data5 = append(data5, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data5[i], data5[i+1])
	}

	nodes[9].Submit(Request{
		ClientID: "reconfig",
		ID:       "30",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     []int64{3, 4, 5, 6, 7, 8, 9, 10},
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: fastConfig}).CurrentConfig,
		},
	})

	data6 := make([]*AppRecord, 0)
	for i := 1; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data6 = append(data6, d)
	}
	for i := 0; i < numberOfNodes-2; i++ {
		assert.Equal(t, data6[i], data6[i+1])
	}

	nodes[1].Disconnect()

	nodes[9].Submit(Request{ID: "31", ClientID: "alice"})
	data7 := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data7 = append(data7, d)
	}
	for i := 0; i < numberOfNodes-3; i++ {
		assert.Equal(t, data7[i], data7[i+1])
	}

	nodes[9].Submit(Request{
		ClientID: "reconfig",
		ID:       "40",
		Reconfig: Reconfig{
			InLatestDecision: true,
			CurrentNodes:     []int64{4, 5, 6, 7, 8, 9, 10},
			CurrentConfig:    recconfigToInt(types.Reconfig{CurrentConfig: fastConfig}).CurrentConfig,
		},
	})

	data8 := make([]*AppRecord, 0)
	for i := 2; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data8 = append(data8, d)
	}
	for i := 0; i < numberOfNodes-3; i++ {
		assert.Equal(t, data6[i], data6[i+1])
	}

	nodes[2].Disconnect()

	nodes[9].Submit(Request{ID: "41", ClientID: "alice"})
	data9 := make([]*AppRecord, 0)
	for i := 3; i < numberOfNodes; i++ {
		d := <-nodes[i].Delivered
		data9 = append(data9, d)
	}
	for i := 0; i < numberOfNodes-4; i++ {
		assert.Equal(t, data9[i], data9[i+1])
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

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

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
