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
