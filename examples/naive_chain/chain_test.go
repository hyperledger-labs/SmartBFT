// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestTxn(t *testing.T) {
	txn := &Transaction{
		Id:       "id",
		ClientID: "client",
	}
	rawTxn := txn.ToBytes()
	txn2 := TransactionFromBytes(rawTxn)
	assert.Equal(t, txn, txn2)
}

func TestBlock(t *testing.T) {
	tx1 := &Transaction{
		Id:       "tx1",
		ClientID: "alice",
	}
	tx2 := &Transaction{
		Id:       "tx2",
		ClientID: "bob",
	}
	tx3 := &Transaction{
		Id:       "tx3",
		ClientID: "carol",
	}

	block := &BlockData{
		Transactions: [][]byte{
			tx1.ToBytes(),
			tx2.ToBytes(),
			tx3.ToBytes(),
		},
	}

	rawBlock := block.ToBytes()
	block2 := BlockDataFromBytes(rawBlock)
	assert.Equal(t, block, block2)
}

func TestBlockHeader(t *testing.T) {
	blockHeader := &BlockHeader{
		PrevHash: "prev",
		Sequence: 2,
		DataHash: "hash",
	}

	headerBytes := blockHeader.ToBytes()
	blockHeader2 := BlockHeaderFromBytes(headerBytes)
	assert.Equal(t, blockHeader, blockHeader2)

}

func TestChain(t *testing.T) {
	blockCount := 10

	testDir, err := ioutil.TempDir("", "naive_chain")
	assert.NoErrorf(t, err, "generate temporary test dir")
	defer os.RemoveAll(testDir)

	chains := setupNetwork(t, NetworkOptions{NumNodes: 4, BatchSize: 1, BatchTimeout: 10 * time.Second}, testDir)

	for blockSeq := 1; blockSeq < blockCount; blockSeq++ {
		err := chains[0].Order(Transaction{
			ClientID: "alice",
			Id:       fmt.Sprintf("tx%d", blockSeq),
		})
		assert.NoError(t, err)
		for _, chain := range chains {
			block := chain.Listen()
			assert.Equal(t, uint64(blockSeq), block.Sequence)
			assert.Equal(t, []Transaction{{Id: fmt.Sprintf("tx%d", blockSeq), ClientID: "alice"}}, block.Transactions)
		}
	}

	for _, chain := range chains {
		chain.node.Stop()
	}
}

func setupNode(t *testing.T, id int, opt NetworkOptions, network map[int]map[int]chan proto.Message, testDir string) *Chain {
	ingress := make(Ingress)
	for from := 0; from < opt.NumNodes; from++ {
		ingress[from] = network[id][from]
	}

	egress := make(Egress)
	for to := 0; to < opt.NumNodes; to++ {
		egress[to] = network[to][id]
	}

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	chain := NewChain(uint64(id), ingress, egress, logger, opt, testDir)

	return chain
}

func setupNetwork(t *testing.T, opt NetworkOptions, testDir string) map[int]*Chain {
	network := make(map[int]map[int]chan proto.Message)

	chains := make(map[int]*Chain)

	for id := 0; id < opt.NumNodes; id++ {
		network[id] = make(map[int]chan proto.Message)
		for i := 0; i < opt.NumNodes; i++ {
			network[id][i] = make(chan proto.Message, 128)
		}
	}

	for id := 0; id < opt.NumNodes; id++ {
		chains[id] = setupNode(t, id, opt, network, testDir)
	}
	return chains
}
