// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"fmt"
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

	chains := setupNetwork(t, NetworkOptions{NumNodes: 4, BatchSize: 1, BatchTimeout: 10 * time.Second})

	for blockSeq := 0; blockSeq < blockCount; blockSeq++ {
		for _, chain := range chains {
			err := chain.Order(Transaction{
				ClientID: "alice",
				Id:       fmt.Sprintf("tx%d", blockSeq),
			})
			assert.NoError(t, err)
		}
	}

	for blockSeq := 0; blockSeq < blockCount; blockSeq++ {
		for _, chain := range chains {
			block := chain.Listen()
			assert.Equal(t, uint64(blockSeq), block.Sequence)
			assert.Equal(t, []Transaction{{Id: fmt.Sprintf("tx%d", blockSeq), ClientID: "alice"}}, block.Transactions)
		}
	}
}

func TestChainPartialSubmissions(t *testing.T) {

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	txCount := 20
	chains := setupNetwork(t, NetworkOptions{NumNodes: 4, BatchSize: 2, BatchTimeout: 1 * time.Second})

	txMap := make(map[Transaction]bool)
	for txSqn := 0; txSqn < txCount; txSqn++ {
		// Each tx to a different chain
		chain := chains[txSqn%len(chains)]

		tx := Transaction{
			ClientID: "alice",
			Id:       fmt.Sprintf("tx%d", txSqn),
		}
		err := chain.Order(tx)
		txMap[tx] = true
		assert.NoError(t, err)
	}

	txByChain := make(map[int][]Transaction)
	var blockSqn uint64 = 0
	for txNum := 0; txNum < txCount; {
		numTxInBlock := 0
		for i, chain := range chains {
			block := chain.Listen()
			numTxInBlock = len(block.Transactions)
			for _, tx := range block.Transactions {
				txByChain[i] = append(txByChain[i], tx)
			}

			assert.Equal(t, blockSqn, block.Sequence)
		}
		txNum += numTxInBlock
		blockSqn++
	}

	// all the chains must have the same tx sequence
	for i, chainTxs := range txByChain {
		if i == 0 {
			continue
		}
		assert.Equal(t, chainTxs, txByChain[0])
	}

	// all tx's arrived
	assert.Equal(t, txCount, len(txByChain[0]))
	txMapResult := make(map[Transaction]bool)
	for _, tx := range txByChain[0] {
		logger.Infof("Tx: %v", tx)
		txMapResult[tx] = true
	}

	assert.Equal(t, txMap, txMapResult)
}

func setupNode(t *testing.T, id int, opt NetworkOptions, network map[int]map[int]chan proto.Message) *Chain {
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

	chain := NewChain(uint64(id), ingress, egress, logger, opt)

	return chain
}

func setupNetwork(t *testing.T, opt NetworkOptions) map[int]*Chain {
	network := make(map[int]map[int]chan proto.Message)

	chains := make(map[int]*Chain)

	for id := 0; id < opt.NumNodes; id++ {
		network[id] = make(map[int]chan proto.Message)
		for i := 0; i < opt.NumNodes; i++ {
			network[id][i] = make(chan proto.Message)
		}
	}

	for id := 0; id < opt.NumNodes; id++ {
		chains[id] = setupNode(t, id, opt, network)
	}
	return chains
}
