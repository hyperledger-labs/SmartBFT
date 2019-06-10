// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"testing"

	"github.com/SmartBFT-Go/consensus/protos"
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
	network := make(map[int]map[int]chan *protos.Message)
	n := 4

	chains := make(map[int]*Chain)

	for id := 0; id < n; id++ {
		network[id] = make(map[int]chan *protos.Message)
		for i := 0; i < n; i++ {
			network[id][i] = make(chan *protos.Message)
		}
	}

	for id := 0; id < n; id++ {
		chains[id] = setupNode(t, id, n, network)
	}

	chains[0].Order(Transaction{
		ClientID: "alice",
		Id:       "1",
	})

	for _, chain := range chains {
		block := chain.Listen()
		assert.Equal(t, uint64(0), block.Sequence)
		assert.Equal(t, []Transaction{{Id: "1", ClientID: "alice"}}, block.Transactions)
	}
}

func setupNode(t *testing.T, id, n int, network map[int]map[int]chan *protos.Message) *Chain {
	ingress := make(Ingress)
	for from := 0; from < n; from++ {
		ingress[from] = network[id][from]
	}

	egress := make(Egress)
	for to := 0; to < n; to++ {
		egress[to] = network[to][id]
	}

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	chain := NewChain(id, ingress, egress, logger)

	return chain
}
