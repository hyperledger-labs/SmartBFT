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

	quitChan := make(chan struct{})
	defer close(quitChan)



	network := make(map[int]chan *protos.Message)
	n := 10

	chains := make(map[int]*Chain)

	for id := 0; id < n; id++ {
		network[id] = make(chan *protos.Message)
	}

	for id := 0; id < n; id++ {
		chains[id] = setupNode(t, id, n, network, quitChan)
	}

	
}

func setupNode(t *testing.T, id, n int, network map[int]chan *protos.Message, abortChan <-chan struct{}) *Chain {
	ingress := make(Ingress)
	for from := 0; from < n; from++ {
		ingress[from] = network[id]
	}

	egress := make(Egress)
	for to := 0; to < n; to++ {
		egress[to] = network[to]
	}

	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger := basicLog.Sugar()

	chain := NewChain(id, ingress, egress, logger)

	for from := 0; from < n; from++ {
		if from == id {
			continue
		}
		go listenForMessages(from, ingress, chain.node.HandleMessage, abortChan)
	}

	return chain
}

func listenForMessages(from int, ingress Ingress, processMsg func(from uint64, msg *protos.Message), abortChan <-chan struct{}) {
	for {
		select {
		case msg := <-ingress[from]:
			processMsg(uint64(from), msg)
		case <-abortChan:
			return
		}
	}
}
