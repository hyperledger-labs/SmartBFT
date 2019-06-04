package naive

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	//network := make(map[int]chan protos.Message)
}
