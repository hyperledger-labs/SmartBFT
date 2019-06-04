// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"crypto/sha256"
	"encoding/hex"

	smart "github.com/SmartBFT-Go/consensus/pkg/api"
	smartbft "github.com/SmartBFT-Go/consensus/pkg/consensus"
	bft "github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"github.com/SmartBFT-Go/consensus/protos"
)

type Ingress map[int]<-chan *protos.Message
type Egress map[int]chan<- *protos.Message

type Node struct {
	stopChan    chan struct{}
	nextSeq     int
	prevHash    string
	id          int
	in          Ingress
	out         Egress
	deliverChan chan<- *Block
	consensus   *smartbft.Consensus
}

func (*Node) Sync() uint64 {
	panic("implement me")
}

func (*Node) RequestID(req []byte) bft.RequestInfo {
	txn := TransactionFromBytes(req)
	return bft.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.Id,
	}
}

func (*Node) VerifyProposal(proposal bft.Proposal, prevHeader []byte) error {
	return nil
}

func (*Node) VerifyRequest(val []byte) error {
	return nil
}

func (*Node) VerifyConsenterSig(signer uint64, signature []byte, prop bft.Proposal) error {
	return nil
}

func (*Node) VerificationSequence() uint64 {
	return 0
}

func (*Node) Sign(msg []byte) []byte {
	return nil
}

func (n *Node) SignProposal(bft.Proposal) *bft.Signature {
	return &bft.Signature{
		Id: uint64(n.id),
	}
}

func (n *Node) AssembleProposal(metadata []byte, requests [][]byte) (nextProp bft.Proposal, remainder [][]byte) {
	blockData := BlockData{Transactions: requests}.ToBytes()
	return bft.Proposal{
		Header: BlockHeader{
			PrevHash: n.prevHash,
			DataHash: computeDigest(blockData),
			Sequence: int64(n.nextSeq),
		}.ToBytes(),
		Payload: BlockData{Transactions: requests}.ToBytes(),
	}, nil
}

func (n *Node) Broadcast(m *protos.Message) {
	for _, out := range n.out {
		out <- m
	}
}

func (n *Node) Send(targetID uint64, message *protos.Message) {
	n.out[int(targetID)] <- message
}

func (n *Node) Deliver(proposal bft.Proposal, signature []bft.Signature) {
	blockData := BlockDataFromBytes(proposal.Payload)
	var txns []Transaction
	for _, rawTxn := range blockData.Transactions {
		txn := TransactionFromBytes(rawTxn)
		txns = append(txns, Transaction{
			ClientID: txn.ClientID,
			Id:       txn.ClientID,
		})
	}
	header := BlockHeaderFromBytes(proposal.Header)
	n.deliverChan <- &Block{
		Sequence:     uint64(header.Sequence),
		PrevHash:     header.PrevHash,
		Transactions: txns,
	}
}

func NewNode(id int, in Ingress, out Egress, deliverChan chan<- *Block, logger smart.Logger) *Node {
	node := &Node{
		id:          id,
		in:          in,
		out:         out,
		deliverChan: deliverChan,
		stopChan:    make(chan struct{}),
	}
	consensus := &smartbft.Consensus{
		Logger:           logger,
		Comm:             node,
		Signer:           node,
		Verifier:         node,
		Application:      node,
		Assembler:        node,
		RequestInspector: node,
		Synchronizer:     node,
		WAL1:             &wal.EphemeralWAL{},
		WAL2:             &wal.EphemeralWAL{},
	}
	node.consensus = consensus
	return node
}

func (n *Node) Start() {
	for id, in := range n.in {
		go func(id uint64) {
			for {
				select {
				case n.stopChan:
					return
				case msg := <-in:
					n.consensus.HandleMessage(id, msg)
				}
			}
		}(uint64(id))
	}
}

func computeDigest(rawBytes []byte) string {
	h := sha256.New()
	h.Write(rawBytes)
	digest := h.Sum(nil)
	return hex.EncodeToString(digest)
}
