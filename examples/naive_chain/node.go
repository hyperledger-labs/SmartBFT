// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	smart "github.com/SmartBFT-Go/consensus/pkg/api"
	smartbft "github.com/SmartBFT-Go/consensus/pkg/consensus"
	bft "github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
)

type Ingress map[int]<-chan proto.Message
type Egress map[int]chan<- proto.Message

type NetworkOptions struct {
	NumNodes     int
	BatchSize    int
	BatchTimeout time.Duration
}

type Node struct {
	clock       *time.Ticker
	secondClock *time.Ticker
	stopChan    chan struct{}
	doneWG      sync.WaitGroup
	nextSeq     uint64
	prevHash    string
	id          uint64
	in          Ingress
	out         Egress
	deliverChan chan<- *Block
	consensus   *smartbft.Consensus
}

func (*Node) Sync() (smartbftprotos.ViewMetadata, uint64) {
	panic("implement me")
}

func (*Node) RequestID(req []byte) bft.RequestInfo {
	txn := TransactionFromBytes(req)
	return bft.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.Id,
	}
}

func (*Node) VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error) {
	blockData := BlockDataFromBytes(proposal.Payload)
	requests := make([]bft.RequestInfo, 0)
	for _, t := range blockData.Transactions {
		tx := TransactionFromBytes(t)
		reqInfo := bft.RequestInfo{ID: tx.Id, ClientID: tx.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

func (*Node) VerifyRequest(val []byte) (bft.RequestInfo, error) {
	return bft.RequestInfo{}, nil
}

func (*Node) VerifyConsenterSig(_ bft.Signature, prop bft.Proposal) error {
	return nil
}

func (*Node) VerifySignature(signature bft.Signature) error {
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
		Id: n.id,
	}
}

func (n *Node) AssembleProposal(metadata []byte, requests [][]byte) (nextProp bft.Proposal, remainder [][]byte) {
	blockData := BlockData{Transactions: requests}.ToBytes()
	return bft.Proposal{
		Header: BlockHeader{
			PrevHash: n.prevHash,
			DataHash: computeDigest(blockData),
			Sequence: int64(atomic.LoadUint64(&n.nextSeq)),
		}.ToBytes(),
		Payload: BlockData{Transactions: requests}.ToBytes(),
		Metadata: marshalOrPanic(&smartbftprotos.ViewMetadata{
			LatestSequence: n.nextSeq,
			ViewId:         0, // TODO: change this when implementing view change
		}),
	}, nil
}

func marshalOrPanic(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return b
}

func (n *Node) BroadcastConsensus(m *smartbftprotos.Message) {
	for receiver, out := range n.out {
		if n.id == uint64(receiver) {
			continue
		}
		out <- m
	}
}

func (n *Node) SendConsensus(targetID uint64, message *smartbftprotos.Message) {
	n.out[int(targetID)] <- message
}

func (n *Node) SendTransaction(targetID uint64, request []byte) {
	msg := &FwdMessage{
		Payload: request,
		Sender:  n.id,
	}
	n.out[int(targetID)] <- msg
}

func (n *Node) Deliver(proposal bft.Proposal, signature []bft.Signature) {
	blockData := BlockDataFromBytes(proposal.Payload)
	var txns []Transaction
	for _, rawTxn := range blockData.Transactions {
		txn := TransactionFromBytes(rawTxn)
		txns = append(txns, Transaction{
			ClientID: txn.ClientID,
			Id:       txn.Id,
		})
	}
	header := BlockHeaderFromBytes(proposal.Header)
	atomic.AddUint64(&n.nextSeq, 1)
	n.deliverChan <- &Block{
		Sequence:     uint64(header.Sequence),
		PrevHash:     header.PrevHash,
		Transactions: txns,
	}
}

func NewNode(id uint64, in Ingress, out Egress, deliverChan chan<- *Block, logger smart.Logger, opts NetworkOptions) *Node {
	node := &Node{
		clock:       time.NewTicker(time.Second),
		nextSeq:     1,
		id:          id,
		in:          in,
		out:         out,
		deliverChan: deliverChan,
		stopChan:    make(chan struct{}),
	}
	node.consensus = &smartbft.Consensus{
		Scheduler:        node.clock.C,
		SelfID:           id,
		BatchSize:        opts.BatchSize,
		BatchTimeout:     opts.BatchTimeout,
		Logger:           logger,
		Comm:             node,
		Signer:           node,
		Verifier:         node,
		Application:      node,
		Assembler:        node,
		RequestInspector: node,
		Synchronizer:     node,
		WAL:              &wal.EphemeralWAL{},
	}
	node.consensus.Start()
	node.Start()
	return node
}

func (n *Node) Start() {
	for id, in := range n.in {
		if uint64(id) == n.id {
			continue
		}
		n.doneWG.Add(1)

		go func(id uint64, in <-chan proto.Message) {
			defer n.doneWG.Done()

			for {
				select {
				case <-n.stopChan:
					return
				case msg := <-in:
					switch msg.(type) {
					case *smartbftprotos.Message:
						n.consensus.HandleMessage(id, msg.(*smartbftprotos.Message))
					case *FwdMessage:
						n.consensus.SubmitRequest(msg.(*FwdMessage).Payload)
					}
				}
			}
		}(uint64(id), in)
	}
}

func (n *Node) Stop() {
	select {
	case <-n.stopChan:
		break
	default:
		close(n.stopChan)
	}
	n.clock.Stop()
	n.doneWG.Wait()
	n.consensus.Stop()
}

func (n *Node) Nodes() []uint64 {
	var nodes []uint64
	for id := range n.in {
		nodes = append(nodes, uint64(id))
	}

	return nodes
}

func computeDigest(rawBytes []byte) string {
	h := sha256.New()
	h.Write(rawBytes)
	digest := h.Sum(nil)
	return hex.EncodeToString(digest)
}
