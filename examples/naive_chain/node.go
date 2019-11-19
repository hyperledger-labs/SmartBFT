// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package naive

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
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
	BatchSize    uint64
	BatchTimeout time.Duration
}

type Node struct {
	clock       *time.Ticker
	secondClock *time.Ticker
	stopChan    chan struct{}
	doneWG      sync.WaitGroup
	prevHash    string
	id          uint64
	in          Ingress
	out         Egress
	deliverChan chan<- *Block
	consensus   *smartbft.Consensus
}

func (*Node) Sync() bft.Decision {
	panic("implement me")
}

func (*Node) RequestID(req []byte) bft.RequestInfo {
	txn := TransactionFromBytes(req)
	return bft.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

func (*Node) VerifyProposal(proposal bft.Proposal) ([]bft.RequestInfo, error) {
	blockData := BlockDataFromBytes(proposal.Payload)
	requests := make([]bft.RequestInfo, 0)
	for _, t := range blockData.Transactions {
		tx := TransactionFromBytes(t)
		reqInfo := bft.RequestInfo{ID: tx.ID, ClientID: tx.ClientID}
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
		ID: n.id,
	}
}

func (n *Node) AssembleProposal(metadata []byte, requests [][]byte) bft.Proposal {
	blockData := BlockData{Transactions: requests}.ToBytes()
	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(metadata, md); err != nil {
		panic(fmt.Sprintf("Unable to unmarshal metadata, error: %v", err))
	}
	return bft.Proposal{
		Header: BlockHeader{
			PrevHash: n.prevHash,
			DataHash: computeDigest(blockData),
			Sequence: int64(md.LatestSequence),
		}.ToBytes(),
		Payload:  BlockData{Transactions: requests}.ToBytes(),
		Metadata: metadata,
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
			ID:       txn.ID,
		})
	}
	header := BlockHeaderFromBytes(proposal.Header)

	select {
	case <-n.stopChan:
		return
	case n.deliverChan <- &Block{
		Sequence:     uint64(header.Sequence),
		PrevHash:     header.PrevHash,
		Transactions: txns,
	}:
	}
}

func NewNode(id uint64, in Ingress, out Egress, deliverChan chan<- *Block, logger smart.Logger, opts NetworkOptions, testDir string) *Node {
	nodeDir := filepath.Join(testDir, fmt.Sprintf("node%d", id))
	writeAheadLog, err := wal.Create(logger, nodeDir, nil)
	if err != nil {
		logger.Panicf("Cannot create WAL at %s", nodeDir)
	}

	node := &Node{
		clock:       time.NewTicker(time.Second),
		secondClock: time.NewTicker(time.Second),
		id:          id,
		in:          in,
		out:         out,
		deliverChan: deliverChan,
		stopChan:    make(chan struct{}),
	}

	config := smartbft.DefaultConfig
	config.SelfID = id
	config.RequestBatchMaxInterval = opts.BatchTimeout
	config.RequestBatchMaxCount = opts.BatchSize

	node.consensus = &smartbft.Consensus{
		Config:            config,
		ViewChangerTicker: node.secondClock.C,
		Scheduler:         node.clock.C,
		Logger:            logger,
		Comm:              node,
		Signer:            node,
		Verifier:          node,
		Application:       node,
		Assembler:         node,
		RequestInspector:  node,
		Synchronizer:      node,
		WAL:               writeAheadLog,
		Metadata: smartbftprotos.ViewMetadata{
			LatestSequence: 0,
			ViewId:         0,
		},
	}
	if err := node.consensus.Start(); err != nil {
		panic("error on consensus start")
	}
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
