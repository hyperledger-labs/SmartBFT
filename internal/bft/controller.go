// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

//go:generate mockery -dir . -name Verifier -case underscore -output ./mocks/
type Verifier interface {
	VerifyProposal(proposal types.Proposal, prevHeader []byte) error
	VerifyRequest(val []byte) error
	VerifyConsenterSig(signer uint64, signature []byte, prop types.Proposal) error
	VerificationSequence() uint64
}

//go:generate mockery -dir . -name Assembler -case underscore -output ./mocks/
type Assembler interface {
	AssembleProposal(metadata []byte, requests [][]byte) (nextProp types.Proposal, remainder [][]byte)
}

//go:generate mockery -dir . -name Application -case underscore -output ./mocks/
type Application interface {
	Deliver(proposal types.Proposal, signature []types.Signature)
}

//go:generate mockery -dir . -name Decider -case underscore -output ./mocks/
type Decider interface {
	Decide(proposal types.Proposal, signatures []types.Signature)
}

//go:generate mockery -dir . -name FailureDetector -case underscore -output ./mocks/
type FailureDetector interface {
	Complain()
}

//go:generate mockery -dir . -name Synchronizer -case underscore -output ./mocks/
type Synchronizer interface {
	SyncIfNeeded()
}

//go:generate mockery -dir . -name Comm -case underscore -output ./mocks/
type Comm interface {
	Broadcast(m *protos.Message)
}

//go:generate mockery -dir . -name Signer -case underscore -output ./mocks/
type Signer interface {
	Sign([]byte) []byte
	SignProposal(types.Proposal) *types.Signature
}

//go:generate mockery -dir . -name RequestPool -case underscore -output ./mocks/
type RequestPool interface {
	Submit(request []byte)
}

//go:generate mockery -dir . -name Batcher -case underscore -output ./mocks/
type Batcher interface {
	NextBatch() [][]byte
	BatchRemainder(remainder [][]byte)
}

type Future interface {
	Wait()
}

type Controller struct {
	// configuration
	ID              uint64
	N               uint64
	RequestPool     RequestPool
	Batcher         Batcher
	Verifier        Verifier
	Logger          Logger
	Assembler       Assembler
	Application     Application
	FailureDetector FailureDetector
	Synchronizer    Synchronizer
	Comm            Comm
	Signer          Signer

	quorum int

	currView       View
	currViewNumber uint64 // should be accessed atomically

	viewAbortChan chan struct{}
	viewLock      sync.RWMutex

	stopChan chan struct{}
	stopWG   sync.WaitGroup

	deliverChan chan struct{}

	viewReadyChan chan struct{}
}

func (c *Controller) iAmTheLeader() bool {
	return c.leaderID() == c.ID
}

func (c *Controller) leaderID() uint64 {
	// TODO use ids order (similar to BFT Smart)
	return atomic.LoadUint64(&c.currViewNumber) % c.N
}

func (c *Controller) computeQuorum() int {
	f := int(math.Floor((float64(c.N) - 1.0) / 3.0))
	q := int(math.Ceil((float64(c.N) + float64(f) + 1) / 2.0))
	c.Logger.Debugf("The number of nodes (N) is %d, F is %d, and the quorum size is %d", c.N, f, q)
	return q
}

// SubmitRequest submits a request to go through consensus
func (c *Controller) SubmitRequest(request []byte) {
	c.RequestPool.Submit(request)
}

// ProcessMessages dispatches the incoming message to the required component
func (c *Controller) ProcessMessages(sender uint64, m *protos.Message) {
	if IsViewMessage(m) {
		c.viewLock.RLock()
		c.currView.HandleMessage(sender, m)
		c.viewLock.RUnlock()
	}
	// TODO the msg can be a view change message or a tx req coming from a node after a timeout
}

func (c *Controller) startView(proposalSequence uint64) {
	// TODO view builder according to metadata returned by sync
	view := View{
		N:                c.N,
		LeaderID:         c.leaderID(),
		Quorum:           c.quorum,
		Number:           atomic.LoadUint64(&c.currViewNumber),
		Decider:          c,
		FailureDetector:  c.FailureDetector,
		Sync:             c.Synchronizer,
		Logger:           c.Logger,
		Comm:             c.Comm,
		Verifier:         c.Verifier,
		Signer:           c.Signer,
		ProposalSequence: proposalSequence,
		PrevHeader:       []byte{0}, // TODO start with real prev header
	}

	c.viewLock.Lock()
	c.currView = view
	c.Logger.Debugf("Starting view with number %d", atomic.LoadUint64(&c.currViewNumber))
	end := c.currView.Start()
	c.viewLock.Unlock()
	c.viewReadyChan <- struct{}{}
	end.Wait()
	c.viewAbortChan <- struct{}{}
}

func (c *Controller) viewAbort() {
	c.Logger.Debugf("Aborting current view with number %d", atomic.LoadUint64(&c.currViewNumber))
	c.viewLock.RLock()
	c.currView.Abort()
	c.viewLock.RUnlock()
	<-c.viewAbortChan
}

func (c *Controller) startNewView(newViewNumber uint64, newProposalSequence uint64) {
	atomic.StoreUint64(&c.currViewNumber, newViewNumber)
	c.stopWG.Add(1)
	go func() {
		defer c.stopWG.Done()
		c.startView(newProposalSequence)
	}()
	<-c.viewReadyChan
	if c.iAmTheLeader() {
		c.Logger.Debugf("Starting leader thread")
		c.stopWG.Add(1)
		go func() {
			defer c.stopWG.Done()
			c.leader()
		}()
	}
}

// ViewChanged makes the controller abort the current view and start a new one with the given numbers
func (c *Controller) ViewChanged(newViewNumber uint64, newProposalSequence uint64) {
	c.viewAbort()
	c.startNewView(newViewNumber, newProposalSequence)
}

func (c *Controller) getNextBatch() [][]byte {
	var validRequests [][]byte
	for len(validRequests) == 0 { // no valid requests in this batch
		select {
		case <-c.viewAbortChan:
			c.viewAbortChan <- struct{}{}
			return nil
		case <-c.stopChan:
			return nil
		default:
		}
		requests := c.Batcher.NextBatch()
		for _, req := range requests {
			err := c.Verifier.VerifyRequest(req)
			if err != nil {
				c.Logger.Warnf("Ignoring bad request: %v, verifier error is: %v", req, err)
				continue
			}
			validRequests = append(validRequests, req)
		}
	}
	return validRequests
}

func (c *Controller) propose() {
	nextBatch := c.getNextBatch()
	if nextBatch == nil {
		return
	}
	metadata := c.currView.GetMetadata()
	proposal, remainder := c.Assembler.AssembleProposal(metadata, nextBatch)
	if len(remainder) != 0 {
		c.Batcher.BatchRemainder(remainder)
	}
	c.Logger.Debugf("Leader proposing proposal: %v", proposal)
	c.viewLock.RLock()
	c.currView.Propose(proposal)
	c.viewLock.RUnlock()
}

func (c *Controller) leader() {
	for {
		c.propose()
		select {
		case <-c.viewAbortChan:
			c.viewAbortChan <- struct{}{}
			return
		case <-c.stopChan:
			return
		case <-c.deliverChan:
		}
	}
}

// Start the controller
func (c *Controller) Start(startViewNumber uint64, startProposalSequence uint64) Future {
	c.viewAbortChan = make(chan struct{})
	c.stopChan = make(chan struct{})
	c.deliverChan = make(chan struct{})
	c.viewReadyChan = make(chan struct{})
	c.quorum = c.computeQuorum()
	c.startNewView(startViewNumber, startProposalSequence)
	return &c.stopWG
}

// Stop the controller
func (c *Controller) Stop() {
	select {
	case <-c.stopChan:
		return
	default:
		c.viewLock.RLock()
		c.currView.Abort()
		c.viewLock.RUnlock()
		<-c.viewAbortChan
		close(c.stopChan)
	}
}

// Decide delivers the decision to the application
func (c *Controller) Decide(proposal types.Proposal, signatures []types.Signature) {
	// TODO write to WAL?
	// TODO remove and stop timeouts of included requests?
	c.Application.Deliver(proposal, signatures)
	if c.iAmTheLeader() {
		c.deliverChan <- struct{}{}
	}
}
