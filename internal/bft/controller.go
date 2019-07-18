// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	protos "github.com/SmartBFT-Go/consensus/smartbftprotos"
)

//go:generate mockery -dir . -name Decider -case underscore -output ./mocks/
type Decider interface {
	Decide(proposal types.Proposal, signatures []types.Signature, requests []types.RequestInfo)
}

//go:generate mockery -dir . -name FailureDetector -case underscore -output ./mocks/
type FailureDetector interface {
	Complain()
}

//go:generate mockery -dir . -name Batcher -case underscore -output ./mocks/
type Batcher interface {
	NextBatch() [][]byte
	BatchRemainder(remainder [][]byte)
	Close()
}

type RequestPool interface {
	Submit(request []byte) error
	Size() int
	NextRequests(n int) [][]byte
	RemoveRequest(request types.RequestInfo) error
}

type Future interface {
	Wait()
}

type Controller struct {
	// configuration
	ID               uint64
	N                uint64
	RequestPool      RequestPool
	RequestTimeout   time.Duration
	Batcher          Batcher
	Verifier         api.Verifier
	Logger           api.Logger
	Assembler        api.Assembler
	Application      api.Application
	FailureDetector  FailureDetector
	Synchronizer     api.Synchronizer
	Comm             api.Comm
	Signer           api.Signer
	RequestInspector api.RequestInspector
	WAL              api.WriteAheadLog

	quorum int

	currView       View
	currViewNumber uint64 // should be accessed atomically

	viewAbortChan chan struct{}
	viewLock      sync.RWMutex

	stopChan chan struct{}
	stopWG   sync.WaitGroup

	deliverChan chan struct{}
}

func (c *Controller) iAmTheLeader() bool {
	return c.leaderID() == c.ID
}

func (c *Controller) leaderID() uint64 {
	// TODO use ids order (similar to BFT Smart)
	return atomic.LoadUint64(&c.currViewNumber) % c.N
}

func (c *Controller) computeQuorum() int {
	f := int((int(c.N) - 1) / 3)
	q := int(math.Ceil((float64(c.N) + float64(f) + 1) / 2.0))
	c.Logger.Debugf("The number of nodes (N) is %d, F is %d, and the quorum size is %d", c.N, f, q)
	return q
}

// SubmitRequest Submits a request to go through consensus.
func (c *Controller) SubmitRequest(request []byte) error {
	info := c.RequestInspector.RequestID(request)

	err := c.RequestPool.Submit(request)
	if err != nil {
		c.Logger.Warnf("Request %s was not submitted, error: %s", info, err)
		return err
	}

	c.Logger.Debugf("Request %s was submitted", info)

	return nil
}

func (c *Controller) OnRequestTimeout(request []byte) {
	info := c.RequestInspector.RequestID(request)
	c.Logger.Warnf("Request %s has timed out, forwarding request to leader", info)
	// TODO forward request to leader, start another timeout, update the timeout-collection
}

func (c *Controller) OnLeaderFwdRequestTimeout(request []byte) {
	info := c.RequestInspector.RequestID(request)
	c.Logger.Warnf("Request %s has timed out, complaining about leader", info)
	// TODO complain about the leader
	// TODO Q: what to do with the request?
}

// ProcessMessages dispatches the incoming message to the required component
func (c *Controller) ProcessMessages(sender uint64, m *protos.Message) {
	if IsViewMessage(m) {
		c.viewLock.RLock()
		c.currView.HandleMessage(sender, m)
		c.viewLock.RUnlock()
	}
	c.Logger.Debugf("Node %d handled message %v from %d with seq %d", c.ID, m, sender, proposalSequence(m))
	// TODO the msg can be a view change message or a tx req coming from a node after a timeout
}

func (c *Controller) startView(proposalSequence uint64) Future {
	// TODO view builder according to metadata returned by sync
	view := View{
		N:                c.N,
		LeaderID:         c.leaderID(),
		SelfID:           c.ID,
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
		State:            &PersistedState{WAL: c.WAL},
	}

	c.viewLock.Lock()
	c.currView = view
	c.Logger.Debugf("Starting view with number %d", atomic.LoadUint64(&c.currViewNumber))
	c.viewLock.Unlock()
	return c.currView.Start()
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
	end := c.startView(newProposalSequence)
	c.stopWG.Add(1)
	go func() {
		defer c.stopWG.Done()
		end.Wait()
		c.viewAbortChan <- struct{}{}
	}()
	if c.iAmTheLeader() {
		c.Logger.Debugf("Starting leader thread in view %d", atomic.LoadUint64(&c.currViewNumber))
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
			_, err := c.Verifier.VerifyRequest(req) // TODO use returned request info
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

		// TODO stop the RequestPool

		close(c.stopChan)
	}
}

// Decide delivers the decision to the application
func (c *Controller) Decide(proposal types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	// TODO write to WAL?
	c.Application.Deliver(proposal, signatures)
	c.Logger.Debugf("Node %d delivered proposal", c.ID)

	for _, reqInfo := range requests {
		if err := c.RequestPool.RemoveRequest(reqInfo); err != nil {
			c.Logger.Warnf("Error during remove of request %s from the pool : %s", reqInfo, err)
		}
		// TODO stop and remove the associated timer from the timeout-collection
	}
	if c.iAmTheLeader() {
		c.deliverChan <- struct{}{}
	}
}
