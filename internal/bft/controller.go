// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math"
	"sync/atomic"
	"time"

	"sync"

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

//go:generate mockery -dir . -name RequestPool -case underscore -output ./mocks/
type RequestPool interface {
	Prune(predicate func([]byte) error)
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

	viewChange chan viewInfo
	viewEnd    Future

	stopChan             chan struct{}
	decisionChan         chan decision
	leaderToken          chan struct{}
	verificationSequence uint64
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
		c.currView.HandleMessage(sender, m)
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

	c.currView = view
	c.Logger.Debugf("Starting view with number %d", atomic.LoadUint64(&c.currViewNumber))
	return c.currView.Start()
}

func (c *Controller) changeView(newViewNumber uint64, newProposalSequence uint64) {
	// Drain the leader token in case we held it,
	// so we won't start proposing after view change.
	c.relinquishLeaderToken()

	latestView := atomic.LoadUint64(&c.currViewNumber)
	if latestView > newViewNumber {
		c.Logger.Debugf("Got view change to %d but already at %d", newViewNumber, latestView)
		return
	}
	// Kill current view
	c.Logger.Debugf("Aborting current view with number %d", atomic.LoadUint64(&c.currViewNumber))
	c.currView.Abort()

	// Wait for previous view to finish
	c.viewEnd.Wait()
	atomic.StoreUint64(&c.currViewNumber, newViewNumber)
	c.viewEnd = c.startView(newProposalSequence)

	// If I'm the leader, I can claim the leader token.
	if c.iAmTheLeader() {
		c.acquireLeaderToken()
	}
}

// ViewChanged makes the controller abort the current view and start a new one with the given numbers
func (c *Controller) ViewChanged(newViewNumber uint64, newProposalSequence uint64) {
	c.viewChange <- viewInfo{proposalSeq: newProposalSequence, viewNumber: newViewNumber}
}

func (c *Controller) getNextBatch() [][]byte {
	var validRequests [][]byte
	for len(validRequests) == 0 { // no valid requests in this batch
		requests := c.Batcher.NextBatch()
		if c.stopped() {
			return nil
		}
		for _, req := range requests {
			// TODO: We don't need to verify requests inside the req pool,
			// but we need to prune the requests in the reminder of the batcher.
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
	if len(nextBatch) == 0 {
		// If our next batch is empty,
		// it can only be because
		// the batcher is stopped and so are we.
		return
	}
	metadata := c.currView.GetMetadata()
	proposal, remainder := c.Assembler.AssembleProposal(metadata, nextBatch)
	if len(remainder) != 0 {
		c.Batcher.BatchRemainder(remainder)
	}
	c.Logger.Debugf("Leader proposing proposal: %v", proposal)
	c.currView.Propose(proposal)
}

func (c *Controller) run() {
	// At exit, always make sure to kill current view
	// and wait for it to finish.
	defer func() {
		c.Logger.Infof("Exiting")
		c.currView.Abort()
		c.viewEnd.Wait()
	}()

	for {
		select {
		case d := <-c.decisionChan:
			c.deliverToApplication(d)
			c.maybePruneRevokedRequests()
			if c.iAmTheLeader() {
				c.acquireLeaderToken()
			}
		case newView := <-c.viewChange:
			c.changeView(newView.viewNumber, newView.proposalSeq)
		case <-c.stopChan:
			return
		case <-c.leaderToken:
			c.propose()
		}
	}
}

func (c *Controller) maybePruneRevokedRequests() {
	old := c.verificationSequence
	new := c.Verifier.VerificationSequence()
	if new == old {
		return
	}
	c.Logger.Infof("Verification sequence changed: %d --> %d", old, new)
	c.RequestPool.Prune(func(req []byte) error {
		_, err := c.Verifier.VerifyRequest(req)
		return err
	})
}

func (c *Controller) acquireLeaderToken() {
	select {
	case c.leaderToken <- struct{}{}:
	default:
		// No room, seems we're already a leader.
	}
}

func (c *Controller) relinquishLeaderToken() {
	select {
	case <-c.leaderToken:
	default:

	}
}

// Start the controller
func (c *Controller) Start(startViewNumber uint64, startProposalSequence uint64) Future {
	c.stopChan = make(chan struct{})
	c.leaderToken = make(chan struct{}, 1)
	c.decisionChan = make(chan decision)
	c.viewChange = make(chan viewInfo)
	c.quorum = c.computeQuorum()
	atomic.StoreUint64(&c.currViewNumber, startViewNumber)
	c.viewEnd = c.startView(startProposalSequence)
	if c.iAmTheLeader() {
		c.acquireLeaderToken()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.run()
	}()
	return &wg
}

// Stop the controller
func (c *Controller) Stop() {
	select {
	case <-c.stopChan:
		return
	default:
		close(c.stopChan)
	}

	c.Batcher.Close()

	// Drain the leader token if we hold it.
	select {
	case <-c.leaderToken:
	default:
		// Do nothing
	}
}

func (c *Controller) stopped() bool {
	select {
	case <-c.stopChan:
		return true
	default:
		return false
	}
}

// Decide delivers the decision to the application
func (c *Controller) Decide(proposal types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	c.decisionChan <- decision{
		proposal:   proposal,
		requests:   requests,
		signatures: signatures,
	}
}

func (c *Controller) deliverToApplication(d decision) {
	c.Application.Deliver(d.proposal, d.signatures)
	c.Logger.Debugf("Node %d delivered proposal", c.ID)

	for _, reqInfo := range d.requests {
		if err := c.RequestPool.RemoveRequest(reqInfo); err != nil {
			c.Logger.Warnf("Error during remove of request %s from the pool : %s", reqInfo, err)
		}
		// TODO stop and remove the associated timer from the timeout-collection
	}
}

type viewInfo struct {
	viewNumber  uint64
	proposalSeq uint64
}

type decision struct {
	proposal   types.Proposal
	signatures []types.Signature
	requests   []types.RequestInfo
}
