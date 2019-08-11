// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
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
	PopRemainder() [][]byte
	Close()
	Reset()
}

//go:generate mockery -dir . -name RequestPool -case underscore -output ./mocks/

type RequestPool interface {
	Prune(predicate func([]byte) error)
	Submit(request []byte) error
	Size() int
	NextRequests(n int) [][]byte
	RemoveRequest(request types.RequestInfo) error
	StopTimers()
	RestartTimers()
	Close()
}

//go:generate mockery -dir . -name LeaderMonitor -case underscore -output ./mocks/

type LeaderMonitor interface {
	ChangeRole(role Role, view uint64, leaderID uint64)
	ProcessMsg(sender uint64, msg *protos.Message)
	Close()
}

type Proposer interface {
	Propose(proposal types.Proposal)
	Start()
	Abort()
	GetMetadata() []byte
	HandleMessage(sender uint64, m *protos.Message)
}

//go:generate mockery -dir . -name ProposerBuilder -case underscore -output ./mocks/
type ProposerBuilder interface {
	NewProposer(leader, proposalSequence, viewNum uint64, quorumSize int) Proposer
}

type Controller struct {
	// configuration
	ID               uint64
	N                uint64
	RequestPool      RequestPool
	RequestTimeout   time.Duration
	Batcher          Batcher
	LeaderMonitor    LeaderMonitor
	Verifier         api.Verifier
	Logger           api.Logger
	Assembler        api.Assembler
	Application      api.Application
	FailureDetector  FailureDetector
	Synchronizer     api.Synchronizer
	Comm             Comm
	Signer           api.Signer
	RequestInspector api.RequestInspector
	WAL              api.WriteAheadLog
	ProposerBuilder  ProposerBuilder
	Checkpoint       types.Checkpoint
	ViewChanger      ViewChanger

	quorum int

	currView Proposer

	currViewNumberLock sync.RWMutex
	currViewNumber     uint64

	viewChange chan viewInfo

	stopOnce sync.Once
	stopChan chan struct{}

	decisionChan         chan decision
	deliverChan          chan struct{}
	leaderToken          chan struct{}
	verificationSequence uint64

	controllerDone sync.WaitGroup
}

func (c *Controller) getCurrentViewNumber() uint64 {
	c.currViewNumberLock.RLock()
	defer c.currViewNumberLock.RUnlock()

	return c.currViewNumber
}

func (c *Controller) setCurrentViewNumber(viewNumber uint64) {
	c.currViewNumberLock.Lock()
	defer c.currViewNumberLock.Unlock()

	c.currViewNumber = viewNumber
}

// thread safe
func (c *Controller) iAmTheLeader() (bool, uint64) {
	leader := c.leaderID()
	return leader == c.ID, leader
}

// thread safe
func (c *Controller) leaderID() uint64 {
	nodes := c.Comm.Nodes()
	return getLeaderID(c.getCurrentViewNumber(), c.N, nodes)
}

func (c *Controller) HandleRequest(sender uint64, req []byte) {
	iAm, leaderID := c.iAmTheLeader()
	if !iAm {
		c.Logger.Warnf("Got request from %d but the leader is %d, dropping request", sender, leaderID)
		return
	}
	reqInfo, err := c.Verifier.VerifyRequest(req)
	if err != nil {
		c.Logger.Warnf("Got bad request from %d: %v", sender, err)
		return
	}
	c.Logger.Debugf("Got request from %d", sender)
	c.addRequest(reqInfo, req)
}

// SubmitRequest Submits a request to go through consensus.
func (c *Controller) SubmitRequest(request []byte) error {
	info := c.RequestInspector.RequestID(request)
	return c.addRequest(info, request)
}

func (c *Controller) addRequest(info types.RequestInfo, request []byte) error {
	err := c.RequestPool.Submit(request)
	if err != nil {
		c.Logger.Warnf("Request %s was not submitted, error: %s", info, err)
		return err
	}

	c.Logger.Debugf("Request %s was submitted", info)

	return nil
}

// OnRequestTimeout is called when request-timeout expires and forwards the request to leader.
// Called by the request-pool timeout goroutine. Upon return, the leader-forward timeout is started.
func (c *Controller) OnRequestTimeout(request []byte, info types.RequestInfo) {
	iAm, leaderID := c.iAmTheLeader()
	if iAm {
		c.Logger.Warnf("Request %s timeout expired, this node is the leader, nothing to do", info)
		return
	}

	c.Logger.Warnf("Request %s timeout expired, forwarding request to leader: %d", info, leaderID)
	c.Comm.SendTransaction(leaderID, request)

	return
}

// OnLeaderFwdRequestTimeout is called when the leader-forward timeout expires, and complains about the leader.
// Called by the request-pool timeout goroutine. Upon return, the auto-remove timeout is started.
func (c *Controller) OnLeaderFwdRequestTimeout(request []byte, info types.RequestInfo) {
	iAm, leaderID := c.iAmTheLeader()
	if iAm {
		c.Logger.Warnf("Request %s leader-forwarding timeout expired, this node is the leader, nothing to do", info)
		return
	}

	c.Logger.Warnf("Request %s leader-forwarding timeout expired, complaining about leader: %d", info, leaderID)
	c.FailureDetector.Complain()

	return
}

// OnAutoRemoveTimeout is called when the auto-remove timeout expires.
// Called by the request-pool timeout goroutine.
func (c *Controller) OnAutoRemoveTimeout(requestInfo types.RequestInfo) {
	c.Logger.Warnf("Request %s auto-remove timeout expired, removed from the request pool", requestInfo)
}

// OnHeartbeatTimeout is called when the heartbeat timeout expires.
// Called by the HeartbeatMonitor timer goroutine.
func (c *Controller) OnHeartbeatTimeout(view uint64, leaderID uint64) {
	c.Logger.Debugf("Heartbeat timeout expired, reported-view: %d, reported-leader: %d", view, leaderID)

	iAm, currentLeaderID := c.iAmTheLeader()
	if iAm {
		c.Logger.Debugf("Heartbeat timeout expired, this node is the leader, nothing to do; current-view: %d, current-leader: %d",
			c.getCurrentViewNumber(), currentLeaderID)
		return
	}

	if leaderID != currentLeaderID {
		c.Logger.Warnf("Heartbeat timeout expired, but current leader: %d, differs from reported leader: %d; ignoring", currentLeaderID, leaderID)
		return
	}

	c.Logger.Warnf("Heartbeat timeout expired, complaining about leader: %d", leaderID)
	c.FailureDetector.Complain()
}

// ProcessMessages dispatches the incoming message to the required component
func (c *Controller) ProcessMessages(sender uint64, m *protos.Message) {
	switch m.GetContent().(type) {
	case *protos.Message_PrePrepare, *protos.Message_Prepare, *protos.Message_Commit:
		c.currView.HandleMessage(sender, m)
		c.Logger.Debugf("Node %d handled message %v from %d with seq %d", c.ID, m, sender, proposalSequence(m))

	case *protos.Message_ViewChange, *protos.Message_ViewData, *protos.Message_NewView:
		c.ViewChanger.HandleMessage(sender, m)
		c.Logger.Debugf("View change not yet implemented, ignoring message: %v, from %d", m, sender)

	case *protos.Message_HeartBeat:
		c.LeaderMonitor.ProcessMsg(sender, m)

	case *protos.Message_Error:
		c.Logger.Debugf("Error message handling not yet implemented, ignoring message: %v, from %d", m, sender)

	default:
		c.Logger.Warnf("Unexpected message type, ignoring")
	}
}

func (c *Controller) startView(proposalSequence uint64) {
	// TODO view builder according to metadata returned by sync
	c.currView = c.ProposerBuilder.NewProposer(c.leaderID(), proposalSequence, c.currViewNumber, c.quorum)
	c.currView.Start()

	role := Follower
	leader, _ := c.iAmTheLeader()
	if leader {
		role = Leader
	}
	c.LeaderMonitor.ChangeRole(role, c.currViewNumber, c.leaderID())
	c.Logger.Debugf("Starting view with number %d", c.currViewNumber)
}

func (c *Controller) changeView(newViewNumber uint64, newProposalSequence uint64) {
	// Drain the leader token in case we held it,
	// so we won't start proposing after view change.
	c.relinquishLeaderToken()

	latestView := c.getCurrentViewNumber()
	if latestView > newViewNumber {
		c.Logger.Debugf("Got view change to %d but already at %d", newViewNumber, latestView)
		return
	}
	// Kill current view
	c.Logger.Debugf("Aborting current view with number %d", latestView)
	c.currView.Abort()

	c.setCurrentViewNumber(newViewNumber)
	c.startView(newProposalSequence)

	// If I'm the leader, I can claim the leader token.
	if iAm, _ := c.iAmTheLeader(); iAm {
		c.Batcher.Reset()
		c.acquireLeaderToken()
	}
}

// ViewChanged makes the controller abort the current view and start a new one with the given numbers
func (c *Controller) ViewChanged(newViewNumber uint64, newProposalSequence uint64) {
	c.viewChange <- viewInfo{proposalSeq: newProposalSequence, viewNumber: newViewNumber}
	c.Batcher.Close()
}

func (c *Controller) getNextBatch() [][]byte {
	var validRequests [][]byte
	for len(validRequests) == 0 { // no valid requests in this batch
		requests := c.Batcher.NextBatch()
		if c.stopped() {
			return nil
		}
		for _, req := range requests {
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
	}()

	for {
		select {
		case d := <-c.decisionChan:
			c.Application.Deliver(d.proposal, d.signatures)
			c.Checkpoint.Set(d.proposal, d.signatures)
			c.Logger.Debugf("Node %d delivered proposal", c.ID)
			c.removeDeliveredFromPool(d)
			c.deliverChan <- struct{}{}
			c.maybePruneRevokedRequests()
			if iAm, _ := c.iAmTheLeader(); iAm {
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
	oldVerSqn := c.verificationSequence
	newVerSqn := c.Verifier.VerificationSequence()
	if newVerSqn == oldVerSqn {
		return
	}
	c.verificationSequence = newVerSqn

	c.Logger.Infof("Verification sequence changed: %d --> %d", oldVerSqn, newVerSqn)
	c.RequestPool.Prune(func(req []byte) error {
		_, err := c.Verifier.VerifyRequest(req)
		return err
	})

	var newRemainder [][]byte
	for _, req := range c.Batcher.PopRemainder() {
		reqInf, err := c.Verifier.VerifyRequest(req)
		if err != nil {
			c.Logger.Warnf("Revoking request %v due to %v", reqInf, err)
			continue
		}
		newRemainder = append(newRemainder, req)
	}
	c.Batcher.BatchRemainder(newRemainder)
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
func (c *Controller) Start(startViewNumber uint64, startProposalSequence uint64) {
	c.controllerDone.Add(1)
	c.stopOnce = sync.Once{}
	c.stopChan = make(chan struct{})
	c.leaderToken = make(chan struct{}, 1)
	c.decisionChan = make(chan decision)
	c.deliverChan = make(chan struct{})
	c.viewChange = make(chan viewInfo)

	Q, F := computeQuorum(c.N)
	c.Logger.Debugf("The number of nodes (N) is %d, F is %d, and the quorum size is %d", c.N, F, Q)
	c.quorum = Q

	c.currViewNumber = startViewNumber
	c.startView(startProposalSequence)
	if iAm, _ := c.iAmTheLeader(); iAm {
		c.acquireLeaderToken()
	}

	go func() {
		defer c.controllerDone.Done()
		c.run()
	}()
}

func (c *Controller) close() {
	c.stopOnce.Do(
		func() {
			select {
			case <-c.stopChan:
				return
			default:
				close(c.stopChan)
			}
		},
	)
}

// Stop the controller
func (c *Controller) Stop() {
	c.close()
	c.Batcher.Close()
	c.RequestPool.Close()
	c.LeaderMonitor.Close()

	// Drain the leader token if we hold it.
	select {
	case <-c.leaderToken:
	default:
		// Do nothing
	}

	c.controllerDone.Wait()
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
	<-c.deliverChan // wait for the delivery of the decision to the application
}

func (c *Controller) removeDeliveredFromPool(d decision) {
	for _, reqInfo := range d.requests {
		if err := c.RequestPool.RemoveRequest(reqInfo); err != nil {
			c.Logger.Warnf("Error during remove of request %s from the pool : %s", reqInfo, err)
		}
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
