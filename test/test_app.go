// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"encoding/asn1"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var fastConfig = types.Configuration{
	RequestBatchMaxCount:          10,
	RequestBatchMaxBytes:          10 * 1024 * 1024,
	RequestBatchMaxInterval:       10 * time.Millisecond,
	IncomingMessageBufferSize:     200,
	RequestPoolSize:               40,
	RequestForwardTimeout:         500 * time.Millisecond,
	RequestComplainTimeout:        2 * time.Second,
	RequestAutoRemoveTimeout:      3 * time.Minute,
	ViewChangeResendInterval:      5 * time.Second,
	ViewChangeTimeout:             1 * time.Minute,
	LeaderHeartbeatTimeout:        1 * time.Minute,
	LeaderHeartbeatCount:          10,
	NumOfTicksBehindBeforeSyncing: 10,
	CollectTimeout:                200 * time.Millisecond,
}

// App implements all interfaces required by an application using this library
type App struct {
	ID             uint64
	Delivered      chan *AppRecord
	Consensus      *consensus.Consensus
	Setup          func()
	Node           *Node
	logLevel       zap.AtomicLevel
	latestMD       *smartbftprotos.ViewMetadata
	lastDecision   *types.Decision
	clock          *time.Ticker
	heartbeatTime  chan time.Time
	viewChangeTime chan time.Time
	secondClock    *time.Ticker
	logger         *zap.SugaredLogger
}

// Mute mutes the log
func (a *App) Mute() {
	a.logLevel.SetLevel(zapcore.PanicLevel)
}

// UnMute unmutes the log
func (a *App) UnMute() {
	a.logLevel.SetLevel(zapcore.DebugLevel)
}

// Submit submits the client request
func (a *App) Submit(req Request) {
	a.Consensus.SubmitRequest(req.ToBytes())
}

// Sync synchronizes and returns the latest decision
func (a *App) Sync() types.Decision {
	records := a.Node.cb.readAll(*a.latestMD)
	for _, record := range records {
		proposal := types.Proposal{
			Payload:  record.Batch.toBytes(),
			Metadata: record.Metadata,
		}
		a.Deliver(proposal, nil)
	}
	return *a.lastDecision
}

// Restart restarts the node
func (a *App) Restart() {
	a.Consensus.Stop()
	a.Node.Lock()
	defer a.Node.Unlock()
	a.Setup()
	if err := a.Consensus.Start(); err != nil {
		a.logger.Panicf("Consensus start returned an error : %v", err)
	}
}

// Disconnect disconnects the node from the network
func (a *App) Disconnect() {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.lossProbability = 1
}

// DisconnectFrom disconnects the node from a specific node
func (a *App) DisconnectFrom(target uint64) {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.peerLossProbability[target] = 1.0
}

// ConnectTo connects the node to a specific node
func (a *App) ConnectTo(target uint64) {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	delete(a.Node.peerLossProbability, target)
}

// Connect connects the node to the network
func (a *App) Connect() {
	a.Node.probabilityLock.Lock()
	defer a.Node.probabilityLock.Unlock()
	a.Node.lossProbability = 0
}

// MutateSend set the mutating function to be called before sending a message to the target node
func (a *App) MutateSend(target uint64, mutating func(uint64, *smartbftprotos.Message)) {
	a.Node.mutatingFuncLock.Lock()
	defer a.Node.mutatingFuncLock.Unlock()
	a.Node.peerMutatingFunc[target] = mutating
}

// ClearMutateSend clears any mutating function called before sending a message to the target node
func (a *App) ClearMutateSend(target uint64) {
	a.Node.mutatingFuncLock.Lock()
	defer a.Node.mutatingFuncLock.Unlock()
	delete(a.Node.peerMutatingFunc, target)
}

// RequestID returns info about the given request
func (a *App) RequestID(req []byte) types.RequestInfo {
	txn := requestFromBytes(req)
	return types.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

// VerifyProposal verifies the given proposal and returns the included requests
func (a *App) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	blockData := batchFromBytes(proposal.Payload)
	requests := make([]types.RequestInfo, 0)
	for _, t := range blockData.Requests {
		req := requestFromBytes(t)
		reqInfo := types.RequestInfo{ID: req.ID, ClientID: req.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

// VerifyRequest verifies the given request and returns its info
func (a *App) VerifyRequest(val []byte) (types.RequestInfo, error) {
	req := requestFromBytes(val)
	return types.RequestInfo{ID: req.ID, ClientID: req.ClientID}, nil
}

// VerifyConsenterSig verifies a nodes signature on the given proposal
func (a *App) VerifyConsenterSig(signature types.Signature, prop types.Proposal) error {
	return nil
}

// VerifySignature verifies a signature
func (a *App) VerifySignature(signature types.Signature) error {
	return nil
}

// VerificationSequence returns the current verification sequence
func (a *App) VerificationSequence() uint64 {
	return 0
}

// Sign signs on the given value
func (a *App) Sign([]byte) []byte {
	return nil
}

// SignProposal signs on the given proposal
func (a *App) SignProposal(types.Proposal) *types.Signature {
	return &types.Signature{ID: a.ID}
}

// AssembleProposal assembles a new proposal from the given requests
func (a *App) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	return types.Proposal{
		Payload:  batch{Requests: requests}.toBytes(),
		Metadata: metadata,
	}
}

// Deliver delivers the given proposal
func (a *App) Deliver(proposal types.Proposal, signatures []types.Signature) types.Reconfig {
	record := &AppRecord{
		Metadata: proposal.Metadata,
		Batch:    batchFromBytes(proposal.Payload),
	}
	a.Node.cb.add(record)
	a.lastDecision = &types.Decision{
		Proposal:   proposal,
		Signatures: signatures,
	}
	a.latestMD = &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(proposal.Metadata, a.latestMD); err != nil {
		panic(err)
	}
	a.Delivered <- record

	for _, req := range record.Batch.Requests {
		request := requestFromBytes(req)
		if request.Reconfig.InLatestDecision {
			reconfig := request.Reconfig.recconfigToUint(a.ID)
			return types.Reconfig{InLatestDecision: true, CurrentNodes: reconfig.CurrentNodes, CurrentConfig: reconfig.CurrentConfig}
		}
	}

	return types.Reconfig{InLatestDecision: false}
}

type committedBatches struct {
	lock     sync.RWMutex
	latestMD smartbftprotos.ViewMetadata
	records  []*AppRecord
}

func (cb *committedBatches) add(record *AppRecord) {
	cb.lock.Lock()
	defer cb.lock.Unlock()

	md := &smartbftprotos.ViewMetadata{}
	if err := proto.Unmarshal(record.Metadata, md); err != nil {
		panic(err)
	}

	if cb.latestMD.ViewId > md.ViewId {
		return
	}
	if cb.latestMD.LatestSequence >= md.LatestSequence {
		return
	}
	cb.latestMD = *md
	cb.records = append(cb.records, record)
}

func (cb *committedBatches) readAll(from smartbftprotos.ViewMetadata) []*AppRecord {
	cb.lock.RLock()
	defer cb.lock.RUnlock()

	var res []*AppRecord

	for _, entry := range cb.records {
		md := &smartbftprotos.ViewMetadata{}
		if err := proto.Unmarshal(entry.Metadata, md); err != nil {
			panic(err)
		}
		if md.ViewId < from.ViewId || md.LatestSequence <= from.LatestSequence {
			continue
		}
		res = append(res, &AppRecord{
			Metadata: entry.Metadata,
			Batch:    entry.Batch,
		})
	}
	return res
}

// Request represents a client's request
type Request struct {
	ClientID string
	ID       string
	Reconfig ReconfigInt
}

// ToBytes returns a byte array representation of the request
func (txn Request) ToBytes() []byte {
	rawTxn, err := asn1.Marshal(txn)
	if err != nil {
		panic(err)
	}
	return rawTxn
}

func requestFromBytes(req []byte) *Request {
	var r Request
	asn1.Unmarshal(req, &r)
	return &r
}

type batch struct {
	Requests [][]byte
}

func (b batch) toBytes() []byte {
	rawBlock, err := asn1.Marshal(b)
	if err != nil {
		panic(err)
	}
	return rawBlock
}

func batchFromBytes(rawBlock []byte) *batch {
	var block batch
	asn1.Unmarshal(rawBlock, &block)
	return &block
}

// AppRecord represents a committed batch and metadata
type AppRecord struct {
	Batch    *batch
	Metadata []byte
}

func newNode(id uint64, network Network, testName string, testDir string) *App {
	logConfig := zap.NewDevelopmentConfig()
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", testName)).With(zap.Int64("id", int64(id)))
	sugaredLogger := logger.Sugar()

	app := &App{
		clock:        time.NewTicker(time.Second),
		secondClock:  time.NewTicker(time.Second),
		ID:           id,
		Delivered:    make(chan *AppRecord, 100),
		logLevel:     logConfig.Level,
		latestMD:     &smartbftprotos.ViewMetadata{},
		lastDecision: &types.Decision{},
		logger:       sugaredLogger,
	}

	config := fastConfig
	config.SelfID = id
	config.SyncOnStart = true

	app.Setup = func() {
		writeAheadLog, walInitialEntries, err := wal.InitializeAndReadAll(app.logger, filepath.Join(testDir, fmt.Sprintf("node%d", id)), nil)
		if err != nil {
			sugaredLogger.Panicf("Failed to initialize WAL: %s", err)
		}
		c := &consensus.Consensus{
			Config:            config,
			ViewChangerTicker: app.secondClock.C,
			Scheduler:         app.clock.C,
			Logger:            app.logger,
			WAL:               writeAheadLog,
			Metadata:          *app.latestMD,
			Verifier:          app,
			Signer:            app,
			RequestInspector:  app,
			Assembler:         app,
			Synchronizer:      app,
			Application:       app,
			WALInitialContent: walInitialEntries,
			LastProposal:      types.Proposal{},
			LastSignatures:    []types.Signature{},
		}
		if app.heartbeatTime != nil {
			app.clock.Stop()
			c.Scheduler = app.heartbeatTime
		}
		if app.viewChangeTime != nil {
			app.secondClock.Stop()
			c.ViewChangerTicker = app.viewChangeTime
		}
		network.AddOrUpdateNode(id, c, app)
		c.Comm = network[id]
		app.Consensus = c
	}
	app.Setup()
	app.Node = network[id]
	return app
}
