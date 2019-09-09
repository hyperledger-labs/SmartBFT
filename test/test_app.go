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

var fastConfig = consensus.Configuration{
	RequestBatchMaxSize:       10,
	RequestBatchMaxInterval:   time.Millisecond,
	IncomingMessageBufferSize: 200,
	RequestPoolSize:           40,
	RequestTimeout:            500 * time.Millisecond,
	RequestLeaderFwdTimeout:   2 * time.Second,
	RequestAutoRemoveTimeout:  3 * time.Minute,
	ViewChangeResendInterval:  time.Second,
	ViewChangeTimeout:         1 * time.Minute,
	LeaderHeartbeatTimeout:    1 * time.Minute,
	LeaderHeartbeatCount:      10,
}

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
}

func (a *App) Mute() {
	a.logLevel.SetLevel(zapcore.PanicLevel)
}

func (a *App) UnMute() {
	a.logLevel.SetLevel(zapcore.DebugLevel)
}

func (a *App) Submit(req Request) {
	a.Consensus.SubmitRequest(req.ToBytes())
}

func (a *App) Sync() types.Decision {
	records := a.Node.cb.readAll(*a.latestMD)
	for _, record := range records {
		proposal := types.Proposal{
			Payload:  record.Batch.ToBytes(),
			Metadata: record.Metadata,
		}
		a.Deliver(proposal, nil)
	}
	return *a.lastDecision
}

func (a *App) Restart() {
	a.Consensus.Stop()
	a.Node.Lock()
	defer a.Node.Unlock()
	a.Setup()
	a.Consensus.Start()
}

func (a *App) Disconnect() {
	a.Node.Lock()
	defer a.Node.Unlock()
	a.Node.lossProbability = 1
}

func (a *App) DisconnectFrom(target uint64) {
	a.Node.Lock()
	defer a.Node.Unlock()
	a.Node.peerLossProbability[target] = 1.0
}

func (a *App) ConnectTo(target uint64) {
	a.Node.Lock()
	defer a.Node.Unlock()
	delete(a.Node.peerLossProbability, target)
}

func (a *App) Connect() {
	a.Node.Lock()
	defer a.Node.Unlock()
	a.Node.lossProbability = 0
}

func (a *App) RequestID(req []byte) types.RequestInfo {
	txn := requestFromBytes(req)
	return types.RequestInfo{
		ClientID: txn.ClientID,
		ID:       txn.ID,
	}
}

func (a *App) VerifyProposal(proposal types.Proposal) ([]types.RequestInfo, error) {
	blockData := BatchFromBytes(proposal.Payload)
	requests := make([]types.RequestInfo, 0)
	for _, t := range blockData.Requests {
		req := requestFromBytes(t)
		reqInfo := types.RequestInfo{ID: req.ID, ClientID: req.ClientID}
		requests = append(requests, reqInfo)
	}
	return requests, nil
}

func (a *App) VerifyRequest(val []byte) (types.RequestInfo, error) {
	req := requestFromBytes(val)
	return types.RequestInfo{ID: req.ID, ClientID: req.ClientID}, nil
}

func (a *App) VerifyConsenterSig(signature types.Signature, prop types.Proposal) error {
	return nil
}

func (a *App) VerifySignature(signature types.Signature) error {
	return nil
}

func (a *App) VerificationSequence() uint64 {
	return 0
}

func (a *App) Sign([]byte) []byte {
	return nil
}

func (a *App) SignProposal(types.Proposal) *types.Signature {
	return &types.Signature{Id: a.ID}
}

func (a *App) AssembleProposal(metadata []byte, requests [][]byte) (nextProp types.Proposal, remainder [][]byte) {
	return types.Proposal{
		Payload:  Batch{Requests: requests}.ToBytes(),
		Metadata: metadata,
	}, nil
}

func (a *App) Deliver(proposal types.Proposal, signatures []types.Signature) {
	record := &AppRecord{
		Metadata: proposal.Metadata,
		Batch:    BatchFromBytes(proposal.Payload),
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

type Request struct {
	ClientID string
	ID       string
}

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

type Batch struct {
	Requests [][]byte
}

func (b Batch) ToBytes() []byte {
	rawBlock, err := asn1.Marshal(b)
	if err != nil {
		panic(err)
	}
	return rawBlock
}

func BatchFromBytes(rawBlock []byte) *Batch {
	var block Batch
	asn1.Unmarshal(rawBlock, &block)
	return &block
}

type AppRecord struct {
	Batch    *Batch
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
	}

	writeAheadLog, walInitialEntries, err := wal.InitializeAndReadAll(sugaredLogger, filepath.Join(testDir, fmt.Sprintf("node%d", id)), nil)
	if err != nil {
		sugaredLogger.Panicf("Failed to initialize WAL: %s", err)
	}

	config := fastConfig
	config.SelfID = id

	app.Setup = func() {
		c := &consensus.Consensus{
			Config:            config,
			ViewChangerTicker: app.secondClock.C,
			Scheduler:         app.clock.C,
			Logger:            sugaredLogger,
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
