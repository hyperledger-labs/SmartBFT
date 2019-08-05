// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/consensus"
	"github.com/SmartBFT-Go/consensus/pkg/wal"
	"github.com/SmartBFT-Go/consensus/smartbftprotos"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var (
	logger *zap.SugaredLogger
)

func init() {
	basicLog, _ := zap.NewDevelopment()
	logger = basicLog.Sugar()
}

func TestBasic(t *testing.T) {
	network := make(Network)
	defer network.Shutdown()

	n1 := newNode(1, network)
	n2 := newNode(2, network)
	n3 := newNode(3, network)
	n4 := newNode(4, network)

	n1.Consensus.Start()
	n2.Consensus.Start()
	n3.Consensus.Start()
	n4.Consensus.Start()

	n1.Submit(Request{ID: "1", ClientID: "alice"})
	n1.Submit(Request{ID: "2", ClientID: "alice"})
	n1.Submit(Request{ID: "3", ClientID: "alice"})
	n1.Submit(Request{ID: "3", ClientID: "alice"})

	<-n1.Delivered
	<-n2.Delivered
	<-n3.Delivered
	<-n4.Delivered
}

func TestRestartFollowers(t *testing.T) {
	network := make(Network)
	defer network.Shutdown()

	n1 := newNode(1, network)
	n2 := newNode(2, network)
	n3 := newNode(3, network)
	n4 := newNode(4, network)

	n1.Consensus.Start()
	n2.Consensus.Start()
	n3.Consensus.Start()
	n4.Consensus.Start()

	n1.Submit(Request{ID: "1", ClientID: "alice"})

	n2.Restart()
	n3.Restart()
	n4.Restart()

	data1 := <-n1.Delivered
	data2 := <-n2.Delivered
	data3 := <-n3.Delivered
	data4 := <-n4.Delivered
	assert.Equal(t, data1, data2)
	assert.Equal(t, data3, data4)
	assert.Equal(t, data1, data4)

	n1.Submit(Request{ID: "2", ClientID: "alice"})

	data1 = <-n1.Delivered
	data2 = <-n2.Delivered
	data3 = <-n3.Delivered
	data4 = <-n4.Delivered
	assert.Equal(t, data1, data2)
	assert.Equal(t, data3, data4)
	assert.Equal(t, data1, data4)
}

func newNode(id uint64, network Network) *App {
	app := &App{
		ID:        id,
		Delivered: make(chan *Batch, 100),
	}

	app.Setup = func() {
		c := &consensus.Consensus{
			SelfID: id,
			Logger: logger,
			WAL:    &wal.EphemeralWAL{},
			N:      4,
			Metadata: smartbftprotos.ViewMetadata{
				ViewId:         1,
				LatestSequence: 0,
			},
			Verifier:         app,
			Signer:           app,
			RequestInspector: app,
			Assembler:        app,
			Synchronizer:     app,
			Application:      app,
			BatchSize:        10,
			BatchTimeout:     time.Millisecond,
		}
		network.AddOrUpdateNode(id, c)
		c.Comm = network[id]
		app.Consensus = c

	}
	app.Setup()
	return app
}
