// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

var (
	noopTimeoutHandler = &mocks.RequestTimeoutHandler{}
)

func init() {
	noopTimeoutHandler.On("OnRequestTimeout", mock.Anything, mock.Anything)
	noopTimeoutHandler.On("OnLeaderFwdRequestTimeout", mock.Anything, mock.Anything)
}

func TestBatcherBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	submittedChan := make(chan struct{}, 1)

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo-bar")
	byteReq3 := makeTestRequest("3", "3", "foo-bar-foo")
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3}, submittedChan)
	err = pool.Submit(byteReq1) // pool: [req1]
	assert.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 1, 2048, 10*time.Millisecond)

	res := batcher.NextBatch()
	assert.Len(t, res, 1)

	res = batcher.NextBatch()
	assert.Len(t, res, 1)

	err = pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"}) // pool: []
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 0) // after timeout

	err = pool.Submit(byteReq2)
	assert.NoError(t, err)
	err = pool.Submit(byteReq3) // pool: [req3, req2]
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	err = pool.RemoveRequest(types.RequestInfo{ID: "2", ClientID: "2"}) // pool: [req3]
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq3, res[0])

	// count limit
	batcher = bft.NewBatchBuilder(pool, submittedChan, 2, 2048, 10*time.Millisecond)

	err = pool.Submit(byteReq2) // pool: [req2, req3]
	assert.NoError(t, err)
	err = pool.Submit(byteReq1) // pool: [req1, req2, req3]
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 2)
	assert.Equal(t, byteReq3, res[0])
	assert.Equal(t, byteReq2, res[1])

	// size limit
	batcher = bft.NewBatchBuilder(pool, submittedChan, 10, uint64(len(byteReq3)+len(byteReq2)), 10*time.Millisecond)

	res = batcher.NextBatch()
	assert.Len(t, res, 2)
	assert.Equal(t, byteReq3, res[0])
	assert.Equal(t, byteReq2, res[1])

	// high limits
	batcher = bft.NewBatchBuilder(pool, submittedChan, 10, 2048, 10*time.Millisecond)
	res = batcher.NextBatch()
	assert.Len(t, res, 3)
	assert.Equal(t, byteReq3, res[0])
	assert.Equal(t, byteReq2, res[1])
	assert.Equal(t, byteReq1, res[2])

	pool.Close()
}

func TestBatcherWhileSubmitting(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	submittedChan := make(chan struct{}, 1)
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 200}, submittedChan)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 100, 5000, 100*time.Second)

	go func() {
		for i := 0; i < 300; i++ {
			iStr := fmt.Sprintf("%d", i)
			byteReq := makeTestRequest(iStr, iStr, "foo")
			err := pool.Submit(byteReq)
			assert.NoError(t, err)
		}
	}()

	res := batcher.NextBatch()
	assert.Len(t, res, 100)
	for i := 0; i < 100; i++ {
		iStr := fmt.Sprintf("%d", i)
		reqInfo := insp.RequestID(res[i])
		assert.Equal(t, iStr, reqInfo.ID)
		err := pool.RemoveRequest(reqInfo)
		assert.NoError(t, err)
	}

	res = batcher.NextBatch()
	assert.Len(t, res, 100)
	for i := 0; i < 100; i++ {
		iStr := fmt.Sprintf("%d", i+100)
		reqInfo := insp.RequestID(res[i])
		assert.Equal(t, iStr, reqInfo.ID)
		err := pool.RemoveRequest(reqInfo)
		assert.NoError(t, err)
	}

	res = batcher.NextBatch()
	assert.Len(t, res, 100)
	for i := 0; i < 100; i++ {
		iStr := fmt.Sprintf("%d", i+200)
		reqInfo := insp.RequestID(res[i])
		assert.Equal(t, iStr, reqInfo.ID)
		err := pool.RemoveRequest(reqInfo)
		assert.NoError(t, err)
	}

	pool.Close()
}

func TestBatcherClose(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	submittedChan := make(chan struct{}, 1)
	byteReq := makeTestRequest("1", "1", "foo")
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3}, submittedChan)
	err = pool.Submit(byteReq)
	assert.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 100, 2048, time.Minute)

	go func() {
		batcher.Close()
	}()

	t1 := time.Now()
	res := batcher.NextBatch()
	assert.Nil(t, res)
	assert.True(t, time.Since(t1) < time.Second*50)
	pool.Close()
}

func TestBatcherReset(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	submittedChan := make(chan struct{}, 1)
	byteReq1 := makeTestRequest("1", "1", "foo")
	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3}, submittedChan)
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, submittedChan, 1, 2048, 10*time.Millisecond)

	res := batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq1, res[0])

	byteReq2 := makeTestRequest("2", "2", "foo")
	err = pool.Submit(byteReq2)
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq1, res[0])

	batcher.Close()
	batcher.Reset()

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq1, res[0])

	err = pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"})
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	batcher.Close()
	batcher.Reset()

	err = pool.RemoveRequest(types.RequestInfo{ID: "2", ClientID: "2"})
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 0)
	pool.Close()
}
