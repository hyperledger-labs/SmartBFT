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
	t.Parallel()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo")
	byteReq3 := makeTestRequest("3", "3", "foo")
	clock := make(chan time.Time, 1)
	clock <- time.Now()
	pool := bft.NewPool(log, insp, noopTimeoutHandler, clock, bft.PoolOptions{QueueSize: 3})
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, 1, 10*time.Millisecond)

	res := batcher.NextBatch()
	assert.Len(t, res, 1)

	batcher.BatchRemainder([][]byte{byteReq2})
	res = batcher.NextBatch()
	assert.Len(t, res, 1)

	err = pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"})
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 0) // after timeout

	err = pool.Submit(byteReq2)
	assert.NoError(t, err)
	err = pool.Submit(byteReq3)
	assert.NoError(t, err)

	batcher.BatchRemainder([][]byte{byteReq1})

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq1, res[0])

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq2, res[0])

	err = pool.RemoveRequest(types.RequestInfo{ID: "2", ClientID: "2"})
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq3, res[0])

	batcher = bft.NewBatchBuilder(pool, 2, 10*time.Millisecond)

	batcher.BatchRemainder([][]byte{byteReq1})

	err = pool.Submit(byteReq2)
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 2)
	assert.Equal(t, byteReq1, res[0])
	assert.Equal(t, byteReq3, res[1])

	err = pool.RemoveRequest(types.RequestInfo{ID: "3", ClientID: "3"})
	assert.NoError(t, err)

	res = batcher.NextBatch()
	assert.Len(t, res, 1) // after timeout
	assert.Equal(t, byteReq2, res[0])
	pool.Close()
}

func TestBatcherWhileSubmitting(t *testing.T) {
	t.Parallel()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	clock := make(chan time.Time, 1)
	clock <- time.Now()
	pool := bft.NewPool(log, insp, noopTimeoutHandler, clock, bft.PoolOptions{QueueSize: 200})

	batcher := bft.NewBatchBuilder(pool, 100, 100*time.Second) // long time

	rem := make([][]byte, 0)
	for i := 0; i < 50; i++ {
		iStr := fmt.Sprintf("%d", 100+i)
		rem = append(rem, makeTestRequest(iStr, iStr, "bar"))
	}

	batcher.BatchRemainder(rem)

	go func() {
		for i := 0; i < 100; i++ {
			iStr := fmt.Sprintf("%d", i)
			byteReq := makeTestRequest(iStr, iStr, "foo")
			err := pool.Submit(byteReq)
			assert.NoError(t, err)
		}
	}()

	res := batcher.NextBatch()
	assert.Len(t, res, 100)
	for i := 0; i < 50; i++ {
		iStr := fmt.Sprintf("%d", 100+i)
		assert.Equal(t, iStr, insp.RequestID(res[i]).ID) // first rem
	}

	for i := 50; i < 100; i++ {
		iStr := fmt.Sprintf("%d", i-50)
		assert.Equal(t, iStr, insp.RequestID(res[i]).ID) // then requests
	}
	pool.Close()
}

func TestBatcherClose(t *testing.T) {
	t.Parallel()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	byteReq := makeTestRequest("1", "1", "foo")
	clock := make(chan time.Time, 1)
	clock <- time.Now()
	pool := bft.NewPool(log, insp, noopTimeoutHandler, clock, bft.PoolOptions{QueueSize: 3})
	err = pool.Submit(byteReq)
	assert.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, 100, time.Minute)

	go func() {
		batcher.Close()
	}()

	t1 := time.Now()
	res := batcher.NextBatch()
	assert.Nil(t, res)
	assert.True(t, time.Since(t1) < time.Second*50)
	pool.Close()
}

func TestBatcherPopReminder(t *testing.T) {
	batcher := bft.BatchBuilder{}
	batcher.BatchRemainder([][]byte{{1, 2, 3}})

	rem := batcher.PopRemainder()
	assert.Equal(t, [][]byte{{1, 2, 3}}, rem)

	rem = batcher.PopRemainder()
	assert.Nil(t, rem)
}

func TestBatcherReset(t *testing.T) {
	t.Parallel()
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	clock := make(chan time.Time, 1)
	pool := bft.NewPool(log, insp, noopTimeoutHandler, clock, bft.PoolOptions{QueueSize: 3})
	clock <- time.Now()
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)

	batcher := bft.NewBatchBuilder(pool, 1, 10*time.Millisecond)

	res := batcher.NextBatch()
	assert.Len(t, res, 1)

	byteReq2 := makeTestRequest("2", "2", "foo")
	batcher.BatchRemainder([][]byte{byteReq2})
	res = batcher.NextBatch()
	assert.Len(t, res, 1)

	batcher.BatchRemainder([][]byte{byteReq2})

	batcher.Close()
	batcher.Reset()

	res = batcher.NextBatch()
	assert.Len(t, res, 1)
	assert.Equal(t, byteReq1, res[0])

	err = pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"})
	assert.NoError(t, err)

	batcher.Close()
	batcher.Reset()

	res = batcher.NextBatch()
	assert.Len(t, res, 0)
	pool.Close()
}
