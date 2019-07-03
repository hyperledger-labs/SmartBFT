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
	"go.uber.org/zap"
)

func TestBatcherBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &mocks.RequestInspector{}
	byteReq1 := []byte{1}
	insp.On("RequestID", byteReq1).Return(types.RequestInfo{ID: "1", ClientID: "1"})
	byteReq2 := []byte{2}
	insp.On("RequestID", byteReq2).Return(types.RequestInfo{ID: "2", ClientID: "2"})
	byteReq3 := []byte{3}
	insp.On("RequestID", byteReq3).Return(types.RequestInfo{ID: "3", ClientID: "3"})
	pool := bft.Pool{
		Log:              log,
		RequestInspector: insp,
		QueueSize:        3,
	}
	pool.Start()
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)

	batcher := bft.Bundler{
		Pool:      &pool,
		BatchSize: 1,
		Timeout:   10 * time.Millisecond,
	}

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

	batcher = bft.Bundler{
		Pool:      &pool,
		BatchSize: 2,
		Timeout:   10 * time.Millisecond,
	}

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
}

func TestBatcherWhileSubmitting(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &mocks.RequestInspector{}
	pool := bft.Pool{
		Log:              log,
		RequestInspector: insp,
		QueueSize:        200,
	}
	pool.Start()

	batcher := bft.Bundler{
		Pool:      &pool,
		BatchSize: 100,
		Timeout:   100 * time.Second, // long time
	}

	rem := make([][]byte, 0)
	for i := 0; i < 50; i++ {
		rem = append(rem, []byte{byte(i + 100)})
	}

	batcher.BatchRemainder(rem)

	go func() {
		for i := 0; i < 100; i++ {
			byteReq := []byte{byte(i)}
			str := fmt.Sprintf("%d", i)
			insp.On("RequestID", byteReq).Return(types.RequestInfo{ID: str, ClientID: str})
			err := pool.Submit(byteReq)
			assert.NoError(t, err)
		}
	}()

	res := batcher.NextBatch()
	assert.Len(t, res, 100)
	for i := 0; i < 50; i++ {
		assert.Equal(t, []byte{byte(i + 100)}, res[i]) // first rem
	}
	for i := 50; i < 100; i++ {
		assert.Equal(t, []byte{byte(i - 50)}, res[i]) // then requests
	}
}
