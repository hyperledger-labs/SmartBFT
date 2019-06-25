// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestReqPoolBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &mocks.RequestInspector{}
	byteReq1 := []byte{1}
	insp.On("RequestID", byteReq1).Return(types.RequestInfo{ID: "1", ClientID: "1"})
	pool := bft.Pool{
		Log:              log,
		RequestInspector: insp,
		QueueSize:        3,
	}
	pool.Start()
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	req1 := bft.Request{
		ID:       "1",
		ClientID: "1",
		Request:  byteReq1,
	}
	err = pool.Submit(byteReq1)
	assert.Error(t, err)
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)

	byteReq2 := []byte{2}
	insp.On("RequestID", byteReq2).Return(types.RequestInfo{ID: "2", ClientID: "2"})
	err = pool.Submit(byteReq2)
	assert.NoError(t, err)
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	err = pool.Submit(byteReq1)
	assert.Error(t, err)
	err = pool.Submit(byteReq2)
	assert.Error(t, err)
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	req2 := bft.Request{
		ID:       "2",
		ClientID: "2",
		Request:  byteReq2,
	}
	err = pool.RemoveRequest(req2)
	assert.NoError(t, err)
	err = pool.Submit(byteReq2)
	assert.NoError(t, err)

	byteReq3 := []byte{3}
	insp.On("RequestID", byteReq3).Return(types.RequestInfo{ID: "3", ClientID: "3"})
	err = pool.Submit(byteReq3)
	assert.NoError(t, err)

	next := pool.NextRequests(4)
	assert.Equal(t, "1", next[0].ID)
	assert.Equal(t, "2", next[1].ID)
	assert.Equal(t, "3", next[2].ID)
	assert.Len(t, next, 3)

	err = pool.RemoveRequest(req2)
	assert.NoError(t, err)

	next = pool.NextRequests(4)
	assert.Equal(t, "1", next[0].ID)
	assert.Equal(t, "3", next[1].ID)
	assert.Len(t, next, 2)

	next = pool.NextRequests(1)
	assert.Equal(t, "1", next[0].ID)
	assert.Len(t, next, 1)

	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)

	req3 := bft.Request{
		ID:       "3",
		ClientID: "3",
		Request:  byteReq3,
	}

	err = pool.RemoveRequest(req3)
	assert.NoError(t, err)

	next = pool.NextRequests(1)
	assert.Len(t, next, 0)

}

func TestEventuallySubmit(t *testing.T) {
	n := 100
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &mocks.RequestInspector{}
	pool := bft.Pool{
		Log:              log,
		RequestInspector: insp,
		QueueSize:        50,
	}
	pool.Start()
	wg := sync.WaitGroup{}
	wg.Add(2 * n)
	for i := 0; i < n; i++ {
		go func(i int) {
			byteReq := []byte{byte(i)}
			str := fmt.Sprintf("%d", i)
			insp.On("RequestID", byteReq).Return(types.RequestInfo{ID: str, ClientID: str})
			err := pool.Submit(byteReq)
			assert.NoError(t, err)
			wg.Done()
		}(i)
		go func(i int) {
			byteReq := []byte{byte(i)}
			str := fmt.Sprintf("%d", i)
			req := bft.Request{
				ID:       str,
				ClientID: str,
				Request:  byteReq,
			}
			err := pool.RemoveRequest(req)
			for err != nil {
				err = pool.RemoveRequest(req)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
