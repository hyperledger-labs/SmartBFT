// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"go.uber.org/zap"

	"bytes"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReqPoolBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	insp := &testRequestInspector{}
	timeoutHandler := &mocks.RequestTimeoutHandler{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	pool := bft.NewPool(log, insp, bft.PoolOptions{QueueSize: 3, RequestTimeout: time.Hour})
	defer pool.Close()
	pool.SetTimeoutHandler(timeoutHandler)

	assert.Equal(t, 0, pool.Size())
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
	req1 := types.RequestInfo{
		ID:       "1",
		ClientID: "1",
	}
	err = pool.Submit(byteReq1)
	assert.Error(t, err)
	assert.Equal(t, 1, pool.Size())
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Size())
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	assert.Equal(t, 0, pool.Size())

	byteReq2 := makeTestRequest("2", "2", "bar")
	err = pool.Submit(byteReq2)
	assert.NoError(t, err)
	assert.Equal(t, 1, pool.Size())
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	assert.Equal(t, 2, pool.Size())
	err = pool.Submit(byteReq1)
	assert.Error(t, err)
	err = pool.Submit(byteReq2)
	assert.Error(t, err)
	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)
	err = pool.Submit(byteReq1)
	assert.NoError(t, err)
	req2 := types.RequestInfo{
		ID:       "2",
		ClientID: "2",
	}
	err = pool.RemoveRequest(req2)
	assert.NoError(t, err)
	err = pool.Submit(byteReq2)
	assert.NoError(t, err)

	byteReq3 := makeTestRequest("3", "3", "bog")
	err = pool.Submit(byteReq3)
	assert.NoError(t, err)

	next := pool.NextRequests(4)
	assert.Equal(t, "1", insp.RequestID(next[0]).ID)
	assert.Equal(t, "2", insp.RequestID(next[1]).ID)
	assert.Equal(t, "3", insp.RequestID(next[2]).ID)
	assert.Len(t, next, 3)

	err = pool.RemoveRequest(req2)
	assert.NoError(t, err)

	next = pool.NextRequests(4)
	assert.Equal(t, "1", insp.RequestID(next[0]).ID)
	assert.Equal(t, "3", insp.RequestID(next[1]).ID)
	assert.Len(t, next, 2)

	next = pool.NextRequests(1)
	assert.Equal(t, "1", insp.RequestID(next[0]).ID)
	assert.Len(t, next, 1)

	err = pool.RemoveRequest(req1)
	assert.NoError(t, err)

	req3 := types.RequestInfo{
		ID:       "3",
		ClientID: "3",
	}

	err = pool.RemoveRequest(req3)
	assert.NoError(t, err)

	next = pool.NextRequests(1)
	assert.Len(t, next, 0)

	timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
	timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
}

func TestEventuallySubmit(t *testing.T) {
	n := 100
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	timeoutHandler := &mocks.RequestTimeoutHandler{}
	pool := bft.NewPool(log, insp, bft.PoolOptions{QueueSize: 50, RequestTimeout: time.Hour})
	defer pool.Close()
	pool.SetTimeoutHandler(timeoutHandler)

	wg := sync.WaitGroup{}
	wg.Add(2 * n)
	for i := 0; i < n; i++ {
		go func(i int) {
			iStr := fmt.Sprintf("%d", i)
			byteReq := makeTestRequest(iStr, iStr, "foo")
			err := pool.Submit(byteReq)
			assert.NoError(t, err)
			wg.Done()
		}(i)

		go func(i int) {
			iStr := fmt.Sprintf("%d", i)
			req := types.RequestInfo{
				ID:       iStr,
				ClientID: iStr,
			}
			err := pool.RemoveRequest(req)
			for err != nil {
				err = pool.RemoveRequest(req)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
	timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
}

func TestReqPoolPrune(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	insp := &testRequestInspector{}
	timeoutHandler := &mocks.RequestTimeoutHandler{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "bar")
	pool := bft.NewPool(log, insp, bft.PoolOptions{QueueSize: 3, RequestTimeout: time.Hour})
	defer pool.Close()
	pool.SetTimeoutHandler(timeoutHandler)

	assert.Equal(t, 0, pool.Size())

	err = pool.Submit(byteReq1)
	err = pool.Submit(byteReq2)
	assert.NoError(t, err)
	assert.Equal(t, 2, pool.Size())

	pool.Prune(func(payload []byte) error {
		if bytes.Equal(byteReq1, payload) {
			return errors.New("revoked")
		}
		return nil
	})

	assert.Equal(t, 1, pool.Size())
	client, tx, _ := parseTestRequest(pool.NextRequests(1)[0])
	assert.Equal(t, "2", client)
	assert.Equal(t, "2", tx)

	timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
	timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
}

func TestReqPoolTimeout(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar()

	byteReq1 := makeTestRequest("1", "1", "foo")
	insp := &testRequestInspector{}

	t.Run("request timeout", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		toWG := &sync.WaitGroup{}
		toWG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			toWG.Done()
		}).Return()

		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			assert.Fail(t, "called OnLeaderFwdRequestTimeout")
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			assert.Fail(t, "called OnAutoRemoveTimeout")
		}).Return()

		pool := bft.NewPool(log, insp,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    10 * time.Millisecond,
				LeaderFwdTimeout:  time.Hour,
				AutoRemoveTimeout: time.Hour,
			},
		)
		defer pool.Close()
		pool.SetTimeoutHandler(timeoutHandler)

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())

		toWG.Wait()

		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 0)

		err := pool.RemoveRequest(insp.RequestID(byteReq1))
		assert.NoError(t, err)
	})

	t.Run("leader fwd timeout", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		to1WG := &sync.WaitGroup{}
		to1WG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to1WG.Done()
		}).Return()

		to2WG := &sync.WaitGroup{}
		to2WG.Add(1)
		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to2WG.Done()
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			assert.Fail(t, "called OnAutoRemoveTimeout")
		}).Return()

		pool := bft.NewPool(log, insp,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    10 * time.Millisecond,
				LeaderFwdTimeout:  10 * time.Millisecond,
				AutoRemoveTimeout: time.Hour,
			},
		)
		defer pool.Close()
		pool.SetTimeoutHandler(timeoutHandler)

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())

		to1WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		to2WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 1)
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 0)

		err := pool.RemoveRequest(insp.RequestID(byteReq1))
		assert.NoError(t, err)
	})

	t.Run("auto remove", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		to1WG := &sync.WaitGroup{}
		to1WG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to1WG.Done()
		}).Return()

		to2WG := &sync.WaitGroup{}
		to2WG.Add(1)
		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to2WG.Done()
		}).Return()

		to3WG := &sync.WaitGroup{}
		to3WG.Add(1)
		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to3WG.Done()
		}).Return()

		pool := bft.NewPool(log, insp,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    10 * time.Millisecond,
				LeaderFwdTimeout:  10 * time.Millisecond,
				AutoRemoveTimeout: 10 * time.Millisecond,
			},
		)
		defer pool.Close()
		pool.SetTimeoutHandler(timeoutHandler)

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())

		to1WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		to2WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 1)
		to3WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 1)

		assert.Equal(t, 0, pool.Size())
	})
}

func TestMakeRequest(t *testing.T) {
	r := makeTestRequest("AB", "CDE", "FGHI")
	assert.Equal(t, 21, len(r))

	clientID, txID, data := parseTestRequest(r)
	assert.Equal(t, "AB", clientID)
	assert.Equal(t, "CDE", txID)
	assert.Equal(t, "FGHI", data)

	ins := &testRequestInspector{}
	info := ins.RequestID(r)
	assert.Equal(t, "AB", info.ClientID)
	assert.Equal(t, "CDE", info.ID)
}

func makeTestRequest(clientID, txID, data string) []byte {
	buffLen := make([]byte, 4)
	buff := make([]byte, 12)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(clientID)))
	buff = append(buff[0:0], buffLen...)
	buff = append(buff, []byte(clientID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(txID)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(txID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(data)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(data)...)

	return buff
}

func parseTestRequest(request []byte) (clientID, txID, data string) {
	l := binary.LittleEndian.Uint32(request)
	buff := request[4:]

	clientID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	txID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	data = string(buff[:l])

	return
}

type testRequestInspector struct {
}

func (ins *testRequestInspector) RequestID(req []byte) types.RequestInfo {
	var info types.RequestInfo
	info.ClientID, info.ID, _ = parseTestRequest(req)
	return info
}
