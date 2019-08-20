// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/internal/bft/mocks"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestReqPoolBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar().With(zap.String("t", t.Name()))

	insp := &testRequestInspector{}
	byteReq1 := makeTestRequest("1", "1", "foo")

	t.Run("create close", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		timeChan := make(chan time.Time, 1)
		timeChan <- time.Now()
		pool := bft.NewPool(log, insp, timeoutHandler, timeChan, bft.PoolOptions{QueueSize: 3, RequestTimeout: time.Hour})

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())
		pool.Close()
		assert.Equal(t, 0, pool.Size())

		err = pool.Submit(byteReq1)
		assert.EqualError(t, err, "pool stopped, request rejected: {1 1}")
	})

	t.Run("submit remove next", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		timeChan := make(chan time.Time, 1)
		timeChan <- time.Now()
		pool := bft.NewPool(log, insp, timeoutHandler, timeChan, bft.PoolOptions{QueueSize: 3, RequestTimeout: time.Hour})
		defer pool.Close()

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
		pool.Close()
	})
}

func TestReqPoolCapacity(t *testing.T) {
	numReq := 100
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar().With(zap.String("t", t.Name()))
	insp := &testRequestInspector{}

	t.Run("submit storm", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}
		timeChan := make(chan time.Time, 1)
		timeChan <- time.Now()
		pool := bft.NewPool(log, insp, timeoutHandler, timeChan, bft.PoolOptions{QueueSize: 50, RequestTimeout: time.Hour})
		defer pool.Close()

		wg := sync.WaitGroup{}
		wg.Add(2 * numReq)
		for i := 0; i < numReq; i++ {
			go func(i string) {
				byteReq := makeTestRequest(i, i, "foo")
				err := pool.Submit(byteReq)
				assert.NoError(t, err)
				wg.Done()
			}(fmt.Sprintf("%d", i))
		}

		time.Sleep(10 * time.Millisecond)

		for i := 0; i < numReq; i++ {
			go func(i string) {
				req := types.RequestInfo{
					ID:       i,
					ClientID: i,
				}
				err := pool.RemoveRequest(req)
				for err != nil {
					err = pool.RemoveRequest(req)
				}
				wg.Done()
			}(fmt.Sprintf("%d", i))
		}
		wg.Wait()

		assert.Equal(t, 0, pool.Size())
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", .0)
		pool.Close()
	})
}

func TestReqPoolPrune(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar().With(zap.String("t", t.Name()))

	insp := &testRequestInspector{}
	timeoutHandler := &mocks.RequestTimeoutHandler{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "bar")

	timeChan := make(chan time.Time, 1)
	timeChan <- time.Now()

	pool := bft.NewPool(log, insp, timeoutHandler, timeChan, bft.PoolOptions{QueueSize: 3, RequestTimeout: time.Hour})
	defer pool.Close()

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
	pool.Close()
}

func TestReqPoolTimeout(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	assert.NoError(t, err)
	log := basicLog.Sugar().With(zap.String("t", t.Name()))

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo")

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

		timeChan := make(chan time.Time, 1)
		start := time.Now()
		timeChan <- start

		pool := bft.NewPool(log,
			insp,
			timeoutHandler,
			timeChan,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    10 * time.Second,
				LeaderFwdTimeout:  time.Hour,
				AutoRemoveTimeout: time.Hour,
			},
		)
		defer pool.Close()

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())

		timeChan <- start.Add(11 * time.Second)

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

		timeChan := make(chan time.Time, 1)
		start := time.Now()
		timeChan <- start

		pool := bft.NewPool(log, insp, timeoutHandler, timeChan,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    10 * time.Second,
				LeaderFwdTimeout:  10 * time.Second,
				AutoRemoveTimeout: time.Hour,
			},
		)
		defer pool.Close()

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())

		timeChan <- start.Add(time.Second * 11)
		to1WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		timeChan <- start.Add(time.Second * 22)
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

		timeChan := make(chan time.Time, 1)
		start := time.Now()
		timeChan <- start
		pool := bft.NewPool(log, insp, timeoutHandler, timeChan,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    10 * time.Second,
				LeaderFwdTimeout:  10 * time.Second,
				AutoRemoveTimeout: 10 * time.Second,
			},
		)
		defer pool.Close()

		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())
		timeChan <- start.Add(time.Second * 11)
		to1WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		timeChan <- start.Add(time.Second * 22)
		to2WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 1)
		timeChan <- start.Add(time.Second * 33)
		to3WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 1)

		assert.Equal(t, 0, pool.Size())
	})

	t.Run("stop restart", func(t *testing.T) {
		start := time.Now()
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			assert.Fail(t, "called OnRequestTimeout")
		}).Return()

		requestTimeoutFired := make(chan struct{})
		timeoutHandler.On("OnRequestTimeout", byteReq2, insp.RequestID(byteReq2)).Run(func(args mock.Arguments) {
			select {
			case <-requestTimeoutFired:
			default:
				close(requestTimeoutFired)
			}

		}).Return()

		timeoutHandler.On("OnLeaderFwdRequestTimeout", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			assert.Fail(t, "called OnLeaderFwdRequestTimeout")
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			assert.Fail(t, "called OnAutoRemoveTimeout")
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq2))

		timeChan := make(chan time.Time, 1)
		timeChan <- time.Now()
		pool := bft.NewPool(log, insp, timeoutHandler, timeChan,
			bft.PoolOptions{
				QueueSize:         3,
				RequestTimeout:    100 * time.Millisecond,
				LeaderFwdTimeout:  time.Hour,
				AutoRemoveTimeout: time.Hour,
			},
		)
		defer pool.Close()
		assert.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		assert.NoError(t, err)
		assert.Equal(t, 1, pool.Size())

		pool.StopTimers()
		timeChan <- start.Add(time.Second)
		err = pool.RemoveRequest(insp.RequestID(byteReq1))
		assert.NoError(t, err)
		err = pool.Submit(byteReq2)
		assert.EqualError(t, err, "pool stopped, request rejected: {2 2}")

		pool.RestartTimers()
		timeChan <- start.Add(time.Second * 2)
		err = pool.Submit(byteReq2)
		assert.NoError(t, err)
		pool.StopTimers()
		pool.RestartTimers()

		go forwardTime(timeChan, start.Add(time.Second*3), requestTimeoutFired)
		<-requestTimeoutFired

		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 0)

		err := pool.RemoveRequest(insp.RequestID(byteReq2))
		assert.NoError(t, err)
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

func forwardTime(timeChan chan<- time.Time, start time.Time, stopChan <-chan struct{}) {
	for i := 1; i < 1000; i++ {
		select {
		case <-stopChan:
			return
		case timeChan <- start.Add(time.Second * time.Duration(i)):
		}
		time.Sleep(time.Millisecond * 10)
	}
}
