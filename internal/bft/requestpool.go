// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

const (
	DefaultRequestTimeoutMillis = 10000
)

//go:generate mockery -dir . -name RequestTimeoutHandler -case underscore -output ./mocks/

// RequestTimeoutHandler defines the methods called by request timeout timers created by time.AfterFunc.
// This interface is implemented by the bft.Controller.
type RequestTimeoutHandler interface {

	// OnRequestTimeout is called when a request timeout expires
	OnRequestTimeout(request []byte)

	// OnLeaderFwdRequestTimeout is called when a leader forwarding timeout expires
	OnLeaderFwdRequestTimeout(request []byte)
}

// Pool implements requests pool, maintains pool of given size provided during
// construction. In case there are more incoming request than given size it will
// block during submit until there will be place to submit new ones.
type Pool struct {
	logger         api.Logger
	inspector      api.RequestInspector
	fifo           *list.List
	semaphore      *semaphore.Weighted
	lock           sync.Mutex
	existMap       map[types.RequestInfo]*list.Element
	timeoutHandler RequestTimeoutHandler
	requestTimeout time.Duration
}

// requestItem captures request related information
type requestItem struct {
	request []byte
	timeout *time.Timer
}

// NewPool constructs new requests pool
func NewPool(log api.Logger, inspector api.RequestInspector, queueSize int64, requestTimeout time.Duration) *Pool {
	if requestTimeout == 0 {
		requestTimeout = DefaultRequestTimeoutMillis * time.Millisecond
	}

	return &Pool{
		logger:         log,
		inspector:      inspector,
		fifo:           list.New(),
		semaphore:      semaphore.NewWeighted(queueSize),
		existMap:       make(map[types.RequestInfo]*list.Element),
		requestTimeout: requestTimeout,
	}
}

func (rp *Pool) SetTimeoutHandler(handler RequestTimeoutHandler) {
	rp.timeoutHandler = handler
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	reqInfo := rp.inspector.RequestID(request)

	if err := rp.semaphore.Acquire(context.Background(), 1); err != nil {
		return errors.Wrapf(err, "acquiring semaphore for request: %s", reqInfo)
	}

	rp.lock.Lock()
	defer rp.lock.Unlock()

	if _, exist := rp.existMap[reqInfo]; exist {
		rp.semaphore.Release(1)
		errStr := fmt.Sprintf("request %s already exists in the pool", reqInfo)
		rp.logger.Errorf(errStr)
		return fmt.Errorf(errStr)
	}

	reqItem := &requestItem{
		request: request,
		timeout: nil, //TODO start a timer
	}
	element := rp.fifo.PushBack(reqItem)
	rp.existMap[reqInfo] = element

	if len(rp.existMap) != rp.fifo.Len() {
		rp.logger.Panicf("RequestPool map and list are of different length: map=%d, list=%d", len(rp.existMap), rp.fifo.Len())
	}

	return nil
}

// Size returns the number of requests currently residing the pool
func (rp *Pool) Size() int {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	return len(rp.existMap)
}

// NextRequests returns the next requests to be batched.
// It returns at most n request, in a newly allocated slice.
func (rp *Pool) NextRequests(n int) [][]byte {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	m := minInt(rp.fifo.Len(), n)
	buff := make([][]byte, m)
	var element = rp.fifo.Front()
	for i := 0; i < m; i++ {
		buff[i] = append(make([]byte, 0), element.Value.(*requestItem).request...)
		element = element.Next()
	}

	return buff
}

// RemoveRequest removes the given request from the pool
func (rp *Pool) RemoveRequest(requestInfo types.RequestInfo) error {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	element, exist := rp.existMap[requestInfo]
	if !exist {
		errStr := fmt.Sprintf("request %s is not in the pool at remove time", requestInfo)
		rp.logger.Warnf(errStr)
		return fmt.Errorf(errStr)
	}

	rp.fifo.Remove(element)
	delete(rp.existMap, requestInfo)
	rp.logger.Infof("Removed request %s from request pool", requestInfo)
	rp.semaphore.Release(1)

	if len(rp.existMap) != rp.fifo.Len() {
		rp.logger.Panicf("RequestPool map and list are of different length: map=%d, list=%d", len(rp.existMap), rp.fifo.Len())
	}

	return nil
}
