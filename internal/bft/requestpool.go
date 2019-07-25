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
	DefaultRequestTimeoutMillis = 60000
)

//go:generate mockery -dir . -name RequestTimeoutHandler -case underscore -output ./mocks/

// RequestTimeoutHandler defines the methods called by request timeout timers created by time.AfterFunc.
// This interface is implemented by the bft.Controller.
type RequestTimeoutHandler interface {

	// OnRequestTimeout is called when a request timeout expires.
	OnRequestTimeout(request []byte, requestInfo types.RequestInfo)

	// OnLeaderFwdRequestTimeout is called when a leader forwarding timeout expires.
	OnLeaderFwdRequestTimeout(request []byte, requestInfo types.RequestInfo)

	// OnAutoRemoveTimeout is called when a auto-remove timeout expires.
	OnAutoRemoveTimeout(requestInfo types.RequestInfo)
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
	options        PoolOptions
}

// requestItem captures request related information
type requestItem struct {
	request []byte
	timeout *time.Timer
}

type PoolOptions struct {
	QueueSize         int64
	RequestTimeout    time.Duration
	LeaderFwdTimeout  time.Duration
	AutoRemoveTimeout time.Duration
}

// NewPool constructs new requests pool
func NewPool(log api.Logger, inspector api.RequestInspector, options PoolOptions) *Pool {
	if options.RequestTimeout == 0 {
		options.RequestTimeout = DefaultRequestTimeoutMillis * time.Millisecond
	}
	if options.LeaderFwdTimeout == 0 {
		options.LeaderFwdTimeout = DefaultRequestTimeoutMillis * time.Millisecond
	}
	if options.AutoRemoveTimeout == 0 {
		options.AutoRemoveTimeout = DefaultRequestTimeoutMillis * time.Millisecond
	}

	return &Pool{
		logger:    log,
		inspector: inspector,
		fifo:      list.New(),
		semaphore: semaphore.NewWeighted(options.QueueSize),
		existMap:  make(map[types.RequestInfo]*list.Element),
		options:   options,
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

	to := time.AfterFunc(
		rp.options.RequestTimeout,
		func() { rp.onRequestTO(request, reqInfo) },
	)
	reqItem := &requestItem{
		request: request,
		timeout: to,
	}

	element := rp.fifo.PushBack(reqItem)
	rp.existMap[reqInfo] = element

	if len(rp.existMap) != rp.fifo.Len() {
		rp.logger.Panicf("RequestPool map and list are of different length: map=%d, list=%d", len(rp.existMap), rp.fifo.Len())
	}

	rp.logger.Debugf("Request %s submitted; started a timeout: %s", reqInfo, rp.options.RequestTimeout)
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

// Prune removes requests for which the given predicate returns error.
func (rp *Pool) Prune(predicate func([]byte) error) {
	reqVec, infoVec := rp.copyRequests()

	var numPruned int
	for i, req := range reqVec {
		err := predicate(req)
		if err == nil {
			continue
		}

		if remErr := rp.RemoveRequest(infoVec[i]); remErr != nil {
			rp.logger.Warnf("Failed to prune request: %s; predicate error: %s; remove error: %s", infoVec[i], err, remErr)
		} else {
			rp.logger.Debugf("Pruned request: %s; predicate error: %s", infoVec[i], err)
			numPruned++
		}
	}

	rp.logger.Debugf("Pruned %d requests", numPruned)
}

func (rp *Pool) copyRequests() (requestVec [][]byte, infoVec []types.RequestInfo) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	requestVec = make([][]byte, len(rp.existMap))
	infoVec = make([]types.RequestInfo, len(rp.existMap))

	var i int
	for info, item := range rp.existMap {
		infoVec[i] = info
		requestVec[i] = item.Value.(*requestItem).request
		i++
	}

	return
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

	return rp.deleteRequest(element, requestInfo)
}

func (rp *Pool) deleteRequest(element *list.Element, requestInfo types.RequestInfo) error {
	item := element.Value.(*requestItem)
	item.timeout.Stop()

	rp.fifo.Remove(element)
	delete(rp.existMap, requestInfo)
	rp.logger.Infof("Removed request %s from request pool", requestInfo)
	rp.semaphore.Release(1)

	if len(rp.existMap) != rp.fifo.Len() {
		rp.logger.Panicf("RequestPool map and list are of different length: map=%d, list=%d", len(rp.existMap), rp.fifo.Len())
	}

	return nil
}

func (rp *Pool) contains(reqInfo types.RequestInfo) bool {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	_, contains := rp.existMap[reqInfo]
	return contains
}

// called by the goroutine spawned by time.AfterFunc
func (rp *Pool) onRequestTO(request []byte, reqInfo types.RequestInfo) {
	if !rp.contains(reqInfo) {
		return
	}
	// may take time, in case Comm channel to leader is full; hence w/o the lock.
	rp.logger.Debugf("Request %s timeout expired, going to send to leader", reqInfo)
	rp.timeoutHandler.OnRequestTimeout(request, reqInfo)

	rp.lock.Lock()
	defer rp.lock.Unlock()

	element, contains := rp.existMap[reqInfo]
	if !contains {
		rp.logger.Debugf("Request %s no longer in pool", reqInfo)
		return
	}

	//start a second timeout
	item := element.Value.(*requestItem)
	item.timeout = time.AfterFunc(
		rp.options.LeaderFwdTimeout,
		func() { rp.onLeaderFwdRequestTO(request, reqInfo) },
	)
	rp.logger.Debugf("Request %s; started a leader-forwarding timeout: %s", reqInfo, rp.options.LeaderFwdTimeout)
}

// called by the goroutine spawned by time.AfterFunc
func (rp *Pool) onLeaderFwdRequestTO(request []byte, reqInfo types.RequestInfo) {
	if !rp.contains(reqInfo) {
		return
	}
	// may take time, in case Comm channel is full; hence w/o the lock.
	rp.logger.Debugf("Request %s leader-forwarding timeout expired, going to complain on leader", reqInfo)
	rp.timeoutHandler.OnLeaderFwdRequestTimeout(request, reqInfo)

	rp.lock.Lock()
	defer rp.lock.Unlock()

	element, contains := rp.existMap[reqInfo]
	if !contains {
		rp.logger.Debugf("Request %s no longer in pool", reqInfo)
		return
	}

	//start a third timeout
	item := element.Value.(*requestItem)
	item.timeout = time.AfterFunc(
		rp.options.AutoRemoveTimeout,
		func() { rp.onAutoRemoveTO(reqInfo) },
	)
	rp.logger.Debugf("Request %s; started auto-remove timeout: %s", reqInfo, rp.options.AutoRemoveTimeout)
}

// called by the goroutine spawned by time.AfterFunc
func (rp *Pool) onAutoRemoveTO(reqInfo types.RequestInfo) {
	rp.logger.Debugf("Request %s auto-remove timeout expired, going to remove from pool", reqInfo)
	if err := rp.RemoveRequest(reqInfo); err != nil {
		rp.logger.Errorf("Removal of request %s failed; error: %s", reqInfo, err)
		return
	}
	rp.timeoutHandler.OnAutoRemoveTimeout(reqInfo)
	return
}
