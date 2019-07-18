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

	"encoding/hex"

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
	request         []byte
	verificationSqn uint64
	timeout         *time.Timer
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
func (rp *Pool) Submit(request []byte, verificationSequence uint64) error {
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
		request:         request,
		verificationSqn: verificationSequence,
		timeout:         nil, //TODO start timer
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

type Requests []Request

type RequestIndex map[string]Request

func (index RequestIndex) FilterByPayloads(payloads [][]byte) Requests {
	var res Requests
	for _, payload := range payloads {
		req, exists := index[hex.EncodeToString(payload)]
		if exists {
			res = append(res, req)
		}
	}
	return res
}

func (requests Requests) Index() RequestIndex {
	// Build a map from request hex to Request
	req2Seq := make(map[string]Request, len(requests))
	for _, req := range requests {
		req2Seq[hex.EncodeToString(req.Payload)] = req
	}
	return req2Seq
}

func (requests Requests) Payloads() [][]byte {
	res := make([][]byte, len(requests))
	for i, req := range requests {
		res[i] = req.Payload
	}
	return res
}

type Request struct {
	Payload         []byte
	VerificationSqn uint64
}

// NextRequests returns the next requests to be batched.
// It returns at most n request, in a newly allocated slice.
func (rp *Pool) NextRequests(n int) []Request {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	m := minInt(rp.fifo.Len(), n)
	buff := make([]Request, m)
	var element = rp.fifo.Front()
	for i := 0; i < m; i++ {
		item := element.Value.(*requestItem)
		buff[i] = Request{
			Payload:         append(make([]byte, 0), item.request...),
			VerificationSqn: item.verificationSqn,
		}
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
