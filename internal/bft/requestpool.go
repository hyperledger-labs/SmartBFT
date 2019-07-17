// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"context"
	"fmt"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"golang.org/x/sync/semaphore"
)

type requestTimeoutHandler interface {

	// called when a request timeout expires
	onRequestTimeout(request []byte)

	// called when a leader forwarding timeout expires
	onLeaderFwdRequestTimeout(request []byte)
}

// Pool implements requests pool, maintains pool of given size provided during
// construction. In case there are more incoming request than given size it will
// block during submit until there will be place to submit new ones.
type Pool struct {
	logger           api.Logger
	RequestInspector api.RequestInspector
	queue            []Request
	semaphore        *semaphore.Weighted
	lock             sync.RWMutex
	existMap         map[string]bool
}

// Request captures request related information
type Request struct {
	ClientID string
	ID       string
	Request  []byte
}

// NewPool constructs new requests pool
func NewPool(
	log api.Logger,
	inspector api.RequestInspector,
	queueSize int64,
) *Pool {
	return &Pool{
		logger:           log,
		RequestInspector: inspector,
		queue:            make([]Request, 0),
		semaphore:        semaphore.NewWeighted(queueSize),
		existMap:         make(map[string]bool),
	}
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	reqInfo := rp.RequestInspector.RequestID(request)
	req := Request{
		ID:       reqInfo.ID,
		Request:  request,
		ClientID: reqInfo.ClientID,
	}
	if err := rp.semaphore.Acquire(context.Background(), 1); err != nil {
		return fmt.Errorf("error in acquiring semaphore: %v", err)
	}
	rp.lock.Lock()
	defer rp.lock.Unlock()
	existStr := fmt.Sprintf("%v~%v", reqInfo.ClientID, reqInfo.ID)
	if _, exist := rp.existMap[existStr]; exist {
		rp.semaphore.Release(1)
		err := fmt.Sprintf("a request with ID %v and client ID %v already exists in the pool", reqInfo.ID, reqInfo.ClientID)
		rp.logger.Errorf(err)
		return fmt.Errorf(err)
	}
	rp.queue = append(rp.queue, req)
	rp.existMap[existStr] = true
	return nil
}

// SizeOfPool returns the number of requests currently residing the pool
func (rp *Pool) SizeOfPool() int {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	return len(rp.queue)
}

// NextRequests returns the next requests to be batched.
// It returns at most n request, in a newly allocated slice.
func (rp *Pool) NextRequests(n int) [][]byte {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	m := minInt(len(rp.queue), n)
	buff := make([][]byte, m)
	for i := 0; i < m; i++ {
		buff[i] = append(make([]byte, 0), rp.queue[i].Request...)
	}

	return buff
}

// RemoveRequest removes the given request from the pool
func (rp *Pool) RemoveRequest(requestInfo types.RequestInfo) error {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	existStr := fmt.Sprintf("%v~%v", requestInfo.ClientID, requestInfo.ID)
	if _, exist := rp.existMap[existStr]; !exist {
		err := fmt.Sprintf("Request %s is not in the pool at remove time", requestInfo)
		rp.logger.Warnf(err)
		return fmt.Errorf(err)
	}
	for i, existingReq := range rp.queue {
		if existingReq.ClientID != requestInfo.ClientID || existingReq.ID != requestInfo.ID {
			continue
		}
		rp.logger.Infof("Removed request %v from request pool", requestInfo)
		rp.queue = append(rp.queue[:i], rp.queue[i+1:]...)
		delete(rp.existMap, existStr)
		rp.semaphore.Release(1)
		return nil
	}
	panic("RemoveRequest should have returned earlier")
}
