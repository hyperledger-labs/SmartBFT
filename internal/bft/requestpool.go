// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"context"
	"fmt"
	"sync"

	"github.com/SmartBFT-Go/consensus/pkg/types"
	"golang.org/x/sync/semaphore"
)

//go:generate mockery -dir . -name RequestInspector -case underscore -output ./mocks/
type RequestInspector interface {
	RequestID(req []byte) types.RequestInfo
}

type Pool struct {
	Log              Logger
	RequestInspector RequestInspector
	queue            []Request
	semaphore        *semaphore.Weighted
	lock             sync.RWMutex
	QueueSize        int64
	existMap         map[string]bool
}

type Request struct {
	ID       string
	Request  []byte
	ClientID string
}

func (rp *Pool) Start() {
	rp.queue = make([]Request, 0)
	rp.semaphore = semaphore.NewWeighted(rp.QueueSize)
	rp.existMap = make(map[string]bool)
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
		rp.Log.Errorf(err)
		return fmt.Errorf(err)
	}
	rp.queue = append(rp.queue, req)
	rp.existMap[existStr] = true
	return nil
}

// NextRequests return the next requests to be batched
func (rp *Pool) NextRequests(n int) []Request {
	rp.lock.RLock()
	defer rp.lock.RUnlock()
	if len(rp.queue) <= n {
		return rp.queue
	}
	return rp.queue[:n]
}

// RemoveRequest removes the given request from the pool
func (rp *Pool) RemoveRequest(request Request) error {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	existStr := fmt.Sprintf("%v~%v", request.ClientID, request.ID)
	if _, exist := rp.existMap[existStr]; !exist {
		err := fmt.Sprintf("Request %v is not in the pool at remove time", request)
		rp.Log.Warnf(err)
		return fmt.Errorf(err)
	}
	for i, existingReq := range rp.queue {
		if existingReq.ClientID != request.ClientID || existingReq.ID != request.ID {
			continue
		}
		rp.Log.Infof("Removed request %v from request pool", request)
		rp.queue = append(rp.queue[:i], rp.queue[i+1:]...)
		delete(rp.existMap, existStr)
		rp.semaphore.Release(1)
		return nil
	}
	panic("RemoveRequest should have returned earlier")
}
