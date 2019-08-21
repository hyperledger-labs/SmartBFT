// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/stretchr/testify/assert"
)

func TestSchedule(t *testing.T) {
	timeChan := make(chan time.Time, 2)
	s := bft.NewScheduler(timeChan)
	s.Start()

	start := time.Now()

	timeChan <- start

	out := make(chan int, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	s.Schedule(time.Second, func() {
		defer wg.Done()
		out <- 1
	})

	taskToStop := s.Schedule(time.Second*4, func() {
		defer wg.Done()
		out <- 4
	})

	s.Schedule(time.Second*3, func() {
		defer wg.Done()
		out <- 3
	})

	s.Schedule(time.Second*2, func() {
		defer wg.Done()
		out <- 2
	})

	// Time cannot go back.
	timeChan <- time.Time{}
	timeChan <- start.Add(time.Millisecond)
	assert.Len(t, out, 0)

	taskToStop.Stop()

	// All tasks should have been scheduled despite a single tick
	start = start.Add(time.Second * 5)
	timeChan <- start
	wg.Wait()
	close(out)
	for i := 1; i <= 3; i++ {
		n := <-out
		assert.Equal(t, i, n)
	}

	// Block the output channel so the function would get stuck
	// until we de-queue from 'out'
	out = make(chan int)
	s.Schedule(time.Second, func() {
		// Wait for the 2 minutes tick below to be consumed by injecting an empty time.
		// This ensures the clock processed both the 7 second increment and the 2 min increment.
		timeChan <- time.Time{}
		out <- 4
	})

	s.Schedule(time.Minute, func() {
		out <- 5
	})

	// Advance time by 2 more seconds and then by 2 minutes.
	timeChan <- start.Add(time.Second * 2)
	timeChan <- start.Add(time.Minute * 2)

	// De-queue from the output channel
	n := <-out
	assert.Equal(t, 4, n)
	wg.Wait()

	// Ensure the time increased by 2 minutes despite it passed while being blocked
	// on the previous function invocation.
	n = <-out
	assert.Equal(t, 5, n)

	s.Stop()
}

func TestEnqueueDequeue(t *testing.T) {
	q := bft.NewTaskQueue()
	now := time.Now()

	// Enqueue latest deadline to earliest, in descending order
	for i := 10; i > 0; i-- {
		q.Enqueue(&bft.Task{Deadline: now.Add(time.Duration(i))})
	}

	// Ensure de-queue is in ascending order
	for i := 1; i <= 10; i++ {
		task := q.DeQueue()
		assert.Equal(t, task.Deadline, now.Add(time.Duration(i)))
	}

	// No more tasks
	assert.Nil(t, q.DeQueue())
	assert.Nil(t, q.Top())
}
