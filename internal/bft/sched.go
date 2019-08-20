// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

type Stopper interface {
	Stop()
}

type task struct {
	deadline time.Time
	f        func()
	stopped  uint32
}

func (t *task) Stop() {
	atomic.StoreUint32(&t.stopped, 1)
}

func (t *task) isStopped() bool {
	return atomic.LoadUint32(&t.stopped) == uint32(1)
}

type TaskQueue []*task

func (h TaskQueue) Len() int {
	return len(h)
}
func (h TaskQueue) Less(i, j int) bool {
	return h[i].deadline.Before(h[j].deadline)
}
func (h TaskQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *TaskQueue) Push(o interface{}) {
	t := o.(*task)
	*h = append(*h, t)
}

func (h *TaskQueue) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *TaskQueue) Top() *task {
	return (*h)[0]
}

type cmd struct {
	timeout time.Duration
	t       *task
}

type Scheduler struct {
	exec        *executor
	currentTime time.Time
	queue       *TaskQueue
	signalChan  chan struct{}
	cmdChan     chan cmd
	timeChan    <-chan time.Time
	stopChan    chan struct{}
	running     sync.WaitGroup
}

func NewScheduler(timeChan <-chan time.Time) *Scheduler {
	s := &Scheduler{
		queue:      &TaskQueue{},
		timeChan:   timeChan,
		signalChan: make(chan struct{}),
		cmdChan:    make(chan cmd),
		stopChan:   make(chan struct{}),
	}

	s.exec = &executor{
		running:  &s.running,
		queue:    make(chan func(), 1),
		stopChan: s.stopChan,
	}

	return s
}

func (s *Scheduler) Start() {
	s.running.Add(2)

	go s.exec.run()
	go s.run()
}

func (s *Scheduler) Stop() {
	select {
	case <-s.stopChan:
		return
	default:

	}
	defer s.running.Wait()
	close(s.stopChan)
}

func (s *Scheduler) Schedule(timeout time.Duration, f func()) Stopper {
	task := &task{f: f}
	s.cmdChan <- cmd{
		t:       task,
		timeout: timeout,
	}
	return task
}

func (s *Scheduler) run() {
	defer s.running.Done()

	s.waitForFirstTick()

	for {
		select {
		case <-s.stopChan:
			return
		case now := <-s.timeChan:
			s.currentTime = now
			s.tick()
		case <-s.signalChan:
			s.tick()
		case cmd := <-s.cmdChan:
			task := cmd.t
			task.deadline = s.currentTime.Add(cmd.timeout)
			s.queue.Push(task)
		}
	}
}

func (s *Scheduler) waitForFirstTick() {
	select {
	case s.currentTime = <-s.timeChan:
	case <-s.stopChan:
	}
}

func (s *Scheduler) tick() {
	for {
		executedSomeTask := s.checkAndExecute()
		// If we executed some task, we can try to execute the next task if
		// such a task is ready to be executed.
		if !executedSomeTask {
			return
		}
	}
}

// checkAndExecute checks if there is an executable task,
// and if so then executes it.
// Returns true if executed a task, else false.
func (s *Scheduler) checkAndExecute() bool {
	if s.queue.Len() == 0 {
		return false
	}

	// Should earliest deadline task be scheduled?
	if s.queue.Top().deadline.After(s.currentTime) {
		return false
	}

	task := heap.Pop(s.queue).(*task)

	f := func() {
		if task.isStopped() {
			return
		}
		task.f()
		select {
		case s.signalChan <- struct{}{}:
		case <-s.exec.stopChan:
			return
		}

	}

	// Check if there is room in the executor queue by trying to enqueue into it.
	select {
	case s.exec.queue <- f:
		// We succeeded in enqueueing, nothing to do.
		return true
	default:
		// We couldn't enqueue to the executor, so re-add the task.
		heap.Push(s.queue, task)
		return false
	}
}

type executor struct {
	running  *sync.WaitGroup
	stopChan chan struct{}
	queue    chan func() // 1 capacity channel
}

func (e *executor) run() {
	defer e.running.Done()
	for {
		select {
		case <-e.stopChan:
			return
		case f := <-e.queue:
			f()
		}
	}
}
