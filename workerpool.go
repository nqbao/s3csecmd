package main

import (
  "sync"
  "errors"
)

var (
  ErrStoppedWorkerPool = errors.New("WorkerPool has been stopped")
)

type Task struct {
  f func() error
  Err error
}

func (t *Task) Run() {
  t.Err = t.f()
}

type WorkerPool struct {
  ResultCh chan *Task
  taskCh chan *Task
  stopOnce *sync.Once
  stopped bool
  wg *sync.WaitGroup
}

func NewWorkerPool(workers int) (p *WorkerPool) {
  p = &WorkerPool{
    ResultCh: make(chan *Task),
    taskCh: make(chan *Task),
    stopOnce: &sync.Once{},
    wg: &sync.WaitGroup{},
  }

  // start worker goroutines
  p.wg.Add(workers)
  for i := 0; i < workers; i++ {
    go p.startWorker(i)
  }

  return p
}

func (p *WorkerPool) SubmitFunc(f func() error) error {
  return p.Submit(&Task{
    f: f,
  })
}

func (p *WorkerPool) Submit(t *Task) (err error) {
  defer func() {
    if recover() != nil {
      err = ErrStoppedWorkerPool
    }
  }()

  p.taskCh <- t
  return nil
}

// Stop all workers, this will also make sure all submitted tasks are executed
func (p *WorkerPool) Stop() {
  p.stopOnce.Do(func() {
    close(p.taskCh)
    p.stopped = true

    // wait for all workers to stop
    p.wg.Wait()
  })
}

func (w *WorkerPool) IsStopped() bool {
  return w.stopped
}

func (w* WorkerPool) startWorker(id int) {
  defer w.wg.Done()

  for item := range w.taskCh {
    item.Run()

    // send back the item in the resultCh
    go func(item *Task) {
      w.ResultCh <- item
    }(item)
  }
}
