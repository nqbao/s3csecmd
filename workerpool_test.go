package main

import (
  "time"
  "testing"
  "errors"
)

var (
  dummyErr = errors.New("Dummy error")
)

func sleepTask(duration time.Duration) (f func() error) {
  f = func() error {
    time.Sleep(duration)
    return nil
  }

  return
}

func errorTask(err error) (f func() error) {
  f = func() error {
    return err
  }

  return
}

func TestSubmitTask(t *testing.T) {
  t.Parallel()

  p := NewWorkerPool(1)

  p.SubmitFunc(sleepTask(100 * time.Millisecond))

  <- p.ResultCh
  select {
  case <- p.ResultCh:
    t.Error("Unexpected task result")
  default:
    // should have no pending result
  }

  p.Stop()
  if !p.IsStopped() {
    t.Error("Worker pool should be closed")
  }
}

func TestErrorTask(t *testing.T) {
  t.Parallel()

  p := NewWorkerPool(1)
  p.SubmitFunc(errorTask(dummyErr))
  p.Stop()

  r := <- p.ResultCh
  if r.Err != dummyErr {
    t.Error("Expect task error to be set")
  }
}

func TestMultiWorker(t *testing.T) {
  t.Parallel()

  workers := 2
  numTasks := workers*2

  p := NewWorkerPool(workers)

  start := time.Now()
  for i := 0; i < numTasks; i++ {
    p.SubmitFunc(sleepTask(1 * time.Second))
  }

  p.Stop()

  numResult := 0
  done := false
  for {
    timeout := time.After(100 * time.Millisecond)

    select {
    case <- p.ResultCh:
      numResult = numResult + 1
    case <- timeout:
      done = true
    }

    if done {
      break
    }
  }

  totalTime := time.Since(start)

  if numResult != numTasks {
    t.Error("Expect numResult to be", workers, "got", numResult)
  }

  // each task take 1 second, but since we have multiple workers, we expect
  // the total time should be less than that
  if totalTime >= (time.Duration(numTasks) * time.Second) {
    t.Error("Expect total execution time to be less than", numTasks)
  }
}

func TestSubmitError(t *testing.T) {
  t.Parallel()

  p := NewWorkerPool(1)
  ch := make(chan error)

  go func() {
    for i := 0; i < 5; i++ {
      ch <- p.SubmitFunc(sleepTask(1 * time.Second))
    }

    close(ch)
  }()

  p.Stop()

  // make sure we receive at least one error
  hasError := false
  for i := range ch {
    if i != nil {
      hasError = true
      break
    }
  }

  if !hasError {
    t.Error("Expect to receive error when sending to a stopped worker pool")
  }
}
