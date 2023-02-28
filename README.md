# Opinionated Sagas

[![Go Reference](https://pkg.go.dev/badge/github.com/markusylisiurunen/go-opinionatedsagas.svg)](https://pkg.go.dev/github.com/markusylisiurunen/go-opinionatedsagas)

**Table of Contents**

1. [Install](#install)
2. [The problem](#the-problem)
3. [This solution](#this-solution)

## Install

```sh
go get github.com/markusylisiurunen/go-opinionatedsagas
```

## The problem

TODO

## This solution

TODO

```go
package main

import (
  events "github.com/markusylisiurunen/go-opinionatedevents"
  sagas "github.com/markusylisiurunen/go-opinionatedsagas"
)

type DoXTask struct {
  X int `json:"x"`
}

func (t *DoXTask) TaskName() string {
  return "tasks.do_x"
}

type CompensateDoXTask struct {
  X int `json:"x"`
}

func (t *CompensateDoXTask) TaskName() string {
  return "tasks.compensate_do_x"
}

type DoYTask struct {
  Y int `json:"y"`
}

func (t *DoYTask) TaskName() string {
  return "tasks.do_y"
}

func Example(receiver *events.Receiver, publisher *events.Publisher) error {
  saga := sagas.NewSaga(receiver, publisher, "tasks")
  saga.AddStep(&sagas.Step{
    HandleFunc: func(ctx context.Context, task *DoXTask) (sagas.Result, *CompensateDoXTask, *DoYTask) {
      return sagas.Success(), &CompensateDoXTask{X: task.X}, &DoYTask{Y: 42}
    },
    CompensateFunc: func(ctx context.Context, task *CompensateDoXTask) sagas.Result {
      return sagas.Success()
    },
  })
  saga.AddStep(&sagas.Step{
    HandleFunc: func(ctx context.Context, task *DoYTask) sagas.Result {
      if rand.Float64() <= 0.33 {
        // if an error occurs (too many times), the saga will roll back and the previous
        // steps' `CompensateFunc`s will be invoked.
        return sagas.Error(errors.New("something went wrong"), 15*time.Second)
      }
      return sagas.Success()
    },
  })
  return saga.RegisterHandlers()
}
```
