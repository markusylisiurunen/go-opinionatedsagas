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
  "context"
  sagas "github.com/markusylisiurunen/go-opinionatedsagas"
)

type DoX struct {
  X int `json:"x"`
}

func (t *DoX) TaskName() string {
  return "do_x"
}

type CompensateX struct {
  X int `json:"x"`
}

func (t *CompensateX) TaskName() string {
  return "compensate_x"
}

type DoY struct {
  Y int `json:"y"`
}

func (t *DoY) TaskName() string {
  return "do_y"
}

func Example(ctx context.Context, connectionString string) error {
  saga, err := sagas.NewSaga(ctx, &sagas.SagaOpts{ConnectionString: connectionString})
  if err != nil {
    return err
  }
  saga.AddStep(&sagas.Step{
    HandleFunc: func(ctx context.Context, task *DoX) (*CompensateX, *DoY, error) {
      return &CompensateX{X: task.X}, &DoY{Y: 42}, nil
    },
    CompensateFunc: func(ctx context.Context, task *CompensateX) error {
      return nil
    },
  })
  saga.AddStep(&sagas.Step{
    MaxAttempts: 3,
    HandleFunc: func(ctx context.Context, task *DoY) error {
      if rand.Float64() <= 0.33 {
        // if an error occurs (too many times), the saga will roll back and the previous
        // steps' `CompensateFunc`s will be invoked.
        return errors.New("something went wrong")
      }
      return nil
    },
  })
  err = saga.RegisterHandlers()
  if err != nil {
    return err
  }
  return nil
}
```
