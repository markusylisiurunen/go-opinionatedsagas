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

func Example(
  receiver *opinionatedevents.Receiver,
  publisher *opinionatedevents.Publisher,
) error {
  saga := opinionatedsagas.NewSaga(receiver, publisher, "tasks")
  saga.AddStep(&opinionatedsagas.Step{
    HandleFunc: func(ctx context.Context, task *DoXTask) (opinionatedevents.ResultContainer, *CompensateDoXTask, *DoYTask) {
      return opinionatedevents.SuccessResult(), &CompensateDoXTask{X: task.X}, &DoYTask{Y: 42}
    },
    CompensateFunc: func(ctx context.Context, task *CompensateDoXTask) opinionatedevents.ResultContainer {
      return opinionatedevents.SuccessResult()
    },
  })
  saga.AddStep(&opinionatedsagas.Step{
    HandleFunc: func(ctx context.Context, task *DoYTask) opinionatedevents.ResultContainer {
      if rand.Float64() <= 0.33 {
        // if an error occurs (too many times), the saga will roll back and the previous steps' `CompensateFunc`s will be invoked.
        return opinionatedevents.ErrorResult(errors.New("something went wrong"), 15*time.Second)
      }
      return opinionatedevents.SuccessResult()
    },
  })
  return saga.RegisterHandlers()
}
```
