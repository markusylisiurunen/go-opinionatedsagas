package opinionatedsagas

import (
	"context"
	"testing"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/stretchr/testify/assert"
)

func TestStepHandleFuncValidation(t *testing.T) {
	tt := []struct {
		name           string
		handleFunc     any
		expectedToBeOk bool
	}{
		{
			name: "a valid handle func",
			handleFunc: func(ctx context.Context, task *testFuncTask) (result, *testFuncTask, *testFuncTask) {
				return events.SuccessResult(), &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}
			},
			expectedToBeOk: true,
		},
		{
			name: "a valid handle func returning only result",
			handleFunc: func(ctx context.Context, task *testFuncTask) result {
				return events.SuccessResult()
			},
			expectedToBeOk: true,
		},
		{
			name: "handle func with no arguments",
			handleFunc: func() (result, *testFuncTask, *testFuncTask) {
				return events.SuccessResult(), &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}
			},
			expectedToBeOk: false,
		},
		{
			name: "handle func with only context argument",
			handleFunc: func(ctx context.Context) (result, *testFuncTask, *testFuncTask) {
				return events.SuccessResult(), &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}
			},
			expectedToBeOk: false,
		},
		{
			name: "handle func with task not implementing the interface",
			handleFunc: func(ctx context.Context, task *notTestFuncTask) (result, *testFuncTask, *testFuncTask) {
				return events.SuccessResult(), &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}
			},
			expectedToBeOk: false,
		},
		{
			name: "handle func with non-pointer task",
			handleFunc: func(ctx context.Context, task testFuncTask) (result, *testFuncTask, *testFuncTask) {
				return events.SuccessResult(), &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}
			},
			expectedToBeOk: false,
		},
		{
			name:           "handle func with no return values",
			handleFunc:     func(ctx context.Context, task *testFuncTask) {},
			expectedToBeOk: false,
		},
		{
			name: "handle func returning only the result and next task",
			handleFunc: func(ctx context.Context, task *testFuncTask) (result, *testFuncTask) {
				return events.SuccessResult(), &testFuncTask{"next", ""}
			},
			expectedToBeOk: false,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			step := &Step{HandleFunc: tc.handleFunc}
			if tc.expectedToBeOk {
				assert.NoError(t, step.handleFuncIsValid())
			} else {
				assert.Error(t, step.handleFuncIsValid())
			}
		})
	}
}

func TestStepCompensateFuncValidation(t *testing.T) {
	tt := []struct {
		name           string
		compensateFunc any
		expectedToBeOk bool
	}{
		{
			name: "a valid compensate func",
			compensateFunc: func(ctx context.Context, task *testFuncTask) result {
				return events.SuccessResult()
			},
			expectedToBeOk: true,
		},
		{
			name:           "a nil compensate func",
			compensateFunc: nil,
			expectedToBeOk: true,
		},
		{
			name: "compensate func with no arguments",
			compensateFunc: func() result {
				return events.SuccessResult()
			},
			expectedToBeOk: false,
		},
		{
			name: "compensate func with only context argument",
			compensateFunc: func(ctx context.Context) result {
				return events.SuccessResult()
			},
			expectedToBeOk: false,
		},
		{
			name: "compensate func with task not implementing the interface",
			compensateFunc: func(ctx context.Context, task *notTestFuncTask) result {
				return events.SuccessResult()
			},
			expectedToBeOk: false,
		},
		{
			name: "compensate func with non-pointer task",
			compensateFunc: func(ctx context.Context, task testFuncTask) result {
				return events.SuccessResult()
			},
			expectedToBeOk: false,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			step := &Step{CompensateFunc: tc.compensateFunc}
			if tc.expectedToBeOk {
				assert.NoError(t, step.compensateFuncIsValid())
			} else {
				assert.Error(t, step.compensateFuncIsValid())
			}
		})
	}
}

type notTestFuncTask struct {
	Value string `json:"value"`
}

type testFuncTask struct {
	name  string
	Value string `json:"value"`
}

func (t *testFuncTask) TaskName() string {
	return t.name
}
