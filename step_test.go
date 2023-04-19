package opinionatedsagas

import (
	"context"
	"testing"

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
			handleFunc: func(ctx context.Context, task *testFuncTask) (*testFuncTask, *testFuncTask, error) {
				return &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}, nil
			},
			expectedToBeOk: true,
		},
		{
			name: "a valid handle func returning only error",
			handleFunc: func(ctx context.Context, task *testFuncTask) error {
				return nil
			},
			expectedToBeOk: true,
		},
		{
			name: "handle func with no arguments",
			handleFunc: func() (*testFuncTask, *testFuncTask, error) {
				return &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}, nil
			},
			expectedToBeOk: false,
		},
		{
			name: "handle func with only context argument",
			handleFunc: func(ctx context.Context) (*testFuncTask, *testFuncTask, error) {
				return &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}, nil
			},
			expectedToBeOk: false,
		},
		{
			name: "handle func with task not implementing the interface",
			handleFunc: func(ctx context.Context, task *notTestFuncTask) (*testFuncTask, *testFuncTask, error) {
				return &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}, nil
			},
			expectedToBeOk: false,
		},
		{
			name: "handle func with non-pointer task",
			handleFunc: func(ctx context.Context, task testFuncTask) (*testFuncTask, *testFuncTask, error) {
				return &testFuncTask{"compensate", ""}, &testFuncTask{"next", ""}, nil
			},
			expectedToBeOk: false,
		},
		{
			name:           "handle func with no return values",
			handleFunc:     func(ctx context.Context, task *testFuncTask) {},
			expectedToBeOk: false,
		},
		{
			name: "handle func returning only the error and next task",
			handleFunc: func(ctx context.Context, task *testFuncTask) (*testFuncTask, error) {
				return &testFuncTask{"next", ""}, nil
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
			compensateFunc: func(ctx context.Context, task *testFuncTask) error {
				return nil
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
			compensateFunc: func() error {
				return nil
			},
			expectedToBeOk: false,
		},
		{
			name: "compensate func with only context argument",
			compensateFunc: func(ctx context.Context) error {
				return nil
			},
			expectedToBeOk: false,
		},
		{
			name: "compensate func with task not implementing the interface",
			compensateFunc: func(ctx context.Context, task *notTestFuncTask) error {
				return nil
			},
			expectedToBeOk: false,
		},
		{
			name: "compensate func with non-pointer task",
			compensateFunc: func(ctx context.Context, task testFuncTask) error {
				return nil
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
