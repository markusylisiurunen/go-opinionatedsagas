package opinionatedsagas

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/stretchr/testify/assert"
)

type testSaga struct {
	saga        *Saga
	destination *testDestination
}

// FIXME: should not init the receiver/publisher at all
func newTestSaga(ctx context.Context, opts *SagaOpts) (*testSaga, error) {
	saga, err := NewSaga(ctx, opts)
	if err != nil {
		return nil, err
	}
	// replace the publisher
	testDestination := newTestDestination()
	postgresDestination, err := events.NewPostgresDestination(opts.ConnectionString,
		events.PostgresDestinationWithTopicToQueues("tasks", "tasks"),
		events.PostgresDestinationWithTableName(fmt.Sprintf("%s.events", opts.getSchema())),
	)
	if err != nil {
		return nil, err
	}
	publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(testDestination, postgresDestination))
	if err != nil {
		return nil, err
	}
	saga.publisher = publisher
	// replace the receiver
	receiver, err := events.NewReceiver()
	if err != nil {
		return nil, err
	}
	saga.receiver = receiver
	return &testSaga{
		saga:        saga,
		destination: testDestination,
	}, nil
}

func (s *testSaga) addStep(step *Step) {
	s.saga.AddStep(step)
}

func (s *testSaga) registerHandlers() error {
	return s.saga.RegisterHandlers()
}

func (s *testSaga) trigger(attempt int, t task, r []task) (result, error) {
	// construct the task
	stack := newRollbackStack()
	for _, t := range r {
		stack.push(newTaskMessage(t).asRollbackStackItem())
	}
	task := newTaskMessageWithRollbackHistory(t, stack)
	// construct the delivery
	msg, err := task.toOpinionatedMessage()
	if err != nil {
		return nil, err
	}
	delivery := newTestDeliveryRaw(attempt, msg)
	// trigger the receiver
	result := s.saga.receiver.Receive(context.Background(), "tasks", delivery)
	return result, nil
}

func TestSagaHandle(t *testing.T) {
	t.Run("executes the first step's handler func", func(t *testing.T) {
		r := rand.New(rand.NewSource(0))
		// init database connection
		connectionString := "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
		schema := fmt.Sprintf("opinionatedsagas_%d", r.Int())
		// init the saga
		ctx, cancel := context.WithCancel(context.Background())
		saga, err := newTestSaga(ctx, &SagaOpts{
			ConnectionString: connectionString,
			Schema:           schema,
		})
		assert.NoError(t, err)
		// init the x-step
		var counterXHandle atomic.Int64
		var counterXCompensate atomic.Int64
		saga.addStep(&Step{
			HandleFunc: func(ctx context.Context, task *testSagaXTask) (result, *testSagaCompensateXTask, *testSagaYTask) {
				counterXHandle.Add(1)
				return events.SuccessResult(), &testSagaCompensateXTask{Value: "test"}, &testSagaYTask{Value: "test"}
			},
			CompensateFunc: func(ctx context.Context, task *testSagaCompensateXTask) result {
				counterXCompensate.Add(1)
				return events.SuccessResult()
			},
		})
		// init the y-step
		var counterYHandle atomic.Int64
		saga.addStep(&Step{
			HandleFunc: func(ctx context.Context, task *testSagaYTask) (result, *testSagaCompensateYTask, *testSagaZTask) {
				counterYHandle.Add(1)
				return events.ErrorResult(errors.New(""), 10*time.Second), nil, nil
			},
		})
		// register the handlers
		err = saga.registerHandlers()
		assert.NoError(t, err)
		// execute the first x-step
		result1, err := saga.trigger(1, &testSagaXTask{Value: "test"}, []task{})
		assert.NoError(t, err)
		assert.NoError(t, result1.GetResult().Err)
		// execute the first y-step
		result2_1, err := saga.trigger(2, &testSagaYTask{Value: "test"}, []task{
			&testSagaCompensateXTask{Value: "test"},
		})
		assert.NoError(t, err)
		assert.Error(t, result2_1.GetResult().Err)
		result2_2, err := saga.trigger(3, &testSagaYTask{Value: "test"}, []task{
			&testSagaCompensateXTask{Value: "test"},
		})
		assert.NoError(t, err)
		assert.Error(t, result2_2.GetResult().Err)
		// execute the second x-step
		result3, err := saga.trigger(1, &testSagaCompensateXTask{Value: "test"}, []task{})
		assert.NoError(t, err)
		assert.NoError(t, result3.GetResult().Err)
		// validate the published tasks
		assert.Len(t, saga.destination.messages, 2)
		assert.Equal(t, "tasks.y", saga.destination.getTaskName(0))
		assert.Equal(t, 1, saga.destination.getRollbackStackSize(0))
		assert.Equal(t, "tasks.compensate_x", saga.destination.getRollbackStackTaskName(0, 0))
		assert.Equal(t, "tasks.compensate_x", saga.destination.getTaskName(1))
		assert.Equal(t, 0, saga.destination.getRollbackStackSize(1))
		// shut down the saga
		cancel()
	})
}

func TestSagaTrigger(t *testing.T) {
	tt := []struct {
		name           string
		handleFunc     any
		triggerTask    task
		expectedToBeOk bool
	}{
		{
			name: "a matching trigger task and handle func",
			handleFunc: func(ctx context.Context, task *testSagaXTask) (result, *testSagaXTask, *testSagaXTask) {
				return events.SuccessResult(), &testSagaXTask{"x"}, &testSagaXTask{"x"}
			},
			triggerTask:    &testSagaXTask{"x"},
			expectedToBeOk: true,
		},
		{
			name: "a non-matching trigger task and handle func",
			handleFunc: func(ctx context.Context, task *testSagaXTask) (result, *testSagaXTask, *testSagaXTask) {
				return events.SuccessResult(), &testSagaXTask{"x"}, &testSagaXTask{"x"}
			},
			triggerTask:    &testSagaYTask{"y"},
			expectedToBeOk: false,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			// init the receiver and publisher
			receiver, err := events.NewReceiver()
			assert.NoError(t, err)
			destination := newTestDestination()
			publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(destination))
			assert.NoError(t, err)
			// init the saga
			saga := &Saga{
				db:        nil,
				publisher: publisher,
				queue:     "tasks",
				receiver:  receiver,
				schema:    "opinionatedsagas",
				steps:     []*Step{},
			}
			saga.AddStep(&Step{HandleFunc: tc.handleFunc})
			assert.NoError(t, saga.RegisterHandlers())
			// trigger the task
			err = saga.TriggerTx(context.Background(), nil, tc.triggerTask)
			if tc.expectedToBeOk {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

type testSagaXTask struct {
	Value string `json:"value"`
}

func (t *testSagaXTask) TaskName() string { return "tasks.x" }

type testSagaCompensateXTask struct {
	Value string `json:"value"`
}

func (t *testSagaCompensateXTask) TaskName() string { return "tasks.compensate_x" }

type testSagaYTask struct {
	Value string `json:"value"`
}

func (t *testSagaYTask) TaskName() string { return "tasks.y" }

type testSagaCompensateYTask struct {
	Value string `json:"value"`
}

func (t *testSagaCompensateYTask) TaskName() string { return "tasks.compensate_y" }

type testSagaZTask struct {
	Value string `json:"value"`
}

func (t *testSagaZTask) TaskName() string { return "tasks.z" }
