package opinionatedsagas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
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
	postgresDestination, err := events.NewPostgresDestination(opts.DB)
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

func (s *testSaga) trigger(attempt int, t task, r []task) error {
	// construct the task
	stack := newRollbackStack()
	for _, t := range r {
		stack.push(newTaskMessage(t).asRollbackStackItem())
	}
	task := newTaskMessageWithRollbackHistory(t, stack)
	// construct the delivery
	msg, err := task.toOpinionatedMessage()
	if err != nil {
		return err
	}
	delivery := newTestDeliveryRaw(attempt, "tasks", msg)
	// trigger the receiver
	result := s.saga.receiver.Deliver(context.Background(), delivery)
	return result
}

func TestSagaHandle(t *testing.T) {
	t.Run("executes the steps' handlers with not errors", func(t *testing.T) {
		r := rand.New(rand.NewSource(4537392487))
		// init database connection
		connectionString := "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
		schema := fmt.Sprintf("opinionatedsagas_%d", r.Int())
		db, err := sql.Open("postgres", connectionString)
		assert.NoError(t, err)
		// init the saga
		ctx, cancel := context.WithCancel(context.Background())
		saga, err := newTestSaga(ctx, &SagaOpts{
			DB:               db,
			ConnectionString: connectionString,
			Schema:           schema,
		})
		assert.NoError(t, err)
		// init the x-step
		var counterXHandle atomic.Int64
		var counterXCompensate atomic.Int64
		saga.addStep(&Step{
			MaxAttempts: 1,
			HandleFunc: func(ctx context.Context, task *testSagaXTask) (*testSagaCompensateXTask, *testSagaYTask, error) {
				counterXHandle.Add(1)
				return &testSagaCompensateXTask{Value: "test"}, &testSagaYTask{Value: "test"}, nil
			},
			CompensateFunc: func(ctx context.Context, task *testSagaCompensateXTask) error {
				counterXCompensate.Add(1)
				return nil
			},
		})
		// init the y-step
		var counterYHandle atomic.Int64
		saga.addStep(&Step{
			MaxAttempts: 1,
			HandleFunc: func(ctx context.Context, task *testSagaYTask) error {
				counterYHandle.Add(1)
				return nil
			},
		})
		// register the handlers
		err = saga.registerHandlers()
		assert.NoError(t, err)
		// execute the first x-step
		result1 := saga.trigger(1, &testSagaXTask{Value: "test"}, []task{})
		assert.NoError(t, result1)
		// execute the first y-step
		result2 := saga.trigger(1, &testSagaYTask{Value: "test"}, []task{
			&testSagaCompensateXTask{Value: "test"},
		})
		assert.NoError(t, result2)
		// validate the task handle execution counts
		assert.Equal(t, int64(1), counterXHandle.Load())
		assert.Equal(t, int64(0), counterXCompensate.Load())
		assert.Equal(t, int64(1), counterXHandle.Load())
		// validate the published tasks
		assert.Len(t, saga.destination.messages, 1)
		assert.Equal(t, "tasks.y", saga.destination.getTaskName(0))
		assert.Equal(t, 1, saga.destination.getRollbackStackSize(0))
		assert.Equal(t, "compensate_x", saga.destination.getRollbackStackTaskName(0, 0))
		// shut down the saga
		cancel()
	})

	t.Run("executes the first step's handler func", func(t *testing.T) {
		r := rand.New(rand.NewSource(2346786792))
		// init database connection
		connectionString := "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
		schema := fmt.Sprintf("opinionatedsagas_%d", r.Int())
		db, err := sql.Open("postgres", connectionString)
		assert.NoError(t, err)
		// init the saga
		ctx, cancel := context.WithCancel(context.Background())
		saga, err := newTestSaga(ctx, &SagaOpts{
			ConnectionString: connectionString,
			DB:               db,
			Schema:           schema,
		})
		assert.NoError(t, err)
		// init the x-step
		var counterXHandle atomic.Int64
		var counterXCompensate atomic.Int64
		saga.addStep(&Step{
			MaxAttempts: 1,
			HandleFunc: func(ctx context.Context, task *testSagaXTask) (*testSagaCompensateXTask, *testSagaYTask, error) {
				counterXHandle.Add(1)
				return &testSagaCompensateXTask{Value: "test"}, &testSagaYTask{Value: "test"}, nil
			},
			CompensateFunc: func(ctx context.Context, task *testSagaCompensateXTask) error {
				counterXCompensate.Add(1)
				return nil
			},
		})
		// init the y-step
		var counterYHandle atomic.Int64
		saga.addStep(&Step{
			MaxAttempts: 2,
			HandleFunc: func(ctx context.Context, task *testSagaYTask) (*testSagaCompensateYTask, *testSagaZTask, error) {
				counterYHandle.Add(1)
				return nil, nil, errors.New("something happened")
			},
		})
		// register the handlers
		err = saga.registerHandlers()
		assert.NoError(t, err)
		// execute the first x-step
		result1 := saga.trigger(1, &testSagaXTask{Value: "test"}, []task{})
		assert.NoError(t, result1)
		// execute the first y-step
		result2_1 := saga.trigger(1, &testSagaYTask{Value: "test"}, []task{
			&testSagaCompensateXTask{Value: "test"},
		})
		assert.Error(t, result2_1)
		result2_2 := saga.trigger(2, &testSagaYTask{Value: "test"}, []task{
			&testSagaCompensateXTask{Value: "test"},
		})
		assert.Error(t, result2_2)
		// execute the second x-step
		result3 := saga.trigger(1, &testSagaCompensateXTask{Value: "test"}, []task{})
		assert.NoError(t, result3)
		// validate the published tasks
		assert.Len(t, saga.destination.messages, 2)
		assert.Equal(t, "tasks.y", saga.destination.getTaskName(0))
		assert.Equal(t, 1, saga.destination.getRollbackStackSize(0))
		assert.Equal(t, "compensate_x", saga.destination.getRollbackStackTaskName(0, 0))
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
			handleFunc: func(ctx context.Context, task *testSagaXTask) (*testSagaXTask, *testSagaXTask, error) {
				return &testSagaXTask{"x"}, &testSagaXTask{"x"}, nil
			},
			triggerTask:    &testSagaXTask{"x"},
			expectedToBeOk: true,
		},
		{
			name: "a non-matching trigger task and handle func",
			handleFunc: func(ctx context.Context, task *testSagaXTask) (*testSagaXTask, *testSagaXTask, error) {
				return &testSagaXTask{"x"}, &testSagaXTask{"x"}, nil
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

func (t *testSagaXTask) TaskName() string { return "x" }

type testSagaCompensateXTask struct {
	Value string `json:"value"`
}

func (t *testSagaCompensateXTask) TaskName() string { return "compensate_x" }

type testSagaYTask struct {
	Value string `json:"value"`
}

func (t *testSagaYTask) TaskName() string { return "y" }

type testSagaCompensateYTask struct {
	Value string `json:"value"`
}

func (t *testSagaCompensateYTask) TaskName() string { return "compensate_y" }

type testSagaZTask struct {
	Value string `json:"value"`
}

func (t *testSagaZTask) TaskName() string { return "z" }
