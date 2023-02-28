package opinionatedsagas

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/gjson"
)

func TestWithRollback(t *testing.T) {
	t.Run("discards the task after too many failed attempts with an empty rollback stack", func(t *testing.T) {
		// initialise a mock publisher
		destination := newTestDestination()
		publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(destination))
		assert.NoError(t, err)
		// initialise the rollback middleware
		middleware := withRollback(publisher, 2)(
			func(ctx context.Context, queue string, delivery events.Delivery) result {
				return events.ErrorResult(errors.New("not implemented"), 1*time.Second)
			},
		)
		// first attempt
		result1 := middleware(context.Background(), "tasks", newTestDelivery(1, 0))
		assert.Error(t, result1.GetResult().Err)
		assert.False(t, result1.GetResult().RetryAt.IsZero())
		assert.Len(t, destination.messages, 0)
		// second attempt
		result2 := middleware(context.Background(), "tasks", newTestDelivery(2, 0))
		assert.Error(t, result2.GetResult().Err)
		assert.True(t, result2.GetResult().RetryAt.IsZero())
		assert.Len(t, destination.messages, 0)
	})

	t.Run("publishes a rollback task after a fatal attempt with a non-empty rollback stack", func(t *testing.T) {
		// initialise a mock publisher
		destination := newTestDestination()
		publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(destination))
		assert.NoError(t, err)
		// initialise the rollback middleware
		middleware := withRollback(publisher, 2)(
			func(ctx context.Context, queue string, delivery events.Delivery) result {
				return events.FatalResult(errors.New("not implemented"))
			},
		)
		// first attempt
		result1 := middleware(context.Background(), "tasks", newTestDelivery(1, 1))
		assert.Error(t, result1.GetResult().Err)
		assert.True(t, result1.GetResult().RetryAt.IsZero())
		assert.Len(t, destination.messages, 1)
		// validate the published message
		rollbackTaskName := gjson.Get(string(destination.messages[0]), "name").String()
		assert.Equal(t, "tasks.rollback_0", rollbackTaskName)
		payload, err := base64.StdEncoding.DecodeString(
			gjson.Get(string(destination.messages[0]), "payload").String(),
		)
		assert.NoError(t, err)
		rollbackTaskValue := gjson.Get(string(payload), "task.value").String()
		assert.Equal(t, "rollback_0", rollbackTaskValue)
	})

	t.Run("publishes a rollback task after too many failed attempts with a non-empty rollback stack", func(t *testing.T) {
		// initialise a mock publisher
		destination := newTestDestination()
		publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(destination))
		assert.NoError(t, err)
		// initialise the rollback middleware
		middleware := withRollback(publisher, 2)(
			func(ctx context.Context, queue string, delivery events.Delivery) result {
				return events.ErrorResult(errors.New("not implemented"), 1*time.Second)
			},
		)
		// first attempt
		result1 := middleware(context.Background(), "tasks", newTestDelivery(1, 1))
		assert.Error(t, result1.GetResult().Err)
		assert.False(t, result1.GetResult().RetryAt.IsZero())
		assert.Len(t, destination.messages, 0)
		// second attempt
		result2 := middleware(context.Background(), "tasks", newTestDelivery(2, 1))
		assert.Error(t, result2.GetResult().Err)
		assert.True(t, result2.GetResult().RetryAt.IsZero())
		assert.Len(t, destination.messages, 1)
		// validate the published message
		rollbackTaskName := gjson.Get(string(destination.messages[0]), "name").String()
		assert.Equal(t, "tasks.rollback_0", rollbackTaskName)
		payload, err := base64.StdEncoding.DecodeString(
			gjson.Get(string(destination.messages[0]), "payload").String(),
		)
		assert.NoError(t, err)
		rollbackTaskValue := gjson.Get(string(payload), "task.value").String()
		assert.Equal(t, "rollback_0", rollbackTaskValue)
	})

	t.Run("does not run the handler after failing to publish the rollback task", func(t *testing.T) {
		// initialise a mock publisher
		destination := newTestDestination()
		publisher, err := events.NewPublisher(events.PublisherWithSyncBridge(destination))
		assert.NoError(t, err)
		// initialise the rollback middleware
		count := 0
		middleware := withRollback(publisher, 1)(
			func(ctx context.Context, queue string, delivery events.Delivery) result {
				count += 1
				return events.FatalResult(errors.New("not implemented"))
			},
		)
		// first attempt
		destination.err = errors.New("cannot publish the message")
		result1 := middleware(context.Background(), "tasks", newTestDelivery(1, 1))
		// --> the handler should have been called once
		assert.Equal(t, 1, count)
		// --> the result should be an error but NOT fatal
		assert.Error(t, result1.GetResult().Err)
		assert.False(t, result1.GetResult().RetryAt.IsZero())
		// --> no message should have been published
		assert.Len(t, destination.messages, 0)
		// second attempt
		destination.err = nil
		result2 := middleware(context.Background(), "tasks", newTestDelivery(2, 1))
		// --> the handler must not be called more than once across the two attempts
		assert.Equal(t, 1, count)
		// --> the result should be a fatal error
		assert.Error(t, result2.GetResult().Err)
		assert.True(t, result2.GetResult().RetryAt.IsZero())
		// --> the rollback message must have been published
		assert.Len(t, destination.messages, 1)
		// validate the published message
		rollbackTaskName := gjson.Get(string(destination.messages[0]), "name").String()
		assert.Equal(t, "tasks.rollback_0", rollbackTaskName)
		payload, err := base64.StdEncoding.DecodeString(
			gjson.Get(string(destination.messages[0]), "payload").String(),
		)
		assert.NoError(t, err)
		rollbackTaskValue := gjson.Get(string(payload), "task.value").String()
		assert.Equal(t, "rollback_0", rollbackTaskValue)
	})
}
