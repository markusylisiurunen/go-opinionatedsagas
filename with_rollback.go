package opinionatedsagas

import (
	"context"
	"errors"
	"time"

	events "github.com/markusylisiurunen/go-opinionatedevents"
)

func withRollback(
	publisher *events.Publisher, limit int,
) events.OnMessageMiddleware {
	doRollback := func(ctx context.Context, delivery events.Delivery) result {
		delay := 10 * time.Second
		// parse the message payload
		msg := newTaskMessage(&noopTask{})
		if err := delivery.GetMessage().Payload(msg); err != nil {
			return events.ErrorResult(err, delay)
		}
		// publish the rollback message
		rollback, ok := msg.rollback()
		if !ok {
			// the rollback stack is empty, processing is done
			err := errors.New("processing the task failed, nothing to roll back")
			return events.FatalResult(err)
		}
		opinionatedRollback, err := rollback.toOpinionatedMessage()
		if err != nil {
			return events.ErrorResult(err, delay)
		}
		if err := publisher.Publish(ctx, opinionatedRollback); err != nil {
			return events.ErrorResult(err, delay)
		}
		err = errors.New("processing the task failed, rolling back")
		return events.FatalResult(err)
	}
	return func(next events.OnMessageHandler) events.OnMessageHandler {
		return func(ctx context.Context, queue string, delivery events.Delivery) result {
			if limit != 0 && delivery.GetAttempt() > limit {
				// processing the message has already been attempted `limit` many times
				return doRollback(ctx, delivery)
			}
			// try to process the message
			result := next(ctx, queue, delivery)
			if result.GetResult().Err != nil && result.GetResult().RetryAt.IsZero() {
				// fatal error, should roll back immediately
				return doRollback(ctx, delivery)
			}
			if result.GetResult().Err != nil && limit > 0 && delivery.GetAttempt() >= limit {
				// retriable error, but has failed too many times
				return doRollback(ctx, delivery)
			}
			// otherwise, don't roll back
			return result
		}
	}
}
