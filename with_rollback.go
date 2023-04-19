package opinionatedsagas

import (
	"context"
	"errors"

	events "github.com/markusylisiurunen/go-opinionatedevents"
)

func withRollback(
	publisher *events.Publisher, limit int,
) events.OnMessageMiddleware {
	doRollback := func(ctx context.Context, delivery events.Delivery) error {
		// parse the message payload
		msg := newTaskMessage(&noopTask{})
		if err := delivery.GetMessage().GetPayload(msg); err != nil {
			return err
		}
		// publish the rollback message
		rollback, ok := msg.rollback()
		if !ok {
			// the rollback stack is empty, processing is done
			err := errors.New("processing the task failed, nothing to roll back")
			return events.Fatal(err)
		}
		opinionatedRollback, err := rollback.toOpinionatedMessage()
		if err != nil {
			return err
		}
		if err := publisher.Publish(ctx, opinionatedRollback); err != nil {
			return err
		}
		err = errors.New("processing the task failed, rolling back")
		return events.Fatal(err)
	}
	return func(next events.OnMessageHandler) events.OnMessageHandler {
		return func(ctx context.Context, delivery events.Delivery) error {
			if limit != 0 && delivery.GetAttempt() > limit {
				// processing the message has already been attempted `limit` many times
				return doRollback(ctx, delivery)
			}
			// try to process the message
			result := next(ctx, delivery)
			if result != nil && events.IsFatal(result) {
				// fatal error, should roll back immediately
				return doRollback(ctx, delivery)
			}
			if result != nil && limit > 0 && delivery.GetAttempt() >= limit {
				// retriable error, but has failed too many times
				return doRollback(ctx, delivery)
			}
			// otherwise, don't roll back
			return result
		}
	}
}
