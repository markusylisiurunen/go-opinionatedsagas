package opinionatedsagas

import (
	"context"
	"testing"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/stretchr/testify/assert"
)

func TestWrappedMiddlewaresExecutionOrder(t *testing.T) {
	data := []int{}
	makeMiddleware := func(idx int) events.OnMessageMiddleware {
		return func(next events.OnMessageHandler) events.OnMessageHandler {
			return func(ctx context.Context, queue string, delivery events.Delivery) result {
				data = append(data, idx)
				return next(ctx, queue, delivery)
			}
		}
	}
	makeHandler := func(idx int) events.OnMessageHandler {
		return func(ctx context.Context, queue string, delivery events.Delivery) result {
			data = append(data, idx)
			return events.SuccessResult()
		}
	}
	var mw1, mw2 events.OnMessageMiddleware
	mw1 = makeMiddleware(1)
	mw2 = makeMiddleware(2)
	handler := makeHandler(3)
	result := middlewares{mw1, mw2}.wrap(handler)(context.Background(), "test", newTestDelivery(1, 0))
	assert.NoError(t, result.GetResult().Err)
	assert.Equal(t, []int{1, 2, 3}, data)
}
