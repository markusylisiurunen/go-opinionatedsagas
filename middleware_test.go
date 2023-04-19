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
			return func(ctx context.Context, delivery events.Delivery) error {
				data = append(data, idx)
				return next(ctx, delivery)
			}
		}
	}
	makeHandler := func(idx int) events.OnMessageHandler {
		return func(ctx context.Context, delivery events.Delivery) error {
			data = append(data, idx)
			return nil
		}
	}
	var mw1, mw2 events.OnMessageMiddleware
	mw1 = makeMiddleware(1)
	mw2 = makeMiddleware(2)
	handler := makeHandler(3)
	result := middlewares{mw1, mw2}.wrap(handler)(context.Background(), newTestDelivery(1, "tasks", 0))
	assert.NoError(t, result)
	assert.Equal(t, []int{1, 2, 3}, data)
}
