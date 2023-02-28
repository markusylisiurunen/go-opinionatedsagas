package opinionatedsagas

import (
	events "github.com/markusylisiurunen/go-opinionatedevents"
)

type middlewares []events.OnMessageMiddleware

func (mws middlewares) wrap(handler events.OnMessageHandler) events.OnMessageHandler {
	wrapped := func(next events.OnMessageHandler) events.OnMessageHandler {
		handler := next
		for i := len(mws) - 1; i >= 0; i -= 1 {
			handler = mws[i](handler)
		}
		return handler
	}
	return wrapped(handler)
}
