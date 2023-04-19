package opinionatedsagas

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	events "github.com/markusylisiurunen/go-opinionatedevents"
	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
)

func TestWithIdempotentSequentially(t *testing.T) {
	r := rand.New(rand.NewSource(5348997842))
	// init database connection
	connectionString := "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
	schema := fmt.Sprintf("opinionatedsagas_%d", r.Int())
	db, err := sql.Open("postgres", connectionString)
	assert.NoError(t, err)
	// ensure migrations
	err = migrate(db, schema)
	assert.NoError(t, err)
	// execute the same test for `n` times
	for i := 0; i < 3; i += 1 {
		// init a handler
		var handlerCount atomic.Int64
		var handler events.OnMessageHandler = func(ctx context.Context, delivery events.Delivery) error {
			handlerCount.Add(1)
			return nil
		}
		// execute the (wrapped) handler `n` times
		var successCount atomic.Int64
		var failureCount atomic.Int64
		wrapped := withIdempotent(db, schema)(handler)
		delivery := newTestDelivery(1, "tasks", 0)
		for i := 0; i < 32; i += 1 {
			result := wrapped(context.Background(), delivery)
			if result != nil {
				failureCount.Add(1)
			} else {
				successCount.Add(1)
			}
		}
		// the handler should have been called only once
		assert.Equal(t, int64(1), handlerCount.Load())
		// every call should have returned a success
		assert.Equal(t, int64(32), successCount.Load())
		assert.Equal(t, int64(0), failureCount.Load())
	}
	db.Close()
}

func TestWithIdempotentConcurrently(t *testing.T) {
	r := rand.New(rand.NewSource(1234893457))
	// init database connection
	connectionString := "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
	schema := fmt.Sprintf("opinionatedsagas_%d", r.Int())
	db, err := sql.Open("postgres", connectionString)
	assert.NoError(t, err)
	// ensure migrations
	err = migrate(db, schema)
	assert.NoError(t, err)
	// execute the same test for `n` times
	for i := 0; i < 3; i += 1 {
		// init a handler
		var handlerCount atomic.Int64
		var handler events.OnMessageHandler = func(ctx context.Context, delivery events.Delivery) error {
			handlerCount.Add(1)
			return nil
		}
		// execute the (wrapped) handler `n` times
		wrapped := withIdempotent(db, schema)(handler)
		delivery := newTestDelivery(1, "tasks", 0)
		var wg sync.WaitGroup
		for i := 0; i < 64; i += 1 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				wrapped(context.Background(), delivery) //nolint
			}()
		}
		wg.Wait()
		// the handler should have been called only once
		assert.Equal(t, int64(1), handlerCount.Load())
	}
	db.Close()
}
