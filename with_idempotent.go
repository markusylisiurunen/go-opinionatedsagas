package opinionatedsagas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/lib/pq"
	events "github.com/markusylisiurunen/go-opinionatedevents"
)

const setSerializableIsolationLevelQuery = `
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
`

const getIdempotencyKeyByKeyQuery = `
SELECT id, key, created_at, locked_at, is_processed
FROM :SCHEMA.idempotency_keys
WHERE key = $1
LIMIT 1
`

const createIdempotencyKeyQuery = `
INSERT INTO :SCHEMA.idempotency_keys (key, created_at, locked_at, is_processed)
VALUES ($1, $2, $3, $4)
RETURNING id
`

const lockIdempotencyKeyByKeyQuery = `
UPDATE :SCHEMA.idempotency_keys
SET locked_at = $1, is_processed = FALSE
WHERE key = $2
`

const markIdempotencyKeyAsProcessedByKeyQuery = `
UPDATE :SCHEMA.idempotency_keys
SET locked_at = NULL, is_processed = TRUE
WHERE key = $1
`

const deleteIdempotencyKeyByKeyQuery = `
DELETE FROM :SCHEMA.idempotency_keys
WHERE key = $1
`

type idempotencyKey struct {
	CreatedAt   time.Time
	ID          int64
	IsProcessed bool
	Key         string
	LockedAt    *time.Time
}

var (
	errIdempotencyKeyRepositoryIsLocked      = errors.New("the idempotency key is already locked")
	errIdempotencyKeyRepositoryIsProcessed   = errors.New("the idempotency key has already been marked as processed")
	errIdempotencyKeyRepositorySerialization = errors.New("could not serialize access due to read/write dependencies among transactions")
)

type idempotencyKeyRepository struct {
	db            *sql.DB
	schema        string
	keepLockedFor time.Duration
}

func (r *idempotencyKeyRepository) withSchema(query string) string {
	return strings.ReplaceAll(query, ":SCHEMA", r.schema)
}

func (r *idempotencyKeyRepository) begin(ctx context.Context, key string) error {
	type result struct {
		isLocked      bool
		isProcessed   bool
		shouldAttempt bool
	}
	isLocked := func() (*result, error) { return &result{true, false, false}, nil }
	isProcessed := func() (*result, error) { return &result{false, true, false}, nil }
	shouldAttempt := func() (*result, error) { return &result{false, false, true}, nil }
	unexpectedError := func(err error) (*result, error) { return nil, err }
	res, err := sqlWithTx(r.db, func(tx *sql.Tx) (*result, error) {
		if _, err := tx.ExecContext(ctx, r.withSchema(setSerializableIsolationLevelQuery)); err != nil {
			return unexpectedError(err)
		}
		// attempt to find an existing idempotency key
		idempotencyKey, err := sqlFindOne(ctx, tx, r.withSchema(getIdempotencyKeyByKeyQuery),
			func(item *idempotencyKey) []any {
				return []any{&item.ID, &item.Key, &item.CreatedAt, &item.LockedAt, &item.IsProcessed}
			},
			key,
		)
		if err != nil {
			// this is the first attempt, create a new (locked) idempotency key
			if err == sql.ErrNoRows {
				now := time.Now()
				_, err := sqlCreate(ctx, tx, r.withSchema(createIdempotencyKeyQuery),
					key, now.UTC(), now.UTC(), false,
				)
				if err != nil {
					return unexpectedError(err)
				}
				// the idempotency key was created, let the client attempt processing it
				return shouldAttempt()
			}
			return unexpectedError(err)
		}
		if idempotencyKey.IsProcessed {
			// the idempotency key was found, and it is marked as processed
			return isProcessed()
		}
		// TODO: should `now` be passed in a parameter?
		locksValidAfter := time.Now().Add(-1 * r.keepLockedFor)
		if idempotencyKey.LockedAt != nil && idempotencyKey.LockedAt.After(locksValidAfter) {
			// the idempotency key was found and it is currently locked
			return isLocked()
		}
		// idempotency key was found, but it is free to be acquired for processing
		if _, err = tx.ExecContext(ctx, r.withSchema(lockIdempotencyKeyByKeyQuery),
			time.Now().UTC(), key,
		); err != nil {
			return unexpectedError(err)
		}
		return shouldAttempt()
	})
	if err != nil {
		if err, ok := err.(*pq.Error); ok && err.Code == "40001" {
			return errIdempotencyKeyRepositorySerialization
		}
		return err
	}
	if res.shouldAttempt {
		return nil
	}
	if res.isLocked {
		return errIdempotencyKeyRepositoryIsLocked
	}
	if res.isProcessed {
		return errIdempotencyKeyRepositoryIsProcessed
	}
	return errors.New("an unexpected begin result state")
}

func (r *idempotencyKeyRepository) commit(ctx context.Context, key string) error {
	_, err := sqlWithTx(r.db, func(tx *sql.Tx) (any, error) {
		query := r.withSchema(markIdempotencyKeyAsProcessedByKeyQuery)
		result, err := tx.ExecContext(ctx, query, key)
		if err != nil {
			return nil, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected != 1 {
			return nil, fmt.Errorf("expected affected rows to be exactly 1, got %d", rowsAffected)
		}
		return nil, nil
	})
	return err
}

func (r *idempotencyKeyRepository) rollback(ctx context.Context, key string) error {
	_, err := sqlWithTx(r.db, func(tx *sql.Tx) (any, error) {
		query := r.withSchema(deleteIdempotencyKeyByKeyQuery)
		result, err := tx.ExecContext(ctx, query, key)
		if err != nil {
			return nil, err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if rowsAffected != 1 {
			return nil, fmt.Errorf("expected affected rows to be exactly 1, got %d", rowsAffected)
		}
		return nil, nil
	})
	return err
}

func withIdempotent(db *sql.DB, schema string) events.OnMessageMiddleware {
	var (
		errBeingProcessed  = errors.New("an identical task is currently being processed")
		errLockNotAcquired = errors.New("the idempotency lock could not be acquired")
	)
	repo := &idempotencyKeyRepository{
		db:            db,
		schema:        schema,
		keepLockedFor: 10 * time.Second,
	}
	return func(next events.OnMessageHandler) events.OnMessageHandler {
		return func(ctx context.Context, delivery events.Delivery) error {
			// use the task's UUID as the idempotency key
			key := delivery.GetMessage().GetUUID()
			// attempt to acquire the idempotency key (retry `n` times if needed)
			var err error
			for i := 0; i < 3; i += 1 {
				err = repo.begin(ctx, key)
				if err == nil {
					break
				}
				if err == errIdempotencyKeyRepositorySerialization {
					// a conflict occurred with another worker -> should retry after a while (this should not happen in reality)
					time.Sleep(time.Duration(rand.Intn(41)+41) * time.Millisecond)
					continue
				}
				if err == errIdempotencyKeyRepositoryIsLocked {
					// the task is currently locked by another worker -> should retry after a while (this should not happen in reality)
					return errBeingProcessed
				}
				if err == errIdempotencyKeyRepositoryIsProcessed {
					// the task has already been processed -> can be marked as successful
					return nil
				}
				return errLockNotAcquired
			}
			if err != nil {
				if err == errIdempotencyKeyRepositorySerialization {
					return errBeingProcessed
				}
				return errLockNotAcquired
			}
			// the idempotency lock was acquired, execute the handler
			result := next(ctx, delivery)
			if result != nil {
				// the handler returned an error, release the idempotency key lock
				if err := repo.rollback(ctx, key); err != nil { //nolint
					// NOTE: nothing can really be done here, the lock will be released after a few seconds
				}
			} else {
				// the handler was successful, mark the idempotency key as processed
				if err := repo.commit(ctx, key); err != nil { //nolint
					// NOTE: nothing can really be done here... :/
				}
			}
			return result
		}
	}
}
