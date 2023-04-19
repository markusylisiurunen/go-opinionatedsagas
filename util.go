package opinionatedsagas

import (
	"context"
	"database/sql"
	"reflect"
)

func isInterface[T any](t reflect.Type) bool {
	return t.Implements(reflect.TypeOf((*T)(nil)).Elem())
}

func sqlWithTx[R any](db *sql.DB, do func(tx *sql.Tx) (R, error)) (R, error) {
	var zero R
	tx, err := db.Begin()
	defer tx.Rollback() //nolint the error is not relevant
	if err != nil {
		return zero, err
	}
	r, err := do(tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	return r, nil
}

func sqlCreate(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	row := tx.QueryRowContext(ctx, query, args...)
	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func sqlFindOne[R any](
	ctx context.Context, tx *sql.Tx, query string, fields func(item *R) []any, args ...any,
) (R, error) {
	row := tx.QueryRowContext(ctx, query, args...)
	var item, zero R
	if err := row.Scan(fields(&item)...); err != nil {
		return zero, err
	}
	return item, nil
}
