package opinionatedsagas

import (
	"database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	_ "github.com/lib/pq"
)

func TestMigrate(t *testing.T) {
	r := rand.New(rand.NewSource(8673244357))
	connectionString := "postgres://postgres:password@localhost:6632/dev?sslmode=disable"
	schema := fmt.Sprintf("opinionatedsagas_%d", r.Int())
	db, err := sql.Open("postgres", connectionString)
	assert.NoError(t, err)
	err = migrate(db, schema)
	assert.NoError(t, err)
}
