package driver

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPure(t *testing.T) {
	t.Parallel()
	sql, args, err := Pure("col > ? AND col < ?", 100, 200).Sql(NewPostgresSqlOptions())
	assert.NoError(t, err)

	assert.Equal(t, `col > ? AND col < ?`, sql)
	assert.Equal(t, []any{100, 200}, args)
}

func TestValue(t *testing.T) {
	t.Parallel()
	sql, args, err := Value([]byte{'n', 'a', 'm', 'e'}).Sql(NewPostgresSqlOptions())
	assert.NoError(t, err)

	assert.Equal(t, "?", sql)
	assert.Equal(t, []any{[]byte{'n', 'a', 'm', 'e'}}, args)
}
