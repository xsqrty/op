package driver

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewPostgresSqlOptions(t *testing.T) {
	options := NewPostgresSqlOptions()
	sql, args, err := Sql(Pure("?", 1), options)
	cast := options.CastFormat(sql, "INTEGER")

	assert.NoError(t, err)
	assert.Equal(t, "$1::INTEGER", cast)
	assert.Equal(t, []any{1}, args)
}

func TestNewSqliteSqlOptions(t *testing.T) {
	options := NewSqliteSqlOptions()
	sql, args, err := Sql(Pure("?", 1), options)
	cast := options.CastFormat(sql, "INTEGER")

	assert.NoError(t, err)
	assert.Equal(t, "CAST($1 AS INTEGER)", cast)
	assert.Equal(t, []any{1}, args)
}
