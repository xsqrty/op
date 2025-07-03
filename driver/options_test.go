package driver

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewPostgresSqlOptions(t *testing.T) {
	options := NewPostgresSqlOptions()
	sql, args, err := Sql(Pure("?", 1), options)
	cast := options.CastFormat(sql, "INTEGER")

	require.NoError(t, err)
	require.Equal(t, "$1::INTEGER", cast)
	require.Equal(t, []any{1}, args)
}

func TestNewSqliteSqlOptions(t *testing.T) {
	options := NewSqliteSqlOptions()
	sql, args, err := Sql(Pure("?", 1), options)
	cast := options.CastFormat(sql, "INTEGER")

	require.NoError(t, err)
	require.Equal(t, "CAST($1 AS INTEGER)", cast)
	require.Equal(t, []any{1}, args)
}
