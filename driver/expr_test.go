package driver

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPure(t *testing.T) {
	sql, args, err := Pure("col > ? AND col < ?", 100, 200).Sql(NewPostgresSqlOptions())
	require.NoError(t, err)

	require.Equal(t, `col > ? AND col < ?`, sql)
	require.Equal(t, []any{100, 200}, args)
}

func TestValue(t *testing.T) {
	sql, args, err := Value([]byte{'n', 'a', 'm', 'e'}).Sql(NewPostgresSqlOptions())
	require.NoError(t, err)

	require.Equal(t, "?", sql)
	require.Equal(t, []any{[]byte{'n', 'a', 'm', 'e'}}, args)
}
