package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op/driver"
)

func TestNewSqlOptions(t *testing.T) {
	options := NewSqlOptions()
	sql, args, err := driver.Sql(driver.Pure("?", 1), options)
	cast := options.CastFormat(sql, "INTEGER")

	require.NoError(t, err)
	require.Equal(t, "$1::INTEGER", cast)
	require.Equal(t, []any{1}, args)
}
