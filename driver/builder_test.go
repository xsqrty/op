package driver

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type errorSqler struct{}

func (e errorSqler) Sql(_ *SqlOptions) (string, []any, error) {
	return "", nil, errors.New("error")
}

func TestSql(t *testing.T) {
	t.Parallel()

	options := NewSqlOptions(
		WithPlaceholderFormat(func(n int) string {
			return "$" + strconv.Itoa(n)
		}),
	)

	sql, args, err := Sql(Pure("?? ?,?,?", 1, "2", 3.01), options)
	require.NoError(t, err)
	require.Equal(t, "?? $1,$2,$3", sql)
	require.Equal(t, []any{1, "2", 3.01}, args)

	sql, args, err = Sql(errorSqler{}, options)
	require.EqualError(t, err, "error")
	require.Equal(t, "", sql)
	require.Equal(t, []any(nil), args)

	sql, args, err = Sql(Pure("?,?,?", 1, "2", 3.01), &SqlOptions{})
	require.NoError(t, err)
	require.Equal(t, "?,?,?", sql)
	require.Equal(t, []any{1, "2", 3.01}, args)
}

func TestSqlWithPlaceholders(t *testing.T) {
	t.Parallel()
	sql, args, err := Sql(
		Pure("?? ?,?,?", 1, "2", 3.01),
		NewSqlOptions(WithPlaceholderFormat(func(i int) string {
			return fmt.Sprintf("@%d", i)
		})),
	)
	require.NoError(t, err)
	require.Equal(t, "?? @1,@2,@3", sql)
	require.Equal(t, []any{1, "2", 3.01}, args)
}
