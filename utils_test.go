package op

import (
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op/driver"
	"testing"
)

func TestExprOrCol(t *testing.T) {
	t.Parallel()
	sql, args, err := exprOrCol("age", options)

	require.NoError(t, err)
	require.Equal(t, `"age"`, sql)
	require.Equal(t, []any(nil), args)

	sql, args, err = exprOrCol(driver.Pure("?", 100), options)

	require.NoError(t, err)
	require.Equal(t, "?", sql)
	require.Equal(t, []any{100}, args)

	sql, args, err = exprOrCol(100, options)

	require.EqualError(t, err, "unknown type: int")
	require.Equal(t, "", sql)
	require.Equal(t, []any(nil), args)
}

func TestExprOrVal(t *testing.T) {
	t.Parallel()
	sql, args, err := exprOrVal("age", options)

	require.NoError(t, err)
	require.Equal(t, "?", sql)
	require.Equal(t, []any{"age"}, args)

	sql, args, err = exprOrVal(Column("age"), options)

	require.NoError(t, err)
	require.Equal(t, `"age"`, sql)
	require.Equal(t, []any(nil), args)
}

func TestConcatUpdates(t *testing.T) {
	t.Parallel()
	sql, args, err := concatUpdates([]Column{"age", "name"}, []driver.Sqler{driver.Value(100), driver.Value("Alex")}, options)

	require.NoError(t, err)
	require.Equal(t, `"age"=?,"name"=?`, sql)
	require.Equal(t, []any{100, "Alex"}, args)

	sql, args, err = concatUpdates([]Column{"a+b"}, []driver.Sqler{driver.Value(100)}, options)

	require.EqualError(t, err, `target "a+b" contains illegal character '+'`)
	require.Equal(t, "", sql)
	require.Equal(t, []any(nil), args)

	sql, args, err = concatUpdates([]Column{"age"}, []driver.Sqler{Column("a+b")}, options)

	require.EqualError(t, err, `target "a+b" contains illegal character '+'`)
	require.Equal(t, "", sql)
	require.Equal(t, []any(nil), args)
}

func TestConcatFields(t *testing.T) {
	t.Parallel()
	sql, args, err := concatFields[Column]([]Column{"id", "age"}, options)

	require.NoError(t, err)
	require.Equal(t, `"id","age"`, sql)
	require.Equal(t, []any(nil), args)
}
