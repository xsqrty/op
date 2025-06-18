package op

import (
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/driver"
	"testing"
)

func TestExprOrCol(t *testing.T) {
	sql, args, err := exprOrCol("age", options)

	assert.NoError(t, err)
	assert.Equal(t, `"age"`, sql)
	assert.Equal(t, []any(nil), args)

	sql, args, err = exprOrCol(driver.Pure("?", 100), options)

	assert.NoError(t, err)
	assert.Equal(t, "?", sql)
	assert.Equal(t, []any{100}, args)

	sql, args, err = exprOrCol(100, options)

	assert.EqualError(t, err, "unknown type: int")
	assert.Equal(t, "", sql)
	assert.Equal(t, []any(nil), args)
}

func TestExprOrVal(t *testing.T) {
	sql, args, err := exprOrVal("age", options)

	assert.NoError(t, err)
	assert.Equal(t, "?", sql)
	assert.Equal(t, []any{"age"}, args)

	sql, args, err = exprOrVal(Column("age"), options)

	assert.NoError(t, err)
	assert.Equal(t, `"age"`, sql)
	assert.Equal(t, []any(nil), args)
}

func TestConcatUpdates(t *testing.T) {
	sql, args, err := concatUpdates([]Column{"age", "name"}, []driver.Sqler{driver.Value(100), driver.Value("Alex")}, options)

	assert.NoError(t, err)
	assert.Equal(t, `"age"=?,"name"=?`, sql)
	assert.Equal(t, []any{100, "Alex"}, args)

	sql, args, err = concatUpdates([]Column{"a+b"}, []driver.Sqler{driver.Value(100)}, options)

	assert.EqualError(t, err, `target "a+b" contains illegal character '+'`)
	assert.Equal(t, "", sql)
	assert.Equal(t, []any(nil), args)

	sql, args, err = concatUpdates([]Column{"age"}, []driver.Sqler{Column("a+b")}, options)

	assert.EqualError(t, err, `target "a+b" contains illegal character '+'`)
	assert.Equal(t, "", sql)
	assert.Equal(t, []any(nil), args)
}

func TestConcatFields(t *testing.T) {
	sql, args, err := concatFields[Column]([]Column{"id", "age"}, options)

	assert.NoError(t, err)
	assert.Equal(t, `"id","age"`, sql)
	assert.Equal(t, []any(nil), args)
}
