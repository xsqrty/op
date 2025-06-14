package op

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestArraySql(t *testing.T) {
	arrayArgs := []any{"a", 1, []byte{'1', '2', '3'}}
	sql, args, err := Array(arrayArgs...).Sql(options)

	assert.NoError(t, err)

	expectedSql := `ARRAY[?,?,?]`
	expectedArgs := []any{"a", 1, []byte{'1', '2', '3'}}

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestArrayError(t *testing.T) {
	arrayArgs := []any{Column("unsafe+name"), "a", 1, []byte{'1', '2', '3'}}
	_, _, err := Array(arrayArgs...).Sql(options)

	assert.EqualError(t, err, `target "unsafe+name" contains illegal character '+'`)
}

func TestArrayLength(t *testing.T) {
	arrayArgs := []any{"a", 1, []byte{'1', '2', '3'}}
	sql, args, err := ArrayLength(Array(arrayArgs...)).Sql(options)

	assert.NoError(t, err)

	expectedSql := `ARRAY_LENGTH(ARRAY[?,?,?],?)`
	expectedArgs := []any{"a", 1, []byte{'1', '2', '3'}, 1}

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestArrayConcat(t *testing.T) {
	arrayArgs := []any{"a", 1, []byte{'1', '2', '3'}}
	sql, args, err := ArrayConcat(Array(arrayArgs...), Array(arrayArgs...)).Sql(options)

	assert.NoError(t, err)

	expectedSql := `ARRAY_CAT(ARRAY[?,?,?],ARRAY[?,?,?])`
	expectedArgs := []any{"a", 1, []byte{'1', '2', '3'}, "a", 1, []byte{'1', '2', '3'}}

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestArrayUnnest(t *testing.T) {
	arrayArgs := []any{"a", 1, []byte{'1', '2', '3'}}
	sql, args, err := ArrayUnnest(Array(arrayArgs...)).Sql(options)

	assert.NoError(t, err)

	expectedSql := `UNNEST(ARRAY[?,?,?])`
	expectedArgs := []any{"a", 1, []byte{'1', '2', '3'}}

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}
