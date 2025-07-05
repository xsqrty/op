package op

import (
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestArraySql(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "create_postgres_array",
			Builder:      Array([]any{"a", 1, []byte{'1', '2', '3'}}...),
			ExpectedSql:  `ARRAY[?,?,?]`,
			ExpectedArgs: []any{"a", 1, []byte{'1', '2', '3'}},
		},
	})
}

func TestArrayError(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "handle_postgres_array_error",
			Builder:      Array([]any{Column("unsafe+name"), "a", 1, []byte{'1', '2', '3'}}...),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
	})
}

func TestArrayLength(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "array_length",
			Builder:      ArrayLength(Array([]any{"a", 1, []byte{'1', '2', '3'}}...)),
			ExpectedSql:  `ARRAY_LENGTH(ARRAY[?,?,?],?)`,
			ExpectedArgs: []any{"a", 1, []byte{'1', '2', '3'}, 1},
		},
	})
}

func TestArrayConcat(t *testing.T) {
	t.Parallel()
	arrayArgs := []any{"a", 1, []byte{'1', '2', '3'}}
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "array_concat",
			Builder:      ArrayConcat(Array(arrayArgs...), Array(arrayArgs...)),
			ExpectedSql:  `ARRAY_CAT(ARRAY[?,?,?],ARRAY[?,?,?])`,
			ExpectedArgs: []any{"a", 1, []byte{'1', '2', '3'}, "a", 1, []byte{'1', '2', '3'}},
		},
	})
}

func TestArrayUnnest(t *testing.T) {
	t.Parallel()
	arrayArgs := []any{"a", 1, []byte{'1', '2', '3'}}
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "array_unnest",
			Builder:      ArrayUnnest(Array(arrayArgs...)),
			ExpectedSql:  `UNNEST(ARRAY[?,?,?])`,
			ExpectedArgs: []any{"a", 1, []byte{'1', '2', '3'}},
		},
	})
}
