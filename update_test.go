package op

import (
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/testutil"
	"testing"
)

func TestUpdate(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "update",
			Builder:      Update("users", Updates{"key": "value"}),
			ExpectedSql:  `UPDATE "users" SET "key"=?`,
			ExpectedArgs: []any{"value"},
		},
		{
			Name:    "update_many_args",
			Builder: Update("users", Updates{"key1": "value", "key2": "value"}),
			ExpectedSqls: []string{
				`UPDATE "users" SET "key1"=?,"key2"=?`,
				`UPDATE "users" SET "key2"=?,"key1"=?`,
			},
			ExpectedArgs: []any{"value", "value"},
		},
		{
			Name:         "update_column",
			Builder:      Update(ColumnAlias("users"), Updates{"key": "value"}),
			ExpectedSql:  `UPDATE "users" SET "key"=?`,
			ExpectedArgs: []any{"value"},
		},
		{
			Name:         "update_where",
			Builder:      Update(ColumnAlias("users"), Updates{"key": "value"}).Where(Like("name", "Al%")),
			ExpectedSql:  `UPDATE "users" SET "key"=? WHERE "name" LIKE ?`,
			ExpectedArgs: []any{"value", "Al%"},
		},
		{
			Name:         "update_where",
			Builder:      Update(ColumnAlias("users"), Updates{"key": "value"}).Where(Like("name", "Al%")).Returning("id", ColumnAlias("name")),
			ExpectedSql:  `UPDATE "users" SET "key"=? WHERE "name" LIKE ? RETURNING "id","name"`,
			ExpectedArgs: []any{"value", "Al%"},
		},
		{
			Name:         "error_table_1",
			Builder:      Update("a+b", Updates{"key": "value"}),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_table_2",
			Builder:      Update(100, Updates{"key": "value"}),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "error_returning_1",
			Builder:      Update("users", Updates{"key": "value"}).Returning("a+b"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_returning_2",
			Builder:      Update("users", Updates{"key": "value"}).Returning(10),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "error_set_data",
			Builder:      Update("users", Updates{"a+b": "value"}),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_where",
			Builder:      Update("users", Updates{"key": "value"}).Where(Eq("a+b", 1)),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
	})
}

func TestUpdateUsingTables(t *testing.T) {
	t.Parallel()
	item := Update("users", Updates{})
	tables := item.UsingTables()

	assert.Equal(t, []string{"users"}, tables)
	assert.Equal(t, "users", item.With())
}

func TestUpdateReturning(t *testing.T) {
	t.Parallel()
	item := Update("users", Updates{}).Returning("id")
	item.LimitReturningOne()

	assert.Equal(t, []Alias{ColumnAlias("id")}, item.GetReturning())

	item.SetReturning([]any{"id", "age"})
	assert.Equal(t, []Alias{ColumnAlias("id"), ColumnAlias("age")}, item.GetReturning())

	item.SetReturningAliases([]Alias{ColumnAlias("col2")})
	assert.Equal(t, []Alias{ColumnAlias("col2")}, item.GetReturning())
}
