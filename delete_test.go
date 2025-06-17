package op

import (
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/testutil"
	"testing"
)

func TestDelete(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "delete",
			Builder:      Delete("users").Where(Ne("DeletedAt", nil)),
			ExpectedSql:  `DELETE FROM "users" WHERE "DeletedAt" IS NOT NULL`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "delete_returning",
			Builder:      Delete("users").Where(Ne("Id", 1)).Returning("Id", ColumnAlias("Name")),
			ExpectedSql:  `DELETE FROM "users" WHERE "Id" != ? RETURNING "Id","Name"`,
			ExpectedArgs: []any{1},
		},
		{
			Name:         "delete_alias",
			Builder:      Delete(ColumnAlias("users")).Where(Ne("Id", 1)).Returning("Id", "Name"),
			ExpectedSql:  `DELETE FROM "users" WHERE "Id" != ? RETURNING "Id","Name"`,
			ExpectedArgs: []any{1},
		},
		{
			Name:         "error_table_1",
			Builder:      Delete(10),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "error_table_2",
			Builder:      Delete("a+b"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_where",
			Builder:      Delete("users").Where(Eq("a+b", 10)),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_returning_1",
			Builder:      Delete("users").Returning("a+b"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_returning_2",
			Builder:      Delete("users").Returning(100),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
	})
}

func TestDeleteUsingTables(t *testing.T) {
	t.Parallel()
	item := Delete("users")
	tables := item.UsingTables()

	assert.Equal(t, []string{"users"}, tables)
	assert.Equal(t, "users", item.With())
}

func TestDeleteReturning(t *testing.T) {
	t.Parallel()
	item := Delete("users").Returning("id")
	item.LimitReturningOne()

	assert.Equal(t, []Alias{ColumnAlias("id")}, item.GetReturning())

	item.SetReturning([]any{"id", "age"})
	assert.Equal(t, []Alias{ColumnAlias("id"), ColumnAlias("age")}, item.GetReturning())

	item.SetReturningAliases([]Alias{ColumnAlias("col2")})
	assert.Equal(t, []Alias{ColumnAlias("col2")}, item.GetReturning())
}
