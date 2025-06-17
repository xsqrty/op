package op

import (
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/testutil"
	"testing"
)

func TestColumn(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "column",
			Builder:      Column("name"),
			ExpectedSql:  `"name"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "composite_column",
			Builder:      Column("table.target"),
			ExpectedSql:  `"table"."target"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "column_error",
			Builder:      Column("name*123"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "name*123" contains illegal character '*'`,
		},
	})

	assert.True(t, Column("").IsZero())
	assert.True(t, ColumnAlias("col").IsPure())
}

func TestAlias(t *testing.T) {
	al := ColumnAlias("target")
	al.Rename("table.target")

	al2 := As("name", Column("target"))
	al2.Rename("newName")

	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "alias",
			Builder:      As("UserName", Column("Name")),
			ExpectedSql:  `("Name") AS "UserName"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "alias_with_subquery",
			Builder:      As("SubQuery", Select("Id", "Name").From("Users").Where(Eq("Id", 1))),
			ExpectedSql:  `(SELECT "Id","Name" FROM "Users" WHERE "Id" = ?) AS "SubQuery"`,
			ExpectedArgs: []any{1},
		},
		{
			Name:         "alias_for_column",
			Builder:      ColumnAlias("Users.Name"),
			ExpectedSql:  `"Users"."Name"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "alias_error",
			Builder:      As("unsafe+name", Column("Col")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `name "unsafe+name" contains illegal character '+'`,
		},
		{
			Name:         "alias_col_rename",
			Builder:      al,
			ExpectedSql:  `"table"."target"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "alias_expr_rename",
			Builder:      al2,
			ExpectedSql:  `("target") AS "newName"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "alias_unknown_type",
			Builder:      As("Name", Select(10).From("Users")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `unknown type: int must be a string or Alias`,
		},
		{
			Name:         "alias_no_target",
			Builder:      &alias{pure: true},
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `no target found in name`,
		},
	})
}
