package op

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumn(t *testing.T) {
	runCases(t, []testCase{
		{
			builder:      Column("name"),
			expectedSql:  `"name"`,
			expectedArgs: []any(nil),
		},
		{
			builder:      Column("table.target"),
			expectedSql:  `"table"."target"`,
			expectedArgs: []any(nil),
		},
		{
			builder:      Column("name*123"),
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `target "name*123" contains illegal character '*'`,
		},
	})
}

func TestAlias(t *testing.T) {
	al := columnAlias("target")
	al.Rename("table.target")

	runCases(t, []testCase{
		{
			builder:      As("UserName", Column("Name")),
			expectedSql:  `("Name") AS "UserName"`,
			expectedArgs: []any(nil),
		},
		{
			builder:      As("SubQuery", Select("Id", "Name").From("Users").Where(Eq("Id", 1))),
			expectedSql:  `(SELECT "Id","Name" FROM "Users" WHERE "Id" = ?) AS "SubQuery"`,
			expectedArgs: []any{1},
		},
		{
			builder:      columnAlias("Users.Name"),
			expectedSql:  `"Users"."Name"`,
			expectedArgs: []any(nil),
		},
		{
			builder:      As("unsafe+name", Column("Col")),
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `name "unsafe+name" contains illegal character '+'`,
		},
		{
			builder:      al,
			expectedSql:  `"table"."target"`,
			expectedArgs: []any(nil),
		},
		{
			builder:      As("Name", Select(10).From("Users")),
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `unknown type: int must be a string or Alias`,
		},
		{
			builder:      &alias{pure: true},
			expectedSql:  "",
			expectedArgs: []any(nil),
			expectedErr:  `no target found in name`,
		},
	})
}

func TestColumnAlias(t *testing.T) {
	al := columnAlias("Users.Name").Alias()
	assert.Equal(t, "Users.Name", al)

	al = As("Rename", Column("name")).Alias()
	assert.Equal(t, "Rename", al)
}
