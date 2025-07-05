package op

import (
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestOperators(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "like",
			Builder:      Like("name", "alex"),
			ExpectedSql:  `"name" LIKE ?`,
			ExpectedArgs: []any{"alex"},
		},
		{
			Name:         "not_like",
			Builder:      NotLike("name", "alex"),
			ExpectedSql:  `"name" NOT LIKE ?`,
			ExpectedArgs: []any{"alex"},
		},
		{
			Name:         "case_insensitive_like",
			Builder:      ILike("name", "alex"),
			ExpectedSql:  `"name" ILIKE ?`,
			ExpectedArgs: []any{"alex"},
		},
		{
			Name:         "case_insensitive_not_like",
			Builder:      NotILike("name", "alex"),
			ExpectedSql:  `"name" NOT ILIKE ?`,
			ExpectedArgs: []any{"alex"},
		},
		{
			Name:         "eq",
			Builder:      Eq("id", 1),
			ExpectedSql:  `"id" = ?`,
			ExpectedArgs: []any{1},
		},
		{
			Name:         "is_null",
			Builder:      Eq("id", nil),
			ExpectedSql:  `"id" IS NULL`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "not_eq",
			Builder:      Ne("id", 1),
			ExpectedSql:  `"id" != ?`,
			ExpectedArgs: []any{1},
		},
		{
			Name:         "is_not_null",
			Builder:      Ne("id", nil),
			ExpectedSql:  `"id" IS NOT NULL`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "lt",
			Builder:      Lt("age", 10),
			ExpectedSql:  `"age" < ?`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "gt",
			Builder:      Gt("age", 10),
			ExpectedSql:  `"age" > ?`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "lte",
			Builder:      Lte("age", 10),
			ExpectedSql:  `"age" <= ?`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "gte",
			Builder:      Gte("age", 10),
			ExpectedSql:  `"age" >= ?`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "lc_1",
			Builder:      Lc("Roles", []string{"admin", "super"}),
			ExpectedSql:  `"Roles" @> ?`,
			ExpectedArgs: []any{[]string{"admin", "super"}},
		},
		{
			Name:         "lc_2",
			Builder:      Lc(driver.Value([]string{"admin", "super"}), Column("Roles")),
			ExpectedSql:  `? @> "Roles"`,
			ExpectedArgs: []any{[]string{"admin", "super"}},
		},
		{
			Name:         "rc_1",
			Builder:      Rc("Roles", []string{"admin", "super"}),
			ExpectedSql:  `"Roles" <@ ?`,
			ExpectedArgs: []any{[]string{"admin", "super"}},
		},
		{
			Name:         "rc_2",
			Builder:      Rc(driver.Value([]string{"admin", "super"}), Column("Roles")),
			ExpectedSql:  `? <@ "Roles"`,
			ExpectedArgs: []any{[]string{"admin", "super"}},
		},
		{
			Name:         "extract_text",
			Builder:      ExtractText("json", "user", "name"),
			ExpectedSql:  `"json" #>> ARRAY[?,?]`,
			ExpectedArgs: []any{"user", "name"},
		},
		{
			Name:         "extract_text",
			Builder:      ExtractObject("json", "user", "name"),
			ExpectedSql:  `"json" #> ARRAY[?,?]`,
			ExpectedArgs: []any{"user", "name"},
		},
		{
			Name:         "has_prop",
			Builder:      HasProp("json", "user", "name"),
			ExpectedSql:  `"json" ?| ARRAY[?,?]`,
			ExpectedArgs: []any{"user", "name"},
		},
		{
			Name:         "has_props",
			Builder:      HasProps("json", "user", "name"),
			ExpectedSql:  `"json" ?& ARRAY[?,?]`,
			ExpectedArgs: []any{"user", "name"},
		},
		{
			Name:         "in",
			Builder:      In("id", 1, 2, 3),
			ExpectedSql:  `"id" IN (?,?,?)`,
			ExpectedArgs: []any{1, 2, 3},
		},
		{
			Name:         "not_in",
			Builder:      Nin("id", 1, 2, 3),
			ExpectedSql:  `"id" NOT IN (?,?,?)`,
			ExpectedArgs: []any{1, 2, 3},
		},
		{
			Name:         "in_subquery",
			Builder:      In("id", Select("id").From("users")),
			ExpectedSql:  `"id" IN (SELECT "id" FROM "users")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "error_1",
			Builder:      Lt("age+age", 10),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "age+age" contains illegal character '+'`,
		},
		{
			Name:         "error_2",
			Builder:      Lt("age", Column("age+age")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "age+age" contains illegal character '+'`,
		},
	})
}
