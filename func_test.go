package op

import (
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/testutil"
	"testing"
)

func TestFunctions(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "custom_1",
			Builder:      Func("CUSTOM", 1, "2"),
			ExpectedSql:  `CUSTOM(?,?)`,
			ExpectedArgs: []any{1, "2"},
		},
		{
			Name:         "custom_2",
			Builder:      Func("CUSTOM", Column("colName"), "2"),
			ExpectedSql:  `CUSTOM("colName",?)`,
			ExpectedArgs: []any{"2"},
		},
		{
			Name:         "custom_prefix",
			Builder:      FuncPrefix("CUSTOM", "DISTINCT", Column("colName"), "2"),
			ExpectedSql:  `CUSTOM(DISTINCT "colName",?)`,
			ExpectedArgs: []any{"2"},
		},
		{
			Name:         "cast",
			Builder:      Cast(Column("colName"), "jsonb"),
			ExpectedSql:  `CAST("colName" AS jsonb)`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "cast_value",
			Builder:      Cast(100, "jsonb"),
			ExpectedSql:  `CAST(? AS jsonb)`,
			ExpectedArgs: []any{100},
		},
		{
			Name:         "handle_cast_error",
			Builder:      Cast("col+col", "jsonb"),
			ExpectedErr:  `target "col+col" contains illegal character '+'`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "handle_cast_error_2",
			Builder:      Cast("col+col", "jsonb"),
			ExpectedErr:  `cast format is not described in sql options`,
			ExpectedArgs: []any(nil),
			SqlOptions:   driver.NewSqlOptions(),
		},
		{
			Name:         "any",
			Builder:      Any(Select("Age").From("Users").Where(Ne("Age", nil))),
			ExpectedSql:  `ANY(SELECT "Age" FROM "Users" WHERE "Age" IS NOT NULL)`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "all",
			Builder:      All(Select("Age").From("Users").Where(Ne("Age", nil))),
			ExpectedSql:  `ALL(SELECT "Age" FROM "Users" WHERE "Age" IS NOT NULL)`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "concat",
			Builder:      Concat("col1", driver.Value(" "), Column("col2")),
			ExpectedSql:  `CONCAT("col1",?,"col2")`,
			ExpectedArgs: []any{" "},
		},
		{
			Name:         "coalesce",
			Builder:      Coalesce("col1", "col2", driver.Value("")),
			ExpectedSql:  `COALESCE("col1","col2",?)`,
			ExpectedArgs: []any{""},
		},
		{
			Name:         "lower",
			Builder:      Lower("col"),
			ExpectedSql:  `LOWER("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "upper",
			Builder:      Upper("col"),
			ExpectedSql:  `LOWER("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "min",
			Builder:      Min("col"),
			ExpectedSql:  `MIN("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "max",
			Builder:      Max("col"),
			ExpectedSql:  `MAX("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "sum",
			Builder:      Sum("col"),
			ExpectedSql:  `SUM("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "avg",
			Builder:      Avg("col"),
			ExpectedSql:  `AVG("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "abs",
			Builder:      Abs("col"),
			ExpectedSql:  `ABS("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "count",
			Builder:      Count("col"),
			ExpectedSql:  `COUNT("col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "count_distinct",
			Builder:      CountDistinct("col"),
			ExpectedSql:  `COUNT(DISTINCT "col")`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "handle_count_error",
			Builder:      Count("col+col"),
			ExpectedErr:  `target "col+col" contains illegal character '+'`,
			ExpectedArgs: []any(nil),
		},
	})
}
