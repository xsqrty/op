package op

import "testing"

func TestFunctions(t *testing.T) {
	runCases(t, []testCase{
		{
			builder:      Func("CUSTOM", 1, "2"),
			expectedSql:  `CUSTOM(?,?)`,
			expectedArgs: []any{1, "2"},
		},
		{
			builder:      Func("CUSTOM", Column("colName"), "2"),
			expectedSql:  `CUSTOM("colName",?)`,
			expectedArgs: []any{"2"},
		},
		{
			builder:      FuncPrefix("CUSTOM", "DISTINCT", Column("colName"), "2"),
			expectedSql:  `CUSTOM(DISTINCT "colName",?)`,
			expectedArgs: []any{"2"},
		},
		{
			builder:      Cast(Column("colName"), "jsonb"),
			expectedSql:  `CAST("colName" AS jsonb)`,
			expectedArgs: []any(nil),
		},
		{
			builder:      Any(Select("Age").From("Users").Where(Ne("Age", nil))),
			expectedSql:  `ANY(SELECT "Age" FROM "Users" WHERE "Age" IS NOT NULL)`,
			expectedArgs: []any(nil),
		},
		{
			builder:      All(Select("Age").From("Users").Where(Ne("Age", nil))),
			expectedSql:  `ALL(SELECT "Age" FROM "Users" WHERE "Age" IS NOT NULL)`,
			expectedArgs: []any(nil),
		},
		{
			builder:      Concat("col1", Value(" "), Column("col2")),
			expectedSql:  `CONCAT("col1",?,"col2")`,
			expectedArgs: []any{" "},
		},
		{
			builder:      Coalesce("col1", "col2", Value("")),
			expectedSql:  `COALESCE("col1","col2",?)`,
			expectedArgs: []any{""},
		},
	})
}
