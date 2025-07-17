package op

import (
	"testing"

	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/internal/testutil"
)

func TestIf(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name: "create_case_when",
			Builder: If(
				Gte("age", 70), driver.Value("old"),
			).
				ElseIf(Gte("age", 30), driver.Value("middle")).
				Else(driver.Value("other")),
			ExpectedSql:  `CASE WHEN "age" >= ? THEN ? WHEN "age" >= ? THEN ? ELSE ? END`,
			ExpectedArgs: []any{70, "old", 30, "middle", "other"},
		},
		{
			Name: "handle_case_when_cond_error",
			Builder: If(
				Gte("unsafe+name", 70),
				driver.Value("old"),
			).Else(driver.Value("other")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
		{
			Name:         "handle_case_when_then_error",
			Builder:      If(Gte("name", 70), Column("unsafe+name")).Else(driver.Value("other")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
		{
			Name:         "handle_case_when_else_error",
			Builder:      If(Gte("name", 70), driver.Value(100)).Else(Column("unsafe+name")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "unsafe+name" contains illegal character '+'`,
		},
	})
}
