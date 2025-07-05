package op

import (
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestGroup(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "or",
			Builder:      Or{Eq("k", 100), Eq("b", 200)},
			ExpectedSql:  `("k" = ? OR "b" = ?)`,
			ExpectedArgs: []any{100, 200},
		},
		{
			Name:         "or_one_arg",
			Builder:      Or{Eq("k", 100)},
			ExpectedSql:  `"k" = ?`,
			ExpectedArgs: []any{100},
		},
		{
			Name:         "and",
			Builder:      And{Eq("k", 100), Eq("b", 200)},
			ExpectedSql:  `("k" = ? AND "b" = ?)`,
			ExpectedArgs: []any{100, 200},
		},
		{
			Name:         "and_one_arg",
			Builder:      And{Eq("k", 100)},
			ExpectedSql:  `"k" = ?`,
			ExpectedArgs: []any{100},
		},
		{
			Name:         "or_and",
			Builder:      Or{Eq("a", 100), And{Eq("b", 200), Eq("c", 300)}},
			ExpectedSql:  `("a" = ? OR ("b" = ? AND "c" = ?))`,
			ExpectedArgs: []any{100, 200, 300},
		},
		{
			Name:         "handle_error",
			Builder:      Or{Eq("col+col", 100)},
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "col+col" contains illegal character '+'`,
		},
	})
}
