package op

import (
	"github.com/xsqrty/op/testutil"
	"testing"
)

func TestMath(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "add",
			Builder:      Add("col1", "col2", 10),
			ExpectedSql:  `("col1"+"col2"+?)`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "sub",
			Builder:      Sub("col1", "col2", 10),
			ExpectedSql:  `("col1"-"col2"-?)`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "div",
			Builder:      Div("col1", "col2", 10),
			ExpectedSql:  `("col1"/"col2"/?)`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "mul",
			Builder:      Mul("col1", "col2", 10),
			ExpectedSql:  `("col1"*"col2"*?)`,
			ExpectedArgs: []any{10},
		},
		{
			Name:         "composite",
			Builder:      Mul("col1", Add("col2", "col3", 10)),
			ExpectedSql:  `("col1"*("col2"+"col3"+?))`,
			ExpectedArgs: []any{10},
		},
	})
}
