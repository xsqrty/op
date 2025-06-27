package op

import (
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestList(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "list",
			Builder:      list{1, 2, "3", Column("test")},
			ExpectedSql:  `?,?,?,"test"`,
			ExpectedArgs: []any{1, 2, "3"},
		},
	})
}
