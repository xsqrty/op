package op

import (
	"testing"

	"github.com/xsqrty/op/internal/testutil"
)

func TestList(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "list",
			Builder:      list{1, 2, "3", Column("test")},
			ExpectedSql:  `?,?,?,"test"`,
			ExpectedArgs: []any{1, 2, "3"},
		},
	})
}
