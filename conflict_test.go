package op

import (
	"testing"

	"github.com/xsqrty/op/internal/testutil"
)

func TestConflict(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "do_nothing",
			Builder:      DoNothing(),
			ExpectedSql:  "NOTHING",
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "excluded",
			Builder:      Excluded("ColName"),
			ExpectedSql:  `EXCLUDED."ColName"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "handle_excluded_error",
			Builder:      Excluded("Col+Name"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "Col+Name" contains illegal character '+'`,
		},
		{
			Name: "do_update",
			Builder: DoUpdate(Updates{
				"Id":   Excluded("Id"),
				"Name": Excluded("Name"),
			}),
			ExpectedArgs: []any(nil),
			ExpectedSqls: []string{
				`UPDATE SET "Id"=EXCLUDED."Id","Name"=EXCLUDED."Name"`,
				`UPDATE SET "Name"=EXCLUDED."Name","Id"=EXCLUDED."Id"`,
			},
		},
	})
}
