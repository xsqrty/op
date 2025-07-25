package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op/driver"
)

// TestCase represents a test case structure for verifying SQL queries generated by a driver.Sqler implementation.
type TestCase struct {
	Name         string
	Builder      driver.Sqler
	ExpectedSqls []string
	ExpectedSql  string
	ExpectedErr  string
	ExpectedArgs any
	SqlOptions   *driver.SqlOptions
}

// RunCases runs a series of test cases, each defined by a TestCase instance, in parallel using the provided options.
// It verifies generated SQL queries, arguments, and errors against the expectations defined in each test case.
func RunCases(t *testing.T, options *driver.SqlOptions, testCases []TestCase) {
	t.Helper()
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			ops := options
			if c.SqlOptions != nil {
				ops = c.SqlOptions
			}

			sql, args, err := c.Builder.Sql(ops)
			if c.ExpectedErr != "" {
				require.EqualError(t, err, c.ExpectedErr)
			} else {
				require.NoError(t, err)
			}

			if len(c.ExpectedSqls) > 0 {
				require.Condition(t, func() bool {
					for _, expectedSql := range c.ExpectedSqls {
						if sql == expectedSql {
							return true
						}
					}

					return false
				})
			} else {
				require.Equal(t, c.ExpectedSql, sql)
			}

			require.Equal(t, c.ExpectedArgs, args)
		})
	}
}
