package testutil

import (
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/driver"
	"testing"
)

type TestCase struct {
	Name         string
	Builder      driver.Sqler
	ExpectedSqls []string
	ExpectedSql  string
	ExpectedErr  string
	ExpectedArgs any
	SqlOptions   *driver.SqlOptions
}

func RunCases(t *testing.T, options *driver.SqlOptions, testCases []TestCase) {
	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			ops := options
			if c.SqlOptions != nil {
				ops = c.SqlOptions
			}

			sql, args, err := c.Builder.Sql(ops)
			if c.ExpectedErr != "" {
				assert.EqualError(t, err, c.ExpectedErr)
			} else {
				assert.NoError(t, err)
			}

			if len(c.ExpectedSqls) > 0 {
				assert.Condition(t, func() bool {
					for _, expectedSql := range c.ExpectedSqls {
						if sql == expectedSql {
							return true
						}
					}

					return false
				})
			} else {
				assert.Equal(t, c.ExpectedSql, sql)
			}

			assert.Equal(t, c.ExpectedArgs, args)
		})
	}
}
