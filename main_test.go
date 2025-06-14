package op

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/driver"
	"testing"
)

var options *driver.SqlOptions

type testCase struct {
	builder      driver.Sqler
	expectedSql  string
	expectedErr  string
	expectedArgs any
}

func TestMain(m *testing.M) {
	options = driver.NewSqlOptions(
		driver.WithSafeColumns(),
		driver.WithColumnsDelim('.'),
		driver.WithFieldsDelim(','),
		driver.WithWrapColumn('"', '"'),
		driver.WithWrapAlias('"', '"'),
		driver.WithCastFormat(func(val string, typ string) string {
			return fmt.Sprintf("CAST(%s AS %s)", val, typ)
		}),
		driver.WithPlaceholderFormat(func(n int) string {
			return fmt.Sprintf("$%d", n)
		}),
	)

	m.Run()
}

func runCases(t *testing.T, testCases []testCase) {
	for _, c := range testCases {
		sql, args, err := c.builder.Sql(options)

		if c.expectedErr != "" {
			assert.EqualError(t, err, c.expectedErr)
		} else {
			assert.NoError(t, err)
		}
		
		assert.Equal(t, c.expectedSql, sql)
		assert.Equal(t, c.expectedArgs, args)
	}
}
