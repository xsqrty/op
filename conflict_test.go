package op

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDoNothing(t *testing.T) {
	sql, args, err := DoNothing().Sql(options)

	assert.NoError(t, err)

	expectedSql := "NOTHING"
	expectedArgs := []any(nil)

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestExcluded(t *testing.T) {
	sql, args, err := Excluded("ColName").Sql(options)

	assert.NoError(t, err)

	expectedSql := `EXCLUDED."ColName"`
	expectedArgs := []any(nil)

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestExcludedError(t *testing.T) {
	_, _, err := Excluded("Col+Name").Sql(options)

	assert.EqualError(t, err, `target "Col+Name" contains illegal character '+'`)
}

func TestDoUpdate(t *testing.T) {
	sql, args, err := DoUpdate(Updates{
		"Id":   Excluded("Id"),
		"Name": Excluded("Name"),
	}).Sql(options)

	assert.NoError(t, err)

	expectedSqls := []string{
		`UPDATE SET "Id"=EXCLUDED."Id","Name"=EXCLUDED."Name"`,
		`UPDATE SET "Name"=EXCLUDED."Name","Id"=EXCLUDED."Id"`,
	}
	expectedArgs := []any(nil)

	assert.Condition(t, func() (success bool) {
		for _, expectedSql := range expectedSqls {
			if sql == expectedSql {
				return true
			}
		}

		return false
	})
	assert.Equal(t, expectedArgs, args)
}
