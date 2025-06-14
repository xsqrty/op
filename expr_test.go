package op

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPure(t *testing.T) {
	sql, args, err := Pure("col > ? AND col < ?", 100, 200).Sql(options)

	assert.NoError(t, err)

	expectedSql := `col > ? AND col < ?`
	expectedArgs := []any{100, 200}

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}

func TestValue(t *testing.T) {
	sql, args, err := Value([]byte{'n', 'a', 'm', 'e'}).Sql(options)

	assert.NoError(t, err)

	expectedSql := `?`
	expectedArgs := []any{[]byte{'n', 'a', 'm', 'e'}}

	assert.Equal(t, expectedSql, sql)
	assert.Equal(t, expectedArgs, args)
}
