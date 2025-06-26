package driver

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

type errorSqler struct {
}

func (e errorSqler) Sql(_ *SqlOptions) (string, []any, error) {
	return "", nil, errors.New("error")
}

func TestSql(t *testing.T) {
	options := NewPostgresSqlOptions()
	sql, args, err := Sql(Pure("?? ?,?,?", 1, "2", 3.01), options)
	assert.NoError(t, err)
	assert.Equal(t, "?? $1,$2,$3", sql)
	assert.Equal(t, []any{1, "2", 3.01}, args)

	sql, args, err = Sql(errorSqler{}, options)
	assert.EqualError(t, err, "error")
	assert.Equal(t, "", sql)
	assert.Equal(t, []any(nil), args)

	sql, args, err = Sql(Pure("?,?,?", 1, "2", 3.01), &SqlOptions{})
	assert.NoError(t, err)
	assert.Equal(t, "?,?,?", sql)
	assert.Equal(t, []any{1, "2", 3.01}, args)
}

func TestSqlWithPlaceholders(t *testing.T) {
	sql, args, err := Sql(Pure("?? ?,?,?", 1, "2", 3.01), NewSqlOptions(WithPlaceholderFormat(func(i int) string {
		return fmt.Sprintf("@%d", i)
	})))
	assert.NoError(t, err)
	assert.Equal(t, "?? @1,@2,@3", sql)
	assert.Equal(t, []any{1, "2", 3.01}, args)
}
