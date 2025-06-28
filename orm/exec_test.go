package orm

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestExec(t *testing.T) {
	expectedSql := `DELETE FROM "users" WHERE ("id" = ? AND "deleted_at" IS NULL)`
	expectedArgs := []any{1}

	executor := testutil.NewMockExecutor()
	executor.On(
		"Exec",
		mock.Anything,
		expectedSql,
		expectedArgs,
	).Return(testutil.NewMockExecResult(100, 200), nil)

	res, err := Exec(op.Delete("users").Where(op.And{
		op.Eq("id", 1),
		op.Eq("deleted_at", nil),
	})).Log(func(sql string, args []any, err error) {
		assert.NoError(t, err)
		assert.Equal(t, expectedArgs, args)
		assert.Equal(t, expectedSql, sql)
	}).With(context.Background(), executor)

	assert.NoError(t, err)

	rowsCount, err := res.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), rowsCount)

	lastId, err := res.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, int64(200), lastId)
}

func TestExecError(t *testing.T) {
	res, err := Exec(op.Delete("a+b")).With(context.Background(), testutil.NewMockExecutor())

	assert.Nil(t, res)
	assert.EqualError(t, err, `target "a+b" contains illegal character '+'`)
}
