package op

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestExec(t *testing.T) {
	executor := testutil.NewMockExecutor()
	executor.On(
		"Exec",
		mock.Anything,
		`DELETE FROM "users" WHERE ("id" = ? AND "deleted_at" IS NULL)`,
		[]any{1},
	).Return(testutil.NewMockExecResult(100), nil)

	res, err := Exec(Delete("users").Where(And{
		Eq("id", 1),
		Eq("deleted_at", nil),
	})).With(context.Background(), executor)

	assert.NoError(t, err)
	assert.Equal(t, uint64(100), res.RowsAffected())
}

func TestExecError(t *testing.T) {
	res, err := Exec(Delete("a+b")).With(context.Background(), testutil.NewMockExecutor())

	assert.Nil(t, res)
	assert.EqualError(t, err, `target "a+b" contains illegal character '+'`)
}
