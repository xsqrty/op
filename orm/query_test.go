package orm

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

type QueryMockUser struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

func TestGetMany(t *testing.T) {
	expectedSql := `SELECT "users"."id","users"."name" FROM "users" WHERE "users"."id" = ?`
	expectedArgs := []any{1}
	
	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRows(nil, []db.Scanner{
			testutil.NewMockRow(nil, []any{1, "Alex"}),
			testutil.NewMockRow(nil, []any{2, "John"}),
		}), nil)

	users, err := Query[QueryMockUser](op.Select().From("users").Where(op.Eq("users.id", 1))).Log(func(sql string, args []any, err error) {
		assert.NoError(t, err)
		assert.Equal(t, expectedArgs, args)
		assert.Equal(t, expectedSql, sql)
	}).GetMany(context.Background(), query)
	assert.NoError(t, err)
	assert.Len(t, users, 2)

	assert.Equal(t, 1, users[0].ID)
	assert.Equal(t, "Alex", users[0].Name)

	assert.Equal(t, 2, users[1].ID)
	assert.Equal(t, "John", users[1].Name)
}

func TestGetOne(t *testing.T) {
	expectedSql := `SELECT "users"."id","users"."name" FROM "users" WHERE "users"."id" = ? LIMIT ?`
	expectedArgs := []any{100, uint64(1)}

	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	user, err := Query[QueryMockUser](op.Select().From("users").Where(op.Eq("users.id", 100))).Log(func(sql string, args []any, err error) {
		assert.NoError(t, err)
		assert.Equal(t, expectedArgs, args)
		assert.Equal(t, expectedSql, sql)
	}).GetOne(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, 100, user.ID)
	assert.Equal(t, "Bob", user.Name)
}

func TestGetOneError(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT "users"."id","users"."name" FROM "users" LIMIT ?`, []any{uint64(1)}).
		Return(testutil.NewMockRow(errors.New("sql syntax error"), nil))

	user, err := Query[QueryMockUser](op.Select().From("users")).GetOne(context.Background(), query)
	assert.Nil(t, user)
	assert.EqualError(t, err, "sql syntax error")
}

func TestGetOneSqlError(t *testing.T) {
	query := testutil.NewMockQueryable()
	user, err := Query[QueryMockUser](op.Select().From("users").Where(op.Eq("a+b", 1))).GetOne(context.Background(), query)
	assert.Nil(t, user)
	assert.EqualError(t, err, `target "a+b" contains illegal character '+'`)
}

func TestGetOneModelError(t *testing.T) {
	query := testutil.NewMockQueryable()
	user, err := Query[QueryMockUser](op.Select("undefined").From("users")).GetOne(context.Background(), query)
	assert.Nil(t, user)
	assert.EqualError(t, err, `"undefined": target is not described in the struct *orm.QueryMockUser`)
}
