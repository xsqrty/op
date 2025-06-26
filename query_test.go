package op

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/testutil"
	"testing"
)

type QueryMockUser struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

func TestGetMany(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, `SELECT "users"."id","users"."name" FROM "users" WHERE "users"."id" = ?`, []any{1}).
		Return(testutil.NewMockRows(nil, []driver.Scanner{
			testutil.NewMockRow(nil, []any{1, "Alex"}),
			testutil.NewMockRow(nil, []any{2, "John"}),
		}), nil)

	users, err := Query[QueryMockUser](Select().From("users").Where(Eq("users.id", 1))).GetMany(context.Background(), query)
	assert.NoError(t, err)
	assert.Len(t, users, 2)

	assert.Equal(t, 1, users[0].ID)
	assert.Equal(t, "Alex", users[0].Name)

	assert.Equal(t, 2, users[1].ID)
	assert.Equal(t, "John", users[1].Name)
}

func TestGetOne(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT "users"."id","users"."name" FROM "users" WHERE "users"."id" = ? LIMIT ?`, []any{100, uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	user, err := Query[QueryMockUser](Select().From("users").Where(Eq("users.id", 100))).GetOne(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, 100, user.ID)
	assert.Equal(t, "Bob", user.Name)
}

func TestGetOneError(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT "users"."id","users"."name" FROM "users" LIMIT ?`, []any{uint64(1)}).
		Return(testutil.NewMockRow(errors.New("sql syntax error"), nil))

	user, err := Query[QueryMockUser](Select().From("users")).GetOne(context.Background(), query)
	assert.Nil(t, user)
	assert.EqualError(t, err, "sql syntax error")
}

func TestGetOneSqlError(t *testing.T) {
	query := testutil.NewMockQueryable()
	user, err := Query[QueryMockUser](Select().From("users").Where(Eq("a+b", 1))).GetOne(context.Background(), query)
	assert.Nil(t, user)
	assert.EqualError(t, err, `target "a+b" contains illegal character '+'`)
}

func TestGetOneModelError(t *testing.T) {
	query := testutil.NewMockQueryable()
	user, err := Query[QueryMockUser](Select("undefined").From("users")).GetOne(context.Background(), query)
	assert.Nil(t, user)
	assert.EqualError(t, err, `"undefined": target is not described in the struct *op.QueryMockUser`)
}
