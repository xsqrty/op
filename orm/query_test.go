package orm

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/internal/testutil"
)

type QueryMockUser struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

func TestGetMany(t *testing.T) {
	t.Parallel()
	expectedSql := `SELECT "users"."id","users"."name" FROM "users" WHERE "users"."id" = ?`
	expectedArgs := []any{1}

	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRows(nil, []db.Scanner{
			testutil.NewMockRow(nil, []any{1, "Alex"}),
			testutil.NewMockRow(nil, []any{2, "John"}),
		}), nil)

	users, err := Query[QueryMockUser](
		op.Select().From("users").Where(op.Eq("users.id", 1)),
	).Log(func(sql string, args []any, err error) {
		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
		require.Equal(t, expectedSql, sql)
	}).
		GetMany(context.Background(), query)

	require.NoError(t, err)
	require.Len(t, users, 2)

	require.Equal(t, 1, users[0].ID)
	require.Equal(t, "Alex", users[0].Name)

	require.Equal(t, 2, users[1].ID)
	require.Equal(t, "John", users[1].Name)
}

func TestGetOne(t *testing.T) {
	t.Parallel()
	expectedSql := `SELECT "users"."id","users"."name" FROM "users" WHERE "users"."id" = ? LIMIT ?`
	expectedArgs := []any{100, uint64(1)}

	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	user, err := Query[QueryMockUser](
		op.Select().From("users").Where(op.Eq("users.id", 100)),
	).Log(func(sql string, args []any, err error) {
		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
		require.Equal(t, expectedSql, sql)
	}).
		GetOne(context.Background(), query)
	require.NoError(t, err)

	require.Equal(t, 100, user.ID)
	require.Equal(t, "Bob", user.Name)
}

func TestGetOneError(t *testing.T) {
	t.Parallel()
	query := testutil.NewMockQueryable()
	query.
		On(
			"QueryRow",
			mock.Anything,
			`SELECT "users"."id","users"."name" FROM "users" LIMIT ?`,
			[]any{uint64(1)},
		).
		Return(testutil.NewMockRow(errors.New("sql syntax error"), nil))

	user, err := Query[QueryMockUser](op.Select().From("users")).GetOne(context.Background(), query)
	require.Nil(t, user)
	require.EqualError(t, err, "sql syntax error")
}

func TestGetOneSqlError(t *testing.T) {
	t.Parallel()
	query := testutil.NewMockQueryable()
	user, err := Query[QueryMockUser](
		op.Select().From("users").Where(op.Eq("a+b", 1)),
	).GetOne(context.Background(), query)
	require.Nil(t, user)
	require.EqualError(t, err, `target "a+b" contains illegal character '+'`)
}

func TestGetOneModelError(t *testing.T) {
	t.Parallel()
	query := testutil.NewMockQueryable()
	user, err := Query[QueryMockUser](
		op.Select("undefined").From("users"),
	).GetOne(context.Background(), query)
	require.Nil(t, user)
	require.EqualError(
		t,
		err,
		`"undefined": target is not described in the struct *orm.QueryMockUser`,
	)
}
