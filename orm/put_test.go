package orm

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

type PutMockUser struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

type PutMockTagsDetails struct {
	ID     int    `op:"id,primary"`
	Name   string `op:"name"`
	Count  string `op:"count,aggregated"`
	Nested string `op:"nested,nested"`
}

func TestPut(t *testing.T) {
	t.Parallel()
	expectedSql := `INSERT INTO "users" ("name") VALUES (?) ON CONFLICT ("id") DO UPDATE SET "name"=EXCLUDED."name" RETURNING "users"."id","users"."name"`
	expectedArgs := []any{"Alex"}

	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	for i := 0; i < 2; i++ {
		user := &PutMockUser{
			Name: "Alex",
		}

		require.Equal(t, 0, user.ID)
		err := Put("users", user).Log(func(sql string, args []any, err error) {
			require.NoError(t, err)
			require.Equal(t, expectedSql, sql)
			require.Equal(t, expectedArgs, args)
		}).With(context.Background(), query)

		require.NoError(t, err)
		require.Equal(t, 100, user.ID)
		require.Equal(t, "Bob", user.Name)
	}
}

func TestPutTagsDetails(t *testing.T) {
	t.Parallel()
	expectedSql := `INSERT INTO "users" ("name") VALUES (?) ON CONFLICT ("id") DO UPDATE SET "name"=EXCLUDED."name" RETURNING "users"."id","users"."name"`
	expectedArgs := []any{"Alex"}

	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	user := &PutMockTagsDetails{
		Name: "Alex",
	}

	require.Equal(t, 0, user.ID)
	err := Put("users", user).Log(func(sql string, args []any, err error) {
		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
		require.Equal(t, expectedSql, sql)
	}).With(context.Background(), query)
	require.NoError(t, err)

	require.Equal(t, 100, user.ID)
	require.Equal(t, "Bob", user.Name)
}
