package orm

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

type PutMockUser struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

func TestPut(t *testing.T) {
	expectedSql := `INSERT INTO "users" ("name") VALUES (?) ON CONFLICT ("id") DO UPDATE SET "name"=EXCLUDED."name" RETURNING "users"."id","users"."name"`
	expectedArgs := []any{"Alex"}

	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	user := &PutMockUser{
		Name: "Alex",
	}

	assert.Equal(t, 0, user.ID)
	err := Put[PutMockUser]("users", user).Log(func(sql string, args []any, err error) {
		assert.NoError(t, err)
		assert.Equal(t, expectedArgs, args)
		assert.Equal(t, expectedSql, sql)
	}).With(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, 100, user.ID)
	assert.Equal(t, "Bob", user.Name)
}
