package op

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/testutil"
	"testing"
)

type PutMockUser struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

func TestPut(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `INSERT INTO "users" ("name") VALUES (?) ON CONFLICT ("id") DO UPDATE SET "name"=EXCLUDED."name" RETURNING "users"."id","users"."name"`, []any{"Alex"}).
		Return(testutil.NewMockRow(nil, []any{100, "Bob"}))

	user := &PutMockUser{
		Name: "Alex",
	}

	assert.Equal(t, 0, user.ID)
	err := Put[PutMockUser]("users", user).With(context.Background(), query)
	assert.NoError(t, err)

	assert.Equal(t, 100, user.ID)
	assert.Equal(t, "Bob", user.Name)
}
