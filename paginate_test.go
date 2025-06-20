package op

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/testutil"
	"testing"
)

type PaginateMockUser struct {
	Name string `op:"user_name"`
	ID   int    `op:"user_id"`
	Age  int    `op:"user_age"`
}

func TestPaginate(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, `SELECT * FROM (SELECT ("users"."id") AS "user_id",("users"."age") AS "user_age",("users"."name") AS "user_name" FROM "users") AS "result" WHERE ("age" = ? OR "age" = ?) ORDER BY "id" DESC LIMIT ?`, []any{float64(25), float64(26), int64(10)}).
		Return(testutil.NewMockRows(nil, []driver.Scanner{
			testutil.NewMockRow(nil, []any{1, 25, "Alex"}),
			testutil.NewMockRow(nil, []any{2, 26, "John"}),
		}), nil)

	query.On("QueryRow", mock.Anything, `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."id") AS "user_id",("users"."age") AS "user_age",("users"."name") AS "user_name" FROM "users") AS "result" WHERE ("age" = ? OR "age" = ?)`, []any{float64(25), float64(26)}).Return(testutil.NewMockRow(nil, []any{int64(2)}))

	reqString := `{
			"limit": 10,
			"filters": {
					"$or": [{"age": 25}, {"age": {"$eq": 26}}]
			},
			"orders": [
					{
							"key": "id",
							"desc": true
					}
			]
	}`

	var req PaginateRequest
	err := json.Unmarshal([]byte(reqString), &req)
	assert.NoError(t, err)

	res, err := Paginate[PaginateMockUser]("users", &req).
		WhiteList("id", "age", "name").
		Fields(
			As("user_id", Column("users.id")),
			As("user_age", Column("users.age")),
			As("user_name", Column("users.name")),
		).
		With(context.Background(), query)

	assert.NoError(t, err)
	assert.Equal(t, int64(2), res.TotalRows)

	assert.Equal(t, 1, res.Rows[0].ID)
	assert.Equal(t, 25, res.Rows[0].Age)
	assert.Equal(t, "Alex", res.Rows[0].Name)

	assert.Equal(t, 2, res.Rows[1].ID)
	assert.Equal(t, 26, res.Rows[1].Age)
	assert.Equal(t, "John", res.Rows[1].Name)
}
