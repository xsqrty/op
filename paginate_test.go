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

func TestPaginate(t *testing.T) {
	t.Parallel()
	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, `SELECT ("users"."id") AS "id",("users"."name") AS "name" FROM "users" WHERE ("id" = ? OR "id" = ?) ORDER BY "id" DESC LIMIT ?`, []any{float64(25), float64(26), int64(10)}).
		Return(testutil.NewMockRows(nil, []driver.Scanner{
			testutil.NewMockRow(nil, []any{1, "Alex"}),
			testutil.NewMockRow(nil, []any{2, "John"}),
		}), nil)

	query.On("QueryRow", mock.Anything, `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."id") AS "id",("users"."name") AS "name" FROM "users" WHERE ("id" = ? OR "id" = ?)) AS "result"`, []any{float64(25), float64(26)}).Return(testutil.NewMockRow(nil, []any{int64(2)}))

	reqString := `{
			"limit": 10,
			"filters": {
					"group": "or",
					"filters": [
							{
									"op": "eq",
									"key": "id",
									"value": 25
							},
							{
									"op": "eq",
									"key": "id",
									"value": 26
							}
					]
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

	res, err := Paginate[User]("users", &req, []string{"id"}).
		Fields(
			As("id", Column("users.id")),
			As("name", Column("users.name")),
		).
		With(context.Background(), query)

	assert.NoError(t, err)
	assert.Equal(t, int64(2), res.TotalRows)

	assert.Equal(t, 1, res.Rows[0].ID)
	assert.Equal(t, "Alex", res.Rows[0].Name)

	assert.Equal(t, 2, res.Rows[1].ID)
	assert.Equal(t, "John", res.Rows[1].Name)
}
