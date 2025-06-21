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
	Name        string `op:"user_name"`
	ID          int    `op:"user_id"`
	Age         int    `op:"user_age"`
	CompanyName string `op:"company_name"`
}

func TestPaginate(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, `SELECT * FROM (SELECT ("users"."id") AS "user_id",("users"."age") AS "user_age",("users"."name") AS "user_name",("companies"."name") AS "company_name" FROM "users" LEFT JOIN "companies" ON "users"."company_id" = "companies"."id" WHERE "companies"."id" = ? GROUP BY "users"."id","companies"."name") AS "result" WHERE ("age" = ? OR "age" = ? OR "id" IN (?,?,?)) ORDER BY "id" DESC LIMIT ?`, []any{111, float64(25), float64(26), float64(1), float64(2), float64(3), int64(5)}).
		Return(testutil.NewMockRows(nil, []driver.Scanner{
			testutil.NewMockRow(nil, []any{1, 25, "Alex", "Company 1"}),
			testutil.NewMockRow(nil, []any{2, 26, "John", "Company 2"}),
		}), nil)

	query.On("QueryRow", mock.Anything, `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."id") AS "user_id",("users"."age") AS "user_age",("users"."name") AS "user_name",("companies"."name") AS "company_name" FROM "users" LEFT JOIN "companies" ON "users"."company_id" = "companies"."id" WHERE "companies"."id" = ? GROUP BY "users"."id","companies"."name") AS "result" WHERE ("age" = ? OR "age" = ? OR "id" IN (?,?,?))`, []any{111, float64(25), float64(26), float64(1), float64(2), float64(3)}).Return(testutil.NewMockRow(nil, []any{int64(2)}))

	reqString := `{
			"limit": 10,
			"filters": {
					"$or": [{"age": 25}, {"age": {"$eq": 26}}, {"id": {"$in": [1,2,3]}}]
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
		LeftJoin("companies", Eq("users.company_id", Column("companies.id"))).
		MinLimit(1).
		MaxLimit(5).
		MaxSliceLen(10).
		MaxFilterDepth(1).
		GroupBy("users.id", "companies.name").
		Where(Eq("companies.id", 111)).
		Fields(
			As("user_id", Column("users.id")),
			As("user_age", Column("users.age")),
			As("user_name", Column("users.name")),
			As("company_name", Column("companies.name")),
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

func TestPaginateError(t *testing.T) {
	query := testutil.NewMockQueryable()
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
		With(context.Background(), query)

	assert.Nil(t, res)
	assert.EqualError(t, err, "fields is empty. Please specify returning by .Fields()")
}

func TestPaginateFilterError(t *testing.T) {
	query := testutil.NewMockQueryable()
	reqString := `{
			"limit": 10,
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
		Fields(
			As("user_id", Column("users.id")),
		).
		With(context.Background(), query)

	assert.Nil(t, res)
	assert.EqualError(t, err, `paginate: target "id" is not allowed`)
}
