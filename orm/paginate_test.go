package orm

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

type PaginateMockUser struct {
	Name        string `op:"user_name"`
	ID          int    `op:"user_id"`
	Age         int    `op:"user_age"`
	CompanyName string `op:"company_name"`
}

func TestPaginateApi(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		expectedSql       string
		expectedSqlCount  string
		expectedArgs      []any
		expectedArgsCount []any
		builder           Paginator[PaginateMockUser]
	}{
		{
			name:              "join",
			expectedSql:       `SELECT * FROM (SELECT ("users"."name") AS "user_name" FROM "users" JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result" LIMIT ?`,
			expectedSqlCount:  `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."name") AS "user_name" FROM "users" JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result"`,
			expectedArgs:      []any{uint64(1)},
			expectedArgsCount: []any(nil),
			builder:           Paginate[PaginateMockUser]("users", &PaginateRequest{}).Fields(op.As("user_name", op.Column("users.name"))).WhiteList("id", "age", "name").Join("companies", op.Eq("users.company_id", op.Column("companies.id"))),
		},
		{
			name:              "inner_join",
			expectedSql:       `SELECT * FROM (SELECT ("users"."name") AS "user_name" FROM "users" INNER JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result" LIMIT ?`,
			expectedSqlCount:  `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."name") AS "user_name" FROM "users" INNER JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result"`,
			expectedArgs:      []any{uint64(1)},
			expectedArgsCount: []any(nil),
			builder:           Paginate[PaginateMockUser]("users", &PaginateRequest{}).Fields(op.As("user_name", op.Column("users.name"))).WhiteList("id", "age", "name").InnerJoin("companies", op.Eq("users.company_id", op.Column("companies.id"))),
		},
		{
			name:              "left_join",
			expectedSql:       `SELECT * FROM (SELECT ("users"."name") AS "user_name" FROM "users" LEFT JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result" LIMIT ?`,
			expectedSqlCount:  `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."name") AS "user_name" FROM "users" LEFT JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result"`,
			expectedArgs:      []any{uint64(1)},
			expectedArgsCount: []any(nil),
			builder:           Paginate[PaginateMockUser]("users", &PaginateRequest{}).Fields(op.As("user_name", op.Column("users.name"))).WhiteList("id", "age", "name").LeftJoin("companies", op.Eq("users.company_id", op.Column("companies.id"))),
		},
		{
			name:              "right_join",
			expectedSql:       `SELECT * FROM (SELECT ("users"."name") AS "user_name" FROM "users" RIGHT JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result" LIMIT ?`,
			expectedSqlCount:  `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."name") AS "user_name" FROM "users" RIGHT JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result"`,
			expectedArgs:      []any{uint64(1)},
			expectedArgsCount: []any(nil),
			builder:           Paginate[PaginateMockUser]("users", &PaginateRequest{}).Fields(op.As("user_name", op.Column("users.name"))).WhiteList("id", "age", "name").RightJoin("companies", op.Eq("users.company_id", op.Column("companies.id"))),
		},
		{
			name:              "cross_join",
			expectedSql:       `SELECT * FROM (SELECT ("users"."name") AS "user_name" FROM "users" CROSS JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result" LIMIT ?`,
			expectedSqlCount:  `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."name") AS "user_name" FROM "users" CROSS JOIN "companies" ON "users"."company_id" = "companies"."id") AS "result"`,
			expectedArgs:      []any{uint64(1)},
			expectedArgsCount: []any(nil),
			builder:           Paginate[PaginateMockUser]("users", &PaginateRequest{}).Fields(op.As("user_name", op.Column("users.name"))).WhiteList("id", "age", "name").CrossJoin("companies", op.Eq("users.company_id", op.Column("companies.id"))),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query := testutil.NewMockQueryable()
			query.
				On("Query", mock.Anything, tc.expectedSql, tc.expectedArgs).
				Return(testutil.NewMockRows(nil, []db.Scanner{
					testutil.NewMockRow(nil, []any{"Alex"}),
					testutil.NewMockRow(nil, []any{"John"}),
				}), nil)

			query.On("QueryRow", mock.Anything, tc.expectedSqlCount, tc.expectedArgsCount).Return(testutil.NewMockRow(nil, []any{uint64(2)}))

			res, err := tc.builder.With(context.Background(), query)

			require.NoError(t, err)
			require.Equal(t, uint64(2), res.TotalRows)
			require.Equal(t, "Alex", res.Rows[0].Name)
			require.Equal(t, "John", res.Rows[1].Name)
		})
	}
}

func TestPaginate(t *testing.T) {
	t.Parallel()
	expectedSql := `SELECT * FROM (SELECT ("users"."id") AS "user_id",("users"."age") AS "user_age",("users"."name") AS "user_name",("companies"."name") AS "company_name" FROM "users" LEFT JOIN "companies" ON "users"."company_id" = "companies"."id" WHERE "companies"."id" = ? GROUP BY "users"."id","companies"."name") AS "result" WHERE ("age" = ? OR "age" = ? OR "id" IN (?,?,?)) ORDER BY "id" DESC LIMIT ?`
	expectedArgs := []any{111, float64(25), float64(26), float64(1), float64(2), float64(3), uint64(5)}

	expectedCounterSql := `SELECT (COUNT(*)) AS "total_count" FROM (SELECT ("users"."id") AS "user_id",("users"."age") AS "user_age",("users"."name") AS "user_name",("companies"."name") AS "company_name" FROM "users" LEFT JOIN "companies" ON "users"."company_id" = "companies"."id" WHERE "companies"."id" = ? GROUP BY "users"."id","companies"."name") AS "result" WHERE ("age" = ? OR "age" = ? OR "id" IN (?,?,?))`
	expectedCounterArgs := []any{111, float64(25), float64(26), float64(1), float64(2), float64(3)}

	query := testutil.NewMockQueryable()
	query.
		On("Query", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRows(nil, []db.Scanner{
			testutil.NewMockRow(nil, []any{1, 25, "Alex", "Company 1"}),
			testutil.NewMockRow(nil, []any{2, 26, "John", "Company 2"}),
		}), nil)

	query.On("QueryRow", mock.Anything, expectedCounterSql, expectedCounterArgs).Return(testutil.NewMockRow(nil, []any{uint64(2)}))

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
	require.NoError(t, err)

	res, err := Paginate[PaginateMockUser]("users", &req).
		WhiteList("id", "age", "name").
		LeftJoin("companies", op.Eq("users.company_id", op.Column("companies.id"))).
		MinLimit(1).
		MaxLimit(5).
		MaxSliceLen(10).
		MaxFilterDepth(1).
		LogQuery(func(sql string, args []any, err error) {
			require.NoError(t, err)
			require.Equal(t, expectedArgs, args)
			require.Equal(t, expectedSql, sql)
		}).
		LogCounter(func(sql string, args []any, err error) {
			require.NoError(t, err)
			require.Equal(t, expectedCounterArgs, args)
			require.Equal(t, expectedCounterSql, sql)
		}).
		GroupBy("users.id", "companies.name").
		Where(op.Eq("companies.id", 111)).
		Fields(
			op.As("user_id", op.Column("users.id")),
			op.As("user_age", op.Column("users.age")),
			op.As("user_name", op.Column("users.name")),
			op.As("company_name", op.Column("companies.name")),
		).
		With(context.Background(), query)

	require.NoError(t, err)
	require.Equal(t, uint64(2), res.TotalRows)

	require.Equal(t, 1, res.Rows[0].ID)
	require.Equal(t, 25, res.Rows[0].Age)
	require.Equal(t, "Alex", res.Rows[0].Name)

	require.Equal(t, 2, res.Rows[1].ID)
	require.Equal(t, 26, res.Rows[1].Age)
	require.Equal(t, "John", res.Rows[1].Name)
}

func TestPaginateError(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err)
	res, err := Paginate[PaginateMockUser]("users", &req).
		WhiteList("id", "age", "name").
		With(context.Background(), query)

	require.Nil(t, res)
	require.EqualError(t, err, "fields is empty. Please specify returning by .Fields()")
}

func TestPaginateFilterError(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, err)
	res, err := Paginate[PaginateMockUser]("users", &req).
		Fields(
			op.As("user_id", op.Column("users.id")),
		).
		With(context.Background(), query)

	require.Nil(t, res)
	require.EqualError(t, err, `paginate: target "id" is not allowed`)
}
