package orm

import (
	"context"
	"errors"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestCountApi(t *testing.T) {
	cases := []struct {
		name         string
		expectedSql  string
		expectedArgs []any
		builder      CountBuilder
	}{
		{
			name:         "join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count("users").By("id").Join("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex")),
		},
		{
			name:         "left_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" LEFT JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count("users").By("id").LeftJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex")),
		},
		{
			name:         "right_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" RIGHT JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count("users").By("id").RightJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex")),
		},
		{
			name:         "inner_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" INNER JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count("users").By("id").InnerJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex")),
		},
		{
			name:         "cross_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" CROSS JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count("users").By("id").CrossJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex")),
		},
		{
			name:         "group_by",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? GROUP BY "email" LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count("users").By("id").GroupBy("email").Where(op.Eq("Name", "Alex")),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			totalCount := gofakeit.Uint64()
			query := testutil.NewMockQueryable()
			query.
				On("QueryRow", mock.Anything, tc.expectedSql, tc.expectedArgs).
				Return(testutil.NewMockRow(nil, []any{totalCount}))

			count, err := tc.builder.With(context.Background(), query)
			require.NoError(t, err)
			require.Equal(t, totalCount, count)
		})
	}
}

func TestCount(t *testing.T) {
	expectedSql := `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`
	expectedArgs := []any{"Alex", uint64(1)}

	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err := Count("users").By("id").Where(op.Eq("Name", "Alex")).Log(func(sql string, args []any, err error) {
		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
		require.Equal(t, expectedSql, sql)
	}).With(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, uint64(55), count)

	query = testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT(DISTINCT "id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err = Count("users").ByDistinct("id").Where(op.Eq("Name", "Alex")).With(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, uint64(55), count)

	query = testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT(*)) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err = Count("users").Where(op.Eq("Name", "Alex")).With(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, uint64(55), count)
}

func TestCountError(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(errors.New("syntax error"), nil))

	count, err := Count("users").By("id").Where(op.Eq("Name", "Alex")).With(context.Background(), query)
	require.Equal(t, uint64(0), count)
	require.EqualError(t, err, "syntax error")
}
