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
	t.Parallel()
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
			builder:      Count(op.Select().From("users").Join("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex"))).By("id"),
		},
		{
			name:         "left_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" LEFT JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count(op.Select().From("users").LeftJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex"))).By("id"),
		},
		{
			name:         "right_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" RIGHT JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count(op.Select().From("users").RightJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex"))).By("id"),
		},
		{
			name:         "inner_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" INNER JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count(op.Select().From("users").InnerJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex"))).By("id"),
		},
		{
			name:         "cross_join",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" CROSS JOIN "companies" ON "companies"."id" = "users"."company_id" WHERE "Name" = ? LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count(op.Select().From("users").CrossJoin("companies", op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("Name", "Alex"))).By("id"),
		},
		{
			name:         "group_by",
			expectedSql:  `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? GROUP BY "email" LIMIT ?`,
			expectedArgs: []any{"Alex", uint64(1)},
			builder:      Count(op.Select().From("users").GroupBy("email").Where(op.Eq("Name", "Alex"))).By("id"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			totalCount := gofakeit.Uint64()
			query := testutil.NewMockQueryExec()
			query.Q.
				On("QueryRow", mock.Anything, tc.expectedSql, tc.expectedArgs).
				Return(testutil.NewMockRow(nil, []any{totalCount}))

			count, err := tc.builder.With(context.Background(), query)
			require.NoError(t, err)
			require.Equal(t, totalCount, count)
		})
	}
}

func TestCount(t *testing.T) {
	t.Parallel()
	expectedSql := `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`
	expectedArgs := []any{"Alex", uint64(1)}

	query := testutil.NewMockQueryExec()
	query.Q.
		On("QueryRow", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err := Count(op.Select().From("users").Where(op.Eq("Name", "Alex"))).By("id").Log(func(sql string, args []any, err error) {
		require.NoError(t, err)
		require.Equal(t, expectedArgs, args)
		require.Equal(t, expectedSql, sql)
	}).With(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, uint64(55), count)

	query = testutil.NewMockQueryExec()
	query.Q.
		On("QueryRow", mock.Anything, `SELECT (COUNT(DISTINCT "id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err = Count(op.Select().From("users").Where(op.Eq("Name", "Alex"))).ByDistinct("id").With(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, uint64(55), count)

	query = testutil.NewMockQueryExec()
	query.Q.
		On("QueryRow", mock.Anything, `SELECT (COUNT(*)) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err = Count(op.Select().From("users").Where(op.Eq("Name", "Alex"))).With(context.Background(), query)
	require.NoError(t, err)
	require.Equal(t, uint64(55), count)
}

func TestCountError(t *testing.T) {
	t.Parallel()
	query := testutil.NewMockQueryExec()
	query.Q.
		On("QueryRow", mock.Anything, `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(errors.New("syntax error"), nil))

	count, err := Count(op.Select().From("users").Where(op.Eq("Name", "Alex"))).By("id").With(context.Background(), query)
	require.Equal(t, uint64(0), count)
	require.EqualError(t, err, "syntax error")
}

func TestCountExec(t *testing.T) {
	t.Parallel()
	expectedSql := `DELETE FROM "users" WHERE "id" = ?`
	expectedArgs := []any{10}

	exec := testutil.NewMockQueryExec()
	exec.E.
		On("Exec", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockExecResult(1, 0), nil)

	count, err := Count(op.Delete("users").Where(op.Eq("id", 10))).With(context.Background(), exec)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	exec = testutil.NewMockQueryExec()
	exec.E.
		On("Exec", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockExecResult(0, 0), errors.New("syntax error"))

	count, err = Count(op.Delete("users").Where(op.Eq("id", 10))).With(context.Background(), exec)
	require.Equal(t, uint64(0), count)
	require.EqualError(t, err, "syntax error")

	exec = testutil.NewMockQueryExec()
	exec.E.
		On("Exec", mock.Anything, expectedSql, expectedArgs).
		Return(testutil.NewMockExecResultAffectedError(errors.New("rows affected error")), nil)

	count, err = Count(op.Delete("users").Where(op.Eq("id", 10))).With(context.Background(), exec)
	require.Equal(t, uint64(0), count)
	require.EqualError(t, err, "rows affected error")
}
