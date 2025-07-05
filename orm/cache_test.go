package orm

import (
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/cache"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestReturnableCache(t *testing.T) {
	t.Parallel()

	q := op.Select("id", "age").From("users").Join("companies", op.Eq("users.company_id", op.Column("companies.id"))).Where(op.Eq("id", cache.Arg("id")))
	c := NewReturnableCache(q)

	for i := 0; i < 2; i++ {
		id := gofakeit.UUID()
		q := c.Use(cache.Args{
			"id": id,
		})

		require.Equal(t, "users", q.With())
		require.Equal(t, op.CounterQuery, q.CounterType())
		require.Equal(t, []string{"users", "companies"}, q.UsingTables())

		q.LimitReturningOne()
		q.SetReturning([]any{"id"})

		q.SetReturningAliases([]op.Alias{
			op.ColumnAlias("users.id"),
			op.ColumnAlias("users.age"),
			op.ColumnAlias("users.name"),
			op.ColumnAlias("companies.name"),
		})

		require.Len(t, q.GetReturning(), 4)

		sql, args, err := q.Sql(testutil.NewDefaultOptions())
		require.NoError(t, err)
		require.Equal(t, `SELECT "users"."id","users"."age","users"."name","companies"."name" FROM "users" JOIN "companies" ON "users"."company_id" = "companies"."id" WHERE "id" = ? LIMIT ?`, sql)
		require.Len(t, args, 2)
		require.IsType(t, &cache.ArgData{}, args[0])
		require.Equal(t, uint64(1), args[1])

		sql, args, err = q.PreparedSql(testutil.NewDefaultOptions())
		require.Equal(t, `SELECT "users"."id","users"."age","users"."name","companies"."name" FROM "users" JOIN "companies" ON "users"."company_id" = "companies"."id" WHERE "id" = ? LIMIT ?`, sql)
		require.Equal(t, []any{id, uint64(1)}, args)
	}
}

func TestReturnableCacheErrors(t *testing.T) {
	t.Parallel()

	c1 := NewReturnableCache(op.Select("id", "age+age").Where(op.Eq("id", cache.Arg("id"))))
	c2 := NewReturnableCache(op.Select("id", "age").Where(op.Eq("id", cache.Arg("id"))))

	for i := 0; i < 2; i++ {
		id := gofakeit.UUID()
		q := c1.Use(cache.Args{
			"id": id,
		})

		sql, args, err := q.PreparedSql(testutil.NewDefaultOptions())
		require.Empty(t, sql)
		require.Empty(t, args)
		require.EqualError(t, err, `target "age+age" contains illegal character '+'`)

		q = c2.Use(cache.Args{
			"id":  id,
			"len": 10,
		})

		sql, args, err = q.PreparedSql(testutil.NewDefaultOptions())
		require.Empty(t, sql)
		require.Empty(t, args)
		require.EqualError(t, err, "incorrect argument count")

		q = c2.Use(cache.Args{
			"uid": id,
		})

		sql, args, err = q.PreparedSql(testutil.NewDefaultOptions())
		require.Empty(t, sql)
		require.Empty(t, args)
		require.EqualError(t, err, `no such arg "uid" inside container`)
	}
}
