package cache

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/internal/testutil"
)

func TestCache(t *testing.T) {
	t.Parallel()
	c := New(op.Select().From("users").Where(op.Eq("id", Arg("id"))))

	for i := 0; i < 2; i++ {
		sql, args, err := c.Use(Args{"id": 100}).Sql(testutil.NewDefaultOptions())
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "users" WHERE "id" = ?`, sql)
		require.Equal(t, []any{100}, args)
	}
}

func TestCacheErrors(t *testing.T) {
	t.Parallel()

	c1 := New(op.Select("id", "age+age").Where(op.Eq("id", Arg("id"))))
	c2 := New(op.Select("id", "age").Where(op.Eq("id", Arg("id"))))

	for i := 0; i < 2; i++ {
		id := gofakeit.UUID()
		sql, args, err := c1.Use(Args{
			"id": id,
		}).Sql(testutil.NewDefaultOptions())

		require.Empty(t, sql)
		require.Empty(t, args)
		require.EqualError(t, err, `target "age+age" contains illegal character '+'`)

		sql, args, err = c2.Use(Args{
			"id":  id,
			"len": 10,
		}).Sql(testutil.NewDefaultOptions())
		require.Empty(t, sql)
		require.Empty(t, args)
		require.EqualError(t, err, "incorrect argument count")

		sql, args, err = c2.Use(Args{
			"uid": id,
		}).Sql(testutil.NewDefaultOptions())
		require.Empty(t, sql)
		require.Empty(t, args)
		require.EqualError(t, err, `no such arg "uid" inside container`)
	}
}

func BenchmarkSimpleCache(b *testing.B) {
	c := New(op.Select().From("users").Where(op.Eq("id", Arg("id"))))
	options := testutil.NewDefaultOptions()
	for i := 0; i < b.N; i++ {
		_, _, err := c.Use(Args{"id": 1}).Sql(options)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSimpleNoCache(b *testing.B) {
	options := testutil.NewDefaultOptions()
	for i := 0; i < b.N; i++ {
		_, _, err := op.Select().From("users").Where(op.Eq("id", 1)).Sql(options)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHeavyCache(b *testing.B) {
	c := New(
		op.Select().
			From("users").
			OrderBy(op.AscNullsFirst("created_at")).
			Join("companies", op.Eq("users.company_id", op.Column("companies.id"))).
			GroupBy("company_id").
			Where(op.Eq("id", Arg("id"))),
	)
	options := testutil.NewDefaultOptions()
	for i := 0; i < b.N; i++ {
		_, _, err := c.Use(Args{"id": 1}).Sql(options)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHeavyNoCache(b *testing.B) {
	options := testutil.NewDefaultOptions()
	for i := 0; i < b.N; i++ {
		_, _, err := op.Select().
			From("users").
			OrderBy(op.AscNullsFirst("created_at")).
			Join("companies", op.Eq("users.company_id", op.Column("companies.id"))).
			GroupBy("company_id").
			Where(op.Eq("id", 1)).
			Sql(options)
		if err != nil {
			b.Fatal(err)
		}
	}
}
