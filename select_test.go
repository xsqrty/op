package op

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op/internal/testutil"
)

type orderCase struct {
	builder        Order
	target         any
	orderType      orderType
	nullsOrderType nullsOrderType
	expectedSql    string
}

func TestSelect(t *testing.T) {
	t.Parallel()
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name:         "select",
			Builder:      Select("id", "name").From("users"),
			ExpectedSql:  `SELECT "id","name" FROM "users"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "select_all",
			Builder:      Select().From("users"),
			ExpectedSql:  `SELECT * FROM "users"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "select_alias",
			Builder:      Select().From(ColumnAlias("users")),
			ExpectedSql:  `SELECT * FROM "users"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name: "join",
			Builder: Select(
				"id",
				"name",
			).From("users").
				Join("roles", Eq("user_id", Column("users.id"))),
			ExpectedSql:  `SELECT "id","name" FROM "users" JOIN "roles" ON "user_id" = "users"."id"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name: "inner_join",
			Builder: Select(
				"id",
				"name",
			).From("users").
				InnerJoin("roles", Eq("user_id", Column("users.id"))),
			ExpectedSql:  `SELECT "id","name" FROM "users" INNER JOIN "roles" ON "user_id" = "users"."id"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name: "left_join",
			Builder: Select(
				"id",
				"name",
			).From("users").
				LeftJoin(ColumnAlias("roles"), Eq("user_id", Column("users.id"))),
			ExpectedSql:  `SELECT "id","name" FROM "users" LEFT JOIN "roles" ON "user_id" = "users"."id"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name: "right_join",
			Builder: Select(
				"id",
				"name",
			).From("users").
				RightJoin("roles", Eq("user_id", Column("users.id"))),
			ExpectedSql:  `SELECT "id","name" FROM "users" RIGHT JOIN "roles" ON "user_id" = "users"."id"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name: "cross_join",
			Builder: Select(
				"id",
				"name",
			).From("users").
				CrossJoin("roles", Eq("user_id", Column("users.id"))),
			ExpectedSql:  `SELECT "id","name" FROM "users" CROSS JOIN "roles" ON "user_id" = "users"."id"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "where",
			Builder:      Select("id", "name").From("users").Where(Or{Eq("id", 1), Eq("id", 2)}),
			ExpectedSql:  `SELECT "id","name" FROM "users" WHERE ("id" = ? OR "id" = ?)`,
			ExpectedArgs: []any{1, 2},
		},
		{
			Name: "order",
			Builder: Select("id", "name").
				From("users").
				OrderBy(
					Asc("name"),
					Desc("name"),
					DescNullsLast("age"),
					DescNullsFirst("age"),
					AscNullsLast("age"),
					AscNullsFirst("age"),
				),
			ExpectedSql:  `SELECT "id","name" FROM "users" ORDER BY "name" ASC,"name" DESC,"age" DESC NULLS LAST,"age" DESC NULLS FIRST,"age" ASC NULLS LAST,"age" ASC NULLS FIRST`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "order_custom",
			Builder:      Select("id", "name").From("users").OrderBy(Asc(Div(Column("age"), 2))),
			ExpectedSql:  `SELECT "id","name" FROM "users" ORDER BY ("age"/?) ASC`,
			ExpectedArgs: []any{2},
		},
		{
			Name: "group_by",
			Builder: Select(
				"id",
				ColumnAlias("name"),
			).From("users").
				GroupBy("category", Column("id")),
			ExpectedSql:  `SELECT "id","name" FROM "users" GROUP BY "category","id"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "limit",
			Builder:      Select("id", "name").From("users").Limit(10),
			ExpectedSql:  `SELECT "id","name" FROM "users" LIMIT ?`,
			ExpectedArgs: []any{uint64(10)},
		},
		{
			Name:         "offset",
			Builder:      Select("id", "name").From("users").Offset(10),
			ExpectedSql:  `SELECT "id","name" FROM "users" OFFSET ?`,
			ExpectedArgs: []any{uint64(10)},
		},
		{
			Name:         "distinct",
			Builder:      Select("id", "name").From("users").Distinct(),
			ExpectedSql:  `SELECT DISTINCT "id","name" FROM "users"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "distinct_on",
			Builder:      Select("id", "name").From("users").Distinct("col1", "col2"),
			ExpectedSql:  `SELECT DISTINCT ON ("col1","col2") "id","name" FROM "users"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "all",
			Builder:      Select("id", "name").From("users").All(),
			ExpectedSql:  `SELECT ALL "id","name" FROM "users"`,
			ExpectedArgs: []any(nil),
		},
		{
			Name:         "having",
			Builder:      Select("id", "name").From("users").Having(Gte(Sum("count"), 100)),
			ExpectedSql:  `SELECT "id","name" FROM "users" HAVING SUM("count") >= ?`,
			ExpectedArgs: []any{100},
		},
		{
			Name:         "error_join_1",
			Builder:      Select("id", "name").From("users").Join("a+b", nil),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_join_2",
			Builder:      Select("id", "name").From("users").Join("a", nil),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "JOIN operation requires an ON clause to specify join condition",
		},
		{
			Name:         "error_join_3",
			Builder:      Select("id", "name").From("users").Join(10, nil),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "error_join_4",
			Builder:      Select("id", "name").From("users").Join("a", Eq("a+b", 10)),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_order",
			Builder:      Select("id", "name").From("users").OrderBy(Asc("a+b")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_having",
			Builder:      Select("id", "name").From("users").Having(Count("a+b")),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_group_by_1",
			Builder:      Select("id", "name").From("users").GroupBy("a+b"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_group_by_2",
			Builder:      Select("id", "name").From("users").GroupBy(100),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or driver.Sqler",
		},
		{
			Name:         "error_where",
			Builder:      Select("id", "name").From("users").Where(Eq("a+b", 10)),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_from_1",
			Builder:      Select("id", "name").From("a+b"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
		{
			Name:         "error_from_2",
			Builder:      Select("id", "name").From(10),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "error_fields",
			Builder:      Select("a+b").From("a"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "a+b" contains illegal character '+'`,
		},
	})
}

func TestSelectUsingTables(t *testing.T) {
	t.Parallel()
	item := Select().From("users").Join("roles", Eq("user_id", "users.id"))
	tables := item.UsingTables()

	require.Equal(t, []string{"users", "roles"}, tables)
	require.Equal(t, "users", item.With())
}

func TestSelectReturning(t *testing.T) {
	t.Parallel()
	item := Select("id").From("users")
	item.LimitReturningOne()

	require.Equal(t, []Alias{ColumnAlias("id")}, item.GetReturning())

	item.SetReturning([]Alias{ColumnAlias("col2")})
	require.Equal(t, []Alias{ColumnAlias("col2")}, item.GetReturning())
	require.Equal(t, CounterQuery, item.CounterType())

	sql, args, err := item.PreparedSql(testutil.NewDefaultOptions())
	require.NoError(t, err)
	require.Equal(t, `SELECT "col2" FROM "users" LIMIT ?`, sql)
	require.Equal(t, []any{uint64(1)}, args)
}

func TestOrder(t *testing.T) {
	t.Parallel()
	orderCases := []orderCase{
		{
			builder:        Asc("age"),
			target:         "age",
			nullsOrderType: nullsNone,
			orderType:      orderAsc,
			expectedSql:    `"age" ASC`,
		},
		{
			builder:        Desc("age"),
			target:         "age",
			nullsOrderType: nullsNone,
			orderType:      orderDesc,
			expectedSql:    `"age" DESC`,
		},
		{
			builder:        AscNullsFirst("age"),
			target:         "age",
			nullsOrderType: nullsFirst,
			orderType:      orderAsc,
			expectedSql:    `"age" ASC NULLS FIRST`,
		},
		{
			builder:        DescNullsFirst("age"),
			target:         "age",
			nullsOrderType: nullsFirst,
			orderType:      orderDesc,
			expectedSql:    `"age" DESC NULLS FIRST`,
		},
		{
			builder:        AscNullsLast("age"),
			target:         "age",
			nullsOrderType: nullsLast,
			orderType:      orderAsc,
			expectedSql:    `"age" ASC NULLS LAST`,
		},
		{
			builder:        DescNullsLast("age"),
			target:         "age",
			nullsOrderType: nullsLast,
			orderType:      orderDesc,
			expectedSql:    `"age" DESC NULLS LAST`,
		},
		{
			builder: &order{
				target:    "age",
				orderType: orderNone,
				nullsType: nullsNone,
			},
			target:         "age",
			nullsOrderType: nullsNone,
			orderType:      orderNone,
			expectedSql:    `"age"`,
		},
	}

	for _, orderCase := range orderCases {
		sql, _, err := orderCase.builder.Sql(options)

		require.NoError(t, err)
		require.Equal(t, orderCase.expectedSql, sql)

		require.Equal(t, orderCase.orderType, orderCase.builder.OrderType())
		require.Equal(t, orderCase.nullsOrderType, orderCase.builder.NullsType())
		require.Equal(t, orderCase.target, orderCase.builder.Target())
	}

	require.Equal(t, "", nullsNone.String())
	require.Equal(t, "", orderNone.String())
}
