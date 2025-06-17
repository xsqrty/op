package op

import (
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op/testutil"
	"testing"
)

func TestInsert(t *testing.T) {
	testutil.RunCases(t, options, []testutil.TestCase{
		{
			Name: "insert",
			Builder: Insert("users", Inserting{
				"age": 100,
			}),
			ExpectedSql:  `INSERT INTO "users" ("age") VALUES (?)`,
			ExpectedArgs: []any{100},
		},
		{
			Name:         "insert_many",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, "Alex").Values(20, "John"),
			ExpectedSql:  `INSERT INTO "users" ("age","name") VALUES (?,?),(?,?)`,
			ExpectedArgs: []any{10, "Alex", 20, "John"},
		},
		{
			Name:         "insert_returning",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, "Alex").Returning("id"),
			ExpectedSql:  `INSERT INTO "users" ("age","name") VALUES (?,?) RETURNING "id"`,
			ExpectedArgs: []any{10, "Alex"},
		},
		{
			Name:         "insert_returning_alias",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, "Alex").Returning(ColumnAlias("id")),
			ExpectedSql:  `INSERT INTO "users" ("age","name") VALUES (?,?) RETURNING "id"`,
			ExpectedArgs: []any{10, "Alex"},
		},
		{
			Name:         "insert_conflict_do_nothing",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, "Alex").Returning("id").OnConflict("id", DoNothing()),
			ExpectedSql:  `INSERT INTO "users" ("age","name") VALUES (?,?) ON CONFLICT ("id") DO NOTHING RETURNING "id"`,
			ExpectedArgs: []any{10, "Alex"},
		},
		{
			Name:         "insert_conflict_alias",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, "Alex").Returning("id").OnConflict(ColumnAlias("id"), DoNothing()),
			ExpectedSql:  `INSERT INTO "users" ("age","name") VALUES (?,?) ON CONFLICT ("id") DO NOTHING RETURNING "id"`,
			ExpectedArgs: []any{10, "Alex"},
		},
		{
			Name:         "insert_error_1",
			Builder:      InsertMany("users"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "insert: fields is empty",
		},
		{
			Name:         "insert_error_2",
			Builder:      InsertMany("users").Columns("age", "name"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "insert: no insert values",
		},
		{
			Name:         "insert_error_3",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, 20).OnConflict("age+age", DoNothing()),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "age+age" contains illegal character '+'`,
		},
		{
			Name:         "insert_error_4",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, 20).Returning("age+age"),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "age+age" contains illegal character '+'`,
		},
		{
			Name:         "insert_error_5",
			Builder:      Insert(ColumnAlias("users+users"), Inserting{"age": 10}),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "target \"users+users\" contains illegal character '+'",
		},
		{
			Name:         "insert_error_6",
			Builder:      InsertMany("users").Columns("age+age", "name").Values(10, 20),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "age+age" contains illegal character '+'`,
		},
		{
			Name:         "insert_error_7",
			Builder:      InsertMany("users").Columns("age", "name").Values(Column("age+age"), 20),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  `target "age+age" contains illegal character '+'`,
		},
		{
			Name:         "insert_error_8",
			Builder:      InsertMany(100),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "insert_error_9",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, "Alex").OnConflict("age", DoUpdate(Updates{})),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "update: fields is empty",
		},
		{
			Name:         "insert_error_10",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, 20).Returning(100),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
		{
			Name:         "insert_error_11",
			Builder:      InsertMany("users").Columns("age", "name").Values(10, 20).OnConflict(100, DoNothing()),
			ExpectedSql:  "",
			ExpectedArgs: []any(nil),
			ExpectedErr:  "unknown type: int must be a string or Alias",
		},
	})
}

func TestInsertUsingTables(t *testing.T) {
	t.Parallel()
	item := Insert("users", Inserting{})
	tables := item.UsingTables()

	assert.Equal(t, []string{"users"}, tables)
	assert.Equal(t, "users", item.With())
}

func TestInsertReturning(t *testing.T) {
	t.Parallel()
	item := Insert("users", Inserting{}).Returning("id")
	item.LimitReturningOne()

	assert.Equal(t, []Alias{ColumnAlias("id")}, item.GetReturning())

	item.SetReturning([]any{"id", "age"})
	assert.Equal(t, []Alias{ColumnAlias("id"), ColumnAlias("age")}, item.GetReturning())

	item.SetReturningAliases([]Alias{ColumnAlias("col2")})
	assert.Equal(t, []Alias{ColumnAlias("col2")}, item.GetReturning())
}
