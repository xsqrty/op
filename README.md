# Golang SQL builder/ORM

[![Test Status](https://github.com/xsqrty/op/actions/workflows/test.yml/badge.svg)](https://github.com/xsqrty/op/actions/workflows/test.yml) ![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/xsqrty/op) [![Go Report Card](https://goreportcard.com/badge/github.com/xsqrty/op)](https://goreportcard.com/report/github.com/xsqrty/op) [![Go Reference](https://pkg.go.dev/badge/github.com/xsqrty/op.svg)](https://pkg.go.dev/github.com/xsqrty/op) [![Coverage Status](https://coveralls.io/repos/github/xsqrty/op/badge.svg?branch=main&v=1)](https://coveralls.io/github/xsqrty/op?branch=main)

```shell
go get github.com/xsqrty/op
```

# Contents

- [Quick start](#quick-start)
- [Dialect support](#dialect-support)
- [ORM](#orm)
  - [Create connection](#create-connection)
    - [Postgres](#postgres)
    - [Sqlite](#sqlite)
  - [Query/Exec](#query-exec)
    - [Select rows](#select-rows)
      - [Nested query example](#nested-query-example)
      - [Aggregated query example](#aggregated-query-example)
    - [Insert rows](#insert-rows)
      - [Insert many rows](#insert-many-rows)
    - [Update rows](#update-rows)
    - [Delete rows](#delete-rows)
  - [Put row](#put-row)
  - [Count rows](#count-rows)
  - [Pagination](#pagination)Count rows
- [Build SQL](#build-sql)
  - [Select builder](#select-builder)
  - [Insert builder](#insert-builder)
  - [Update builder](#update-builder)
  - [Delete builder](#delete-builder)
  - [Comparison operators](#comparison-operators)
  - [Functions](#functions)
  - [Math operations](#math-operations)
- [Native API](#native-api)
  - [Query](#query)
  - [Exec](#exec)
  - [Transactions](#transactions)
- [Caching and optimization](#caching-and-optimization)

# Quick start

```go
type User struct {
  ID    int64    `op:"id,primary"`
  Name  string   `op:"name"`
  Roles []string `op:"roles"`
  Age   int      `op:"age"`
}

ctx := context.Background()

// Create postgres connection pool
pool, err := db.OpenPostgres(ctx, "postgres://...")
if err != nil {
  panic(err)
}

// There is no migration tool. You can use any migration tool of your choice.
pool.Exec(ctx, `create table if not exists users (
  id    serial primary key ,
  name  text,
  roles text[],
  age   integer
)`)

// Create new user or update existed (by ID)
user := &User{
  Name:  "Alex",
  Roles: []string{"admin"},
  Age:   40,
}

err := orm.Put("users", user).With(ctx, pool)
if err != nil {
  panic(err)
}

// User ID
fmt.Printf("User ID: %d", user.ID)

// Get a list of users over 18 years old
// `.Select()` will automatically determine the list of fields, but
// you can describe manually `.Select("id", "name")`
users, err := orm.Query[User](
  op.Select().From("users").Where(op.Gt("age", 18)),
).GetMany(ctx, pool)

if err != nil {
  panic(err)
}

for _, user := range users {
  fmt.Println(user)
}
```

# Dialect Support

| Dialect    | Sql builder | ORM |
|------------|-------------|-----|
| Postgres   | ✅           | ✅   |
| SQLite     | ✅           | ✅   |
| MySQL      | -           | -   |
| SQL Server | -           | -   |

# ORM

Object relation mapping API

- `orm.Query[Model]` QueryBuilder - execute sql and return rows
- `orm.Exec` ExecBuilder - execute sql and return `db.ExecResult`
- `orm.Put[Model]` Insert/Replace implementation
- `orm.Count` CountBuilder - count the number of rows
- `orm.Paginate[Model]` PaginateBuilder - paginator implementation

## Create connection

When an open connection, a `.Ping()` is automatically performed to ensure a successful connection.

### Postgres

Using [pgx](https://github.com/jackc/pgx/tree/master) driver

```go
pool, err := db.OpenPostgres(
  ctx,
  dsn,
  db.WithPgxHealthCheckPeriod(time.Minute),
  db.WithPgxMinIdleConns(0),
  // ...other options db.WithPgx...
)

if err != nil {
  panic(err)
}
```

### Sqlite

Using [sqlite3](https://github.com/mattn/go-sqlite3) driver

```go
pool, err := db.OpenSqlite(
  ctx,
  dsn,
  db.WithConnMaxIdleTime(5*time.Minute),
  db.WithConnMaxLifetime(5*time.Minute),
  db.WithMaxIdleConns(2),
  db.WithMaxOpenConns(10),
)

if err != nil {
  panic(err)
}
```

## Query exec

`orm.Query[Model](...)` accepts the `orm.Returnable` interface `op.Select()` `op.Insert()` `op.Update` `op.Delete()`

- GetMany(context.Context, db.ConnPool) ([]*Model, error) - get many rows (empty slice if no rows)
- GetOne(context.Context, db.ConnPool) (*Model, error) - get one row (error if there are no rows)
- Log(handler LoggerHandler) ExecBuilder - register SQL logger

`orm.Exec(...)` accepts the `driver.Sqler` interface, including `op.Select()` `op.Insert()` `op.Update` `op.Delete()`...

- With(context.Context, db.ConnPool) (db.ExecResult, error) - execute query and get result
- Log(handler LoggerHandler) ExecBuilder - register SQL logger

### Select rows

```go
users, err := orm.Query[User](
  op.Select().From("users").Limit(10),
).GetMany(ctx, pool)
```

Get a list of users

`.Select()` will automatically determine the list of fields, but
you can describe manually `.Select("id", "name")`

### Nested query example

In this example, the fields from the `companies` table will be placed in a nested `User.Company` structure.

To do this, you need to add a tag `op:"companies,nested"`. Name of the field must match the name of the table.

Inserting data via `orm.Put()` will not work for nested structure (you must put it manually)

```go
type User struct {
  ID        int64    `op:"id,primary"`
  Name      string   `op:"name"`
  CompanyId int64    `op:"company_id"`
  Company   Company  `op:"companies,nested"`
}

type Company struct {
  ID   int64  `op:"id,primary"`
  Name string `op:"name"`
}

users, err := orm.Query[User](
  op.Select().
  From("users").
  Join("companies", op.Eq("company_id", op.Column("companies.id"))),
).GetMany(ctx, pool)
```

### Aggregated query example

```go
type User struct {
  ID          int64    `op:"id,primary"`
  Name        string   `op:"name"`
  CompanyName string   `op:"CompanyName,aggregated"`
}

users, err := orm.Query[User](
  op.Select(
    "id",
    "name",
    op.As("CompanyName",
      op.Select("name").From("companies").Where(op.Eq("id", op.Column("users.company_id"))),
    ),
  ).From("users"),
).GetMany(ctx, pool)
```

### Insert rows

```go
event, err := orm.Exec(op.Insert("users", op.Inserting{
  "id":    uuid.Must(uuid.NewV7()),
  "name":  "Jack",
  "roles": []string{"manager"},
  "age":   20,
})).With(ctx, pool)

insertedCount, err := event.RowsAffected()
lastId, err := event.LastInsertedId()
```

Insert one row and return db.ExecResult interface

`pgx` does not support the last_insert_id interface, it will return `db.ErrPgxUnsupported`,
you can use `orm.Query` + `.Returning` (see below)

```go
user, err := orm.Query[User](
  op.Insert("users", op.Inserting{
    "id":    uuid.Must(uuid.NewV7()),
    "name":  "Jack",
    "roles": []string{"manager"},
    "age": 20,
  }).
  Returning("id"),
).GetOne(ctx, pool)
```

Insert one row and return it (only id, other fields will be ignored)

You can manually describe the return list,
then only the specified fields will be extracted, if you omit .Returning, then all fields will be extracted
automatically.

### Insert many rows

```go
event, err := orm.Exec(
  op.InsertMany("users").
    Columns("id", "name", "age").
    Values(uuid.Must(uuid.NewV7()), "Bill", 10).
    Values(uuid.Must(uuid.NewV7()), "John", 20),
).With(ctx, pool)

insertedCount, err := event.RowsAffected()
```

Insert and get affected (inserted) count

```go
users, err := orm.Query[User](
  op.InsertMany("users").
    Columns("id", "name", "age").
    Values(uuid.Must(uuid.NewV7()), "Bill", 10).
    Values(uuid.Must(uuid.NewV7()), "John", 20).
		Returning("id", "name", "age"),
).GetMany(ctx, pool)
```

Returns a list of inserted rows

### Update rows

```go
event, err := orm.Exec(
  op.Update("users", op.Updates{
    "name": "rename",
  }).Where(op.Eq("name", "Alex")),
).With(ctx, pool)

updatedCount, err := event.RowsAffected()
```

Update and get affected (updated) count

```go
users, err := orm.Query[User](
  op.Update("users", op.Updates{
    "roles": []string{"all"},
    "age":   op.Add("age", driver.Value(1)),
  }).Where(op.Eq("name", "Alex")).
  Returning("id", "roles", "age"),
).GetMany(ctx, pool)
```

Update roles, and increment age (+1) for selected users.
Returns a list of updated rows

### Delete rows

```go
event, err := orm.Exec(
  op.Delete("users").Where(op.Eq("name", "Alex")),
)

deletedCount, err := event.RowsAffected()
```

Delete and get affected (deleted) count

```go
users, err := orm.Query[User](
  op.Delete("users").Where(op.Eq("name", "Alex")).
  Returning("id"),
).GetMany(ctx, pool)
```

Returns a list of deleted rows

## Put row

Insert or replace implementation.

`PutBuilder` based on `on conflict` sql clause. It tries to insert a row, if there is a conflict on the primary key,
then this row will be updated.

After insert or update, data will be pulled from the database (fields that are empty in the structure, but which
are filled in the database by default: ID, ...)

Model must contain a primary tag option `op: "ID,primary"`

```go
user := &User{
  ID: 1,
  Name:  "Alex",
  Roles: []string{"admin"},
  Age:   40,
}

err := orm.Put("users", user).With(ctx, pool)
```

## Count rows

`orm.Count()` count the number of rows

* By(key string) CountBuilder - explicitly specify the key for `COUNT("key")`, by default `COUNT(*)`
* ByDistinct(key string) CountBuilder - explicitly specify the key (with distinct) for `COUNT(DISTINCT "key")`
* Log(handler LoggerHandler) CountBuilder - register SQL logger
* With(ctx context.Context, db Queryable) (uint64, error) - exec query and get result

```go
count, err := orm.Count(
  op.Select().From("roles_users").
    Join("roles", op.Eq("role_id", op.Column("roles.id"))).
    Where(op.And{
      op.Eq("user_id", u.ID),
      op.Lc("permissions", []string{"read", "write"}),
    }),
  ).With(ctx, pool)
```

## Pagination

`orm.Paginate[Model]()` simple paginator implementation

You must describe the fields `.WhiteList()` that will be available for filtering/ordering. Otherwise, no fields will be
available.

You must describe returning fields `.Fields()` using Aliases `op.As("name", op.Column("column"))`. This is necessary
because paginator wrap the query, and the fields described in the inner query are no longer available

* WhiteList(whitelist ...string) Paginator[Model] - restrict access to the fields from request
* Fields(fields ...op.Alias) Paginator[Model] - describes a list of exported fields
* MaxFilterDepth(depth uint64) Paginator[Model] - limits the max nesting for the filter
* MaxSliceLen(maxLen uint64) Paginator[Model] - limits the max length of slices (`"$in": [...]`, `"$or": [...]`)
* MaxLimit(limit uint64) Paginator[Model] - set the max request limit
* MinLimit(limit uint64) Paginator[Model] - set the min request limit
* Where(exp driver.Sqler) Paginator[Model] - `WHERE` clause
* Join(table any, on driver.Sqler) Paginator[Model] - join
* LeftJoin(table any, on driver.Sqler) Paginator[Model] - left join
* RightJoin(table any, on driver.Sqler) Paginator[Model] - right join
* InnerJoin(table any, on driver.Sqler) Paginator[Model] - inner join
* CrossJoin(table any, on driver.Sqler) Paginator[Model] - cross join
* GroupBy(groups ...any) Paginator[Model] - `GROUP BY` clause
* LogQuery(handler LoggerHandler) Paginator[Model] - register SQL logger (for rows query)
* LogCounter(handler LoggerHandler) Paginator[Model] - register SQL logger (for count query)
* With(ctx context.Context, db Queryable) (*PaginateResult[Model], error) - exec paginator queries and return
  `orm.PaginateResult`

Available filter operators

* `$or` - example `"$or": [{"age": 25}, {"age": {"$eq": 26}}]`
* `$and` - example `"$and": [{"age": 25}, {"age": {"$eq": 26}}]`
* `$in` - in operator, example `"id": {"$in": [1,2,3]}`
* `$nin` - not in operator, example  `"id": {"$nin": [1,2,3]}`
* `$eq` - equal, examples `"age": {"$eq": 10}` or `"age": {"$eq": null}` sql: IS NULL
* `$ne` - not equal, examples `"age": {"$ne": 10}` or `"age": {"$ne": null}` sql: IS NOT NULL
* `$lt` - less than
* `$gt` - greater than
* `$lte` - less or equal
* `$gte` - greater or equal
* `$like` - string contains a substring (case-insensitive)
* `$llike` - string contains a substring on the left (case-insensitive)
* `$rlike` - string contains a substring on the right (case-insensitive)

```go
req := `{
  "limit": 10,
  "offset": 10,
  "filters": {
    "name": "Alex",
    "company": {"$eq": "CompanyName"},
    "age": {"$gte": 18, "$lte": 50},
    "$or": [{"age": 25}, {"age": {"$eq": 26}}]
  },
  "orders": [
    {
      "key": "id",
      "desc": true
    }
  ]
}`

var paginateRequest PaginateRequest
err := json.Unmarshal([]byte(req), &paginateRequest)
if err != nil {
  panic(err)
}

res, err := Paginate[PaginateMockUser]("users", &paginateRequest).
  WhiteList("id", "age", "name").
  With(context.Background(), query)
```

# Build SQL

```go
options := driver.NewPostgresSqlOptions() // driver.NewSqliteSqlOptions() for sqlite syntax
sql, args, err := driver.Sql(op.Select().From("Users"), options)
```

Generated SQL

```sql
SELECT *
FROM "Users"
```

You can also use a connection pool for a build with specified options.

```go
pool, _ := db.OpenPostgres(ctx, "postgres://...")
sql, args, err := pool.Sql(op.Select().From("Users"))
```

## Select builder

`op.Select(...)` create select builder

* Distinct(...string) SelectBuilder - `SELECT DISTINCT` eliminates duplicate rows from the result. If you specify arguments it will be interpreted as `DISTINCT ON(col, ...)`
* All() SelectBuilder - `SELECT ALL` specifies the opposite: all rows are kept; that is the default.
* From(from any) SelectBuilder - table (string) or op.Alias, for example `op.As("subquery", Select().From(...))`
* Where(exp driver.Sqler) SelectBuilder - `WHERE` clause
* Having(exp driver.Sqler) SelectBuilder - `HAVING` clause
* Join(table any, on driver.Sqler) SelectBuilder - join, table (string) or op.Alias
* LeftJoin(table any, on driver.Sqler) SelectBuilder - left join, table (string) or op.Alias
* RightJoin(table any, on driver.Sqler) SelectBuilder - right join, table (string) or op.Alias
* InnerJoin(table any, on driver.Sqler) SelectBuilder - inner join, table (string) or op.Alias
* CrossJoin(table any, on driver.Sqler) SelectBuilder - cross join, table (string) or op.Alias
* Limit(limit uint64) SelectBuilder - `LIMIT` clause
* Offset(offset uint64) SelectBuilder - `OFFSET` clause
* GroupBy(groups ...any) SelectBuilder - `GROUB BY` clause
* OrderBy(orders ...Order) SelectBuilder - `ORDER BY` clause
* Sql(options *driver.SqlOptions) (sql string, args []any, err error) - builder

```go
options := driver.NewPostgresSqlOptions()
multiple := op.Mul("count", driver.Value(100))
sql, args, err := driver.Sql(
  op.Select(
    "id", 
    op.As("sub", op.Select().From("Companies")),
  ).
    From("Users").
    Limit(1).
    Offset(10).
    Join("otherTable", op.Eq("Users.col", op.Column("otherTable.col"))).
    LeftJoin(op.As("subJoin", op.Select().From("third")), op.Ne("third.key", 100)).
    OrderBy(
      op.AscNullsFirst("name"),
      op.Desc(multiple),
    ).
    GroupBy(
      "name",
      multiple,
    ).
    Where(op.Or{op.Eq("key", "value1"), op.Eq("key", "value2")}),
  options,
)
```

Generated SQL

You can read more
about [comparison operators](#comparison-operators), [math operations](#math-operations), [functions](#functions) below

```sql
SELECT "id", (SELECT * FROM "Companies") AS "sub"
FROM "Users"
         JOIN "otherTable" ON "Users"."col" = "otherTable"."col"
         LEFT JOIN (SELECT * FROM "third") AS "subJoin" ON "third"."key" != $1
WHERE ("key" = $2 OR "key" = $3)
GROUP BY "name", ("count" * $4)
ORDER BY "name" ASC NULLS FIRST, ("count" * $5) DESC
LIMIT $6 OFFSET $7
```

## Insert builder

`op.Insert(...)` or `op.InsertMany(...)` create insert builder

* Columns(columns ...string) InsertBuilder - define columns (only for `op.InsertMany`)
* Values(values ...any) InsertBuilder - add values list (only for `op.InsertMany`)
* OnConflict(target any, do driver.Sqler) InsertBuilder - `ON CONFLICT` clause
* Returning(keys ...any) InsertBuilder - set returning fields
* Sql(options *driver.SqlOptions) (string, []any, error) - builder

```go
op.Insert("users", op.Inserting{
  "key": "value",
  "age": 20,
}).Returning("id").OnConflict("id", op.DoUpdate(op.Updates{
  "key": op.Excluded("key"),
  "age": 20,
}))
```

Generated SQL

```sql
INSERT INTO "users" ("age", "key")
VALUES ($1, $2)
ON CONFLICT ("id") DO UPDATE SET "key"=EXCLUDED."key",
                                 "age"=$3
RETURNING "id"
```

You can insert multiple rows at once

```go
op.InsertMany("users").
  Columns("key", "age").
  Values("key", 20).
  Values("key2", 30).
  Returning("id").
  OnConflict("id", op.DoUpdate(op.Updates{
    "key": op.Excluded("key"),
    "age": 20,
  }))
```

Generated SQL

```sql
INSERT INTO "users" ("key", "age")
VALUES ($1, $2),
       ($3, $4)
ON CONFLICT ("id") DO UPDATE SET "key"=EXCLUDED."key",
                                 "age"=$5
RETURNING "id"
```

## Update builder
`op.Update(...)` create update builder

* Where(exp driver.Sqler) UpdateBuilder - `WHERE` clause
* Returning(keys ...any) UpdateBuilder - set returning fields
* Sql(options *driver.SqlOptions) (string, []any, error) - builder

```go
op.Update("users", op.Updates{
  "name": "Updated",
  "age":  op.Add(op.Column("age"), 1),
}).Where(op.Gte("age", 18)).Returning("id", "age")
```
Generated SQL

```sql
UPDATE "users"
SET "age"=("age" + $1),
    "name"=$2
WHERE "age" >= $3
RETURNING "id","age"
```

## Delete builder
`op.Delete(...)` create delete builder

* Where(exp driver.Sqler) DeleteBuilder - `WHERE` clause
* Returning(keys ...any) DeleteBuilder - set returning fields
* Sql(options *driver.SqlOptions) (string, []any, error) - builder

```go
op.Delete("users").Where(op.Eq("id", 1)).Returning("id", "name")
```

Generated SQL
```sql
DELETE
FROM "users"
WHERE "id" = $1
RETURNING "id","name"
```

## Comparison operators

* op.Like(key any, val any) - `LIKE`
* op.NotLike(key any, val any)  - `NOT LIKE`
* op.ILike(key any, val any) - `ILIKE`
* op.NotILike(key any, val any) - `NOT ILIKE`
* op.Lt(key any, val any) - `<`
* op.Gt(key any, val any) - `>`
* op.Gte(key any, val any) - `>=`
* op.Lte(key any, val any) - `<=`
* op.Eq(key any, val any) - `=` if the value is `nil` then the operator will be `IS NULL`
* op.Ne(key any, val any) - `!=` if the value is `nil` then the operator will be `IS NOT NULL`
* op.Lc(key any, val any) - `@>`
* op.Rc(key any, val any) - `<@`
* op.ExtractText(arg any, path ...any) - `#>>`
* op.ExtractObject(arg any, path ...any) - `#>`
* op.HasProp(key any, args ...any) - `?|`
* op.HasProps(key any, args ...any) - `?&`
* op.In(key any, values ...any) - `IN`
* op.Nin(key any, values ...any) - `NOT IN`

```go
op.In("id", 1, 2, 3) // "id" IN ($1,$2,$3)
op.In("id", op.Select("id").From("users")) // "id" IN (SELECT "id" FROM "users")

op.Eq("name", "alex") // "name" = $1
op.Eq("name", nil) // "name" IS NULL
op.Eq("name", op.Column("companies.name")) // "name" = "companies"."name"
op.Eq(driver.Value("alex"), op.Column("companies.name")) // $1 = "companies"."name"

// SQL: "description" LIKE $1
// Arguments: ["%text%"]
op.Like("description", "%text%")
```

For combined expressions, use `op.And{}` / `op.Or{}` operators
```go
op.Or{
  op.And{
    op.Eq("id", 10),
    op.Eq("name", "alex"),
  },
  op.Eq("group", "admin"),
}
```

Generated SQL

```sql
(("id" = $1 AND "name" = $2) OR "group" = $3)
```

## Functions

All func arguments interpret string values as columns, if you want to use a string as argument, use `driver.Value(any)`

* op.Cast(arg any, typ string)
* op.Any(arg driver.Sqler) - `ANY` operator compares a value with a set of values returned by a subquery
* op.All(arg driver.Sqler) - `ALL` operator allows you to compare a value with all values in a set returned by a
  subquery
* op.Concat(args ...any)
* op.Coalesce(args ...any)
* op.Lower(arg any)
* op.Upper(arg any)
* op.Max(arg any)
* op.Min(arg any)
* op.Sum(arg any)
* op.Avg(arg any)
* op.Abs(arg any)
* op.Count(arg any)
* op.CountDistinct(arg any)
* op.Array(args ...any)
* op.ArrayLength(arg any)
* op.ArrayConcat(arg1, arg2 any)
* op.ArrayUnnest(arg any)

```go
op.Cast("data", "jsonb") // out: "data"::jsonb
op.Cast([]int{1}, "jsonb") // $1::jsonb 
op.Cast(driver.Value("text"), "text") // $1::text, sqlite: CAST($1 AS text)

op.Count(driver.Pure("*")) // COUNT(*)

// Array example (for postgres)
op.Array(1, 2, 3, op.Column("age")) // ARRAY[$1,$2,$3,"age"]
```

### Custom functions

if you need to use a function that is not in the list, you can use `op.Func` or `op.FuncPrefix`. In this case, all func
params interpret as sql args

```go
op.Func("COUNT", "id") // COUNT($1) - fail
op.Func("COUNT", op.Column("id")) // COUNT("id")
op.FuncPrefix("COUNT", "DISTINCT", op.Column("id")) // COUNT(DISTINCT "id")
```

## Math operations

All math operations interpret string values as columns, if you want to use a string as argument, use `driver.Value(any)`

Operations `op.Add` `op.Sub` `op.Div` `op.Mul`

```go
options := driver.NewPostgresSqlOptions()
sql, args, err := driver.Sql(op.Sub(1, op.Mul("age", 2)), options)
```

Generated SQL

```sql
($1-("age"*$2))
```

# Native API

* db.Exec(ctx context.Context, sql string, args ...any) (ExecResult, error)
* db.Query(ctx context.Context, sql string, args ...any) (Rows, error)
* db.QueryRow(ctx context.Context, sql string, args ...any) Row
* Transact(ctx context.Context, handler func(ctx context.Context) error) error

## Query
Query one row
```go
err := conn.QueryRow(ctx, sql, args...).Scan(&id, &name)
```

Query many rows
```go
rows, err := conn.Query(ctx, sql, args...)
if err != nil {
  return nil, err
}

// Need to close
defer rows.Close()

for _, row := range rows.Rows() {
  fmt.Printf("%v\n", row)
}

// Need to check for an error
if err := rows.Err(); err != nil {
  return nil, err
}
```

## Exec
```go
event, err := conn.Exec(ctx, sql, args...)
count, err := event.RowsAffected()
lastId, err := event.LastInsertId()
```

## Transactions

`.Transact()` creates a new context with a transaction object (everything executed with this context falls into the transaction). If the function returns an error, then `Rollback` will be called, otherwise `Commit`.
```go
err := conn.Transact(ctx, func (ctx context.Context) error {
  err := orm.Put("users", &User{
    ID:    id,
    Roles: roles,
    Data:  data,
  }).With(ctx, conn)
  
  if err != nil {
  return err
  }
  
  rows, err := orm.Query[User](
    op.Select().From("users")
  ).GetMany(ctx, conn)
  
  return err
})
```

# Caching and optimization

If you need to perform a heavy query, then building SQL may take some time.

You can use a query caching tool `cache.New()` for native usage or `orm.NewReturnableCache` for use with orm

```go
options := driver.NewPostgresSqlOptions()
getById := cache.New(
  op.Select().
    From("users").
    Where(op.Eq("id", cache.Arg("id")))
)

for id := 1; id <= 10; id++ {
  getById.Use(Args{"id": id}).Sql(options)
}
```

First you need to create a cache container `cache.New()` in which it is necessary to designate arguments `cache.Args()`

Then you can use the container `container.Use()` by specifying the argument values `cache.Args{}`

```go
getById := orm.NewReturnableCache(
  op.Select().From("users").Where(op.Eq("id", cache.Arg("id")))
)

for id := 1; id <= 10; id++ {
  user, err := orm.Query[User](getById.Use(cache.Args{
    "id": id,
  })).GetOne(ctx, conn)
}
```

For ORM you need to use a special cache container `orm.NewReturnableCache()`.

This container supports `orm.Count` `orm.Exec` `orm.Query`

`orm.Put` already implements caching