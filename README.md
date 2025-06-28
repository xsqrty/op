# Golang sql builder/ORM
[![Test Status](https://github.com/xsqrty/op/actions/workflows/test.yml/badge.svg)](https://github.com/xsqrty/op/actions/workflows/test.yml) ![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/xsqrty/op) [![Go Report Card](https://goreportcard.com/badge/github.com/xsqrty/op)](https://goreportcard.com/report/github.com/xsqrty/op) [![Go Reference](https://pkg.go.dev/badge/github.com/xsqrty/op.svg)](https://pkg.go.dev/github.com/xsqrty/op) [![Coverage Status](https://coveralls.io/repos/github/xsqrty/op/badge.svg?branch=main&v=1)](https://coveralls.io/github/xsqrty/op?branch=main)

```shell
go get github.com/xsqrty/op
```

## Dialect Support

| Dialect     | Sql builder | ORM |
|-------------|-------------|-----|
| Postgres    | ✅           | ✅   |
| SQLite      | ✅           | ✅   |
| MySQL       | -           | -   |
| SQL Server  | -           | -   |

## Basic usage
```go
import (
  "github.com/google/uuid"
  "github.com/xsqrty/op"
  "github.com/xsqrty/op/db"
  "github.com/xsqrty/op/orm"
)

type User struct {
  ID    uuid.UUID `op:"id,primary"`
  Name  string    `op:"name"`
  Roles []string  `op:"roles"`
  Age   int       `op:"age"`
}

func main() {
  ctx := context.Background()
	
  // Create postgres connection pool
  pool, err := db.OpenPostgres(ctx, "postgres://...")
  if err != nil {
    panic(err)
  }
	
  // Create new user or update existed (by ID)
  err = orm.Put[User]("users", &User{
    ID: uuid.Must(uuid.NewV7()),
    Name: "Alex",
    Roles: []string{"admin"},
    Age: 40,
  }).With(ctx, qe)
	
  if err != nil {
    panic(err)
  }
	
  // Get a list of users over 18 years old
  // `.Select()` will automatically determine the list of fields, but
  // you can manually specify `.Select("id", "name")`
  users, err := orm.Query[User](
    op.Select().From("users").Where(op.Gt("age", 18))
  ).With(ctx, pool)
}
```