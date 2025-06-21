# SQL builder for GO
[![Test Status](https://github.com/xsqrty/op/actions/workflows/test.yml/badge.svg)](https://github.com/xsqrty/op/actions/workflows/test.yml) ![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/xsqrty/op) [![Go Report Card](https://goreportcard.com/badge/github.com/xsqrty/op)](https://goreportcard.com/report/github.com/xsqrty/op) [![Go Reference](https://pkg.go.dev/badge/github.com/xsqrty/op.svg)](https://pkg.go.dev/github.com/xsqrty/op) [![Coverage Status](https://coveralls.io/repos/github/xsqrty/op/badge.svg?branch=main&v=1)](https://coveralls.io/github/xsqrty/op?branch=main)

```go
import "github.com/xsqrty/op"
```

## Dialect Support

| Dialect     | Select | Insert | Update | Delete |
|-------------|--------|--------|--------|--------|
| Postgres    | ✅      | ✅      | ✅      | ✅      |
| MySQL       | -      | -      | -      | -      |
| SQLite      | -      | -      | -      | -      |
| SQL Server  | -      | -      | -      | -      |