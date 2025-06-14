package op

import (
	"context"
	"github.com/xsqrty/op/driver"
)

type ExecBuilder interface {
	With(ctx context.Context, db Executable) (driver.ExecResult, error)
}

type Executable interface {
	Exec(ctx context.Context, sql string, args ...any) (driver.ExecResult, error)
	Sql(sqler driver.Sqler) (string, []any, error)
}

type execBuilder struct {
	driver.Sqler
}

func Exec(sqler driver.Sqler) ExecBuilder {
	return &execBuilder{sqler}
}

func (e *execBuilder) With(ctx context.Context, db Executable) (driver.ExecResult, error) {
	sql, args, err := db.Sql(e.Sqler)
	if err != nil {
		return nil, err
	}

	return db.Exec(ctx, sql, args...)
}
