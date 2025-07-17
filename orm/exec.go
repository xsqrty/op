package orm

import (
	"context"

	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

type ExecBuilder interface {
	Log(handler LoggerHandler) ExecBuilder
	With(ctx context.Context, db Executable) (db.ExecResult, error)
}

type Executable interface {
	Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error)
	SqlOptions() *driver.SqlOptions
}

type execBuilder struct {
	exp    driver.PreparedSqler
	logger LoggerHandler
}

func Exec(sqler driver.PreparedSqler) ExecBuilder {
	return &execBuilder{exp: sqler}
}

func (eb *execBuilder) With(ctx context.Context, db Executable) (db.ExecResult, error) {
	sql, args, err := eb.exp.PreparedSql(db.SqlOptions())
	if eb.logger != nil {
		eb.logger(sql, args, err)
	}
	if err != nil {
		return nil, err
	}

	return db.Exec(ctx, sql, args...)
}

func (eb *execBuilder) Log(lh LoggerHandler) ExecBuilder {
	eb.logger = lh
	return eb
}
