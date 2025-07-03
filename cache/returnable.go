package cache

import (
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
	"sync"
)

type Container interface {
	Use() orm.Returnable
}

type result struct {
	sql  string
	args []any
	err  error
}

type retContainer struct {
	res    result
	ret    orm.Returnable
	retErr error

	resOnce sync.Once
	limOnce sync.Once
	salOnce sync.Once
	sreOnce sync.Once
}

type retCache struct {
	container *retContainer
}

func NewReturnable(input orm.Returnable) Container {
	return &retContainer{
		ret: input,
	}
}

func (rc *retContainer) Use() orm.Returnable {
	return &retCache{
		container: rc,
	}
}

func (rc *retCache) UsingTables() []string {
	return rc.container.ret.UsingTables()
}

func (rc *retCache) With() string {
	return rc.container.ret.With()
}

func (rc *retCache) GetReturning() []op.Alias {
	return rc.container.ret.GetReturning()
}

func (rc *retCache) SetReturning(fields []any) error {
	rc.container.sreOnce.Do(func() {
		rc.container.retErr = rc.container.ret.SetReturning(fields)
	})

	return rc.container.retErr
}

func (rc *retCache) SetReturningAliases(aliases []op.Alias) {
	rc.container.salOnce.Do(func() {
		rc.container.ret.SetReturningAliases(aliases)
	})
}

func (rc *retCache) LimitReturningOne() {
	rc.container.limOnce.Do(func() {
		rc.container.ret.LimitReturningOne()
	})
}

func (rc *retCache) Sql(options *driver.SqlOptions) (string, []any, error) {
	return rc.container.ret.Sql(options)
}

func (rc *retCache) PreparedSql(options *driver.SqlOptions) (string, []any, error) {
	rc.container.resOnce.Do(func() {
		sql, args, err := rc.container.ret.PreparedSql(options)

		rc.container.res.sql = sql
		rc.container.res.args = args
		rc.container.res.err = err
	})

	return rc.container.res.sql, rc.container.res.args, rc.container.res.err
}
