package cache

import (
	"fmt"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
	"sync"
)

type ReturnableContainer interface {
	Use(args Args) Returnable
}

type Returnable interface {
	UsingTables() []string
	With() string
	GetReturning() []op.Alias
	SetReturning([]any) error
	SetReturningAliases([]op.Alias)
	Sql(options *driver.SqlOptions) (string, []any, error)
	PreparedSql(options *driver.SqlOptions) (string, []any, error)
	LimitReturningOne()
}

type retContainer struct {
	res    result
	ret    Returnable
	retErr error

	resOnce sync.Once
	limOnce sync.Once
	salOnce sync.Once
	sreOnce sync.Once
}

type retCache struct {
	args      Args
	container *retContainer
}

func NewReturnable(input Returnable) ReturnableContainer {
	return &retContainer{
		ret: input,
	}
}

func (rc *retContainer) Use(args Args) Returnable {
	return &retCache{
		args:      args,
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

		if err != nil {
			rc.container.res.err = err
			return
		}

		rc.container.res.sql = sql
		rc.container.res.args = args
		rc.container.res.argsIndexes = make(map[string]int)
		for i := range rc.container.res.args {
			if argPair, ok := rc.container.res.args[i].(*arg); ok {
				rc.container.res.argsIndexes[argPair.key] = i
			}
		}
	})

	if rc.container.res.err != nil {
		return "", nil, rc.container.res.err
	}

	if len(rc.container.res.argsIndexes) != len(rc.args) {
		return "", nil, fmt.Errorf("incorrect argument count")
	}

	args := make([]any, len(rc.container.res.args))
	copy(args, rc.container.res.args)

	for k, v := range rc.args {
		index, ok := rc.container.res.argsIndexes[k]
		if !ok {
			return "", nil, fmt.Errorf("no such arg %q inside container", k)
		}

		args[index] = v
	}

	return rc.container.res.sql, args, rc.container.res.err
}
