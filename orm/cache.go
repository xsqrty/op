package orm

import (
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/cache"
	"github.com/xsqrty/op/driver"
	"sync"
)

type ReturnableContainer interface {
	Use(args cache.Args) op.Returnable
}

type retContainer struct {
	res    cache.Result
	ret    op.Returnable
	retErr error

	retM    sync.RWMutex
	resOnce sync.Once
	limOnce sync.Once
	salOnce sync.Once
	sreOnce sync.Once
}

type retCache struct {
	args      cache.Args
	container *retContainer
}

func NewReturnableCache(input op.Returnable) ReturnableContainer {
	return &retContainer{
		ret: input,
	}
}

func (rc *retContainer) Use(args cache.Args) op.Returnable {
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
	rc.container.retM.RLock()
	defer rc.container.retM.RUnlock()

	returning := rc.container.ret.GetReturning()
	result := make([]op.Alias, len(returning))

	for i := 0; i < len(returning); i++ {
		result[i] = returning[i].Clone()
	}

	return result
}

func (rc *retCache) SetReturning(fields []any) error {
	rc.container.sreOnce.Do(func() {
		rc.container.retM.Lock()
		defer rc.container.retM.Unlock()

		rc.container.retErr = rc.container.ret.SetReturning(fields)
	})

	return rc.container.retErr
}

func (rc *retCache) SetReturningAliases(aliases []op.Alias) {
	rc.container.salOnce.Do(func() {
		rc.container.retM.Lock()
		defer rc.container.retM.Unlock()

		rc.container.ret.SetReturningAliases(aliases)
	})
}

func (rc *retCache) LimitReturningOne() {
	rc.container.limOnce.Do(func() {
		rc.container.ret.LimitReturningOne()
	})
}

func (rc *retCache) CounterType() op.CounterType {
	return rc.container.ret.CounterType()
}

func (rc *retCache) Sql(options *driver.SqlOptions) (string, []any, error) {
	return rc.container.ret.Sql(options)
}

func (rc *retCache) PreparedSql(options *driver.SqlOptions) (string, []any, error) {
	rc.container.resOnce.Do(func() {
		sql, args, err := rc.container.ret.PreparedSql(options)

		if err != nil {
			rc.container.res.Err = err
			return
		}

		cache.PrepareArgs(&rc.container.res, sql, args)
	})

	args, err := cache.GetArgs(&rc.container.res, rc.args)
	if err != nil {
		return "", nil, err
	}

	return rc.container.res.Sql, args, rc.container.res.Err
}
