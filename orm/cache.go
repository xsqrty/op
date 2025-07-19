package orm

import (
	"sync"

	"github.com/xsqrty/op"
	"github.com/xsqrty/op/cache"
	"github.com/xsqrty/op/driver"
)

// ReturnableContainer defines an interface for creating and managing returnable operations with cached arguments.
type ReturnableContainer interface {
	Use(args cache.Args) op.Returnable
}

// retContainer is a structure that handles concurrent operations and ensures safe initialization for returnable components.
type retContainer struct {
	res cache.Result
	ret op.Returnable

	retM    sync.RWMutex
	resOnce sync.Once
	limOnce sync.Once
	retOnce sync.Once
}

// retCache is a structure that combines caching arguments and a container for managing SQL operation returns.
type retCache struct {
	args      cache.Args
	container *retContainer
}

// NewReturnableCache creates a new ReturnableContainer instance for the provided op.Returnable input.
func NewReturnableCache(input op.Returnable) ReturnableContainer {
	return &retContainer{
		ret: input,
	}
}

// Use initializes a retCache with the provided cache arguments and links it to the retContainer, returning it as Returnable.
func (rc *retContainer) Use(args cache.Args) op.Returnable {
	return &retCache{
		args:      args,
		container: rc,
	}
}

// UsingTables returns a list of table names utilized in the underlying SQL operation.
func (rc *retCache) UsingTables() []string {
	return rc.container.ret.UsingTables()
}

// With returns the `WITH` clause as a string, provided by the underlying `retContainer` implementation.
func (rc *retCache) With() string {
	return rc.container.ret.With()
}

// GetReturning returns a slice of cloned op.Alias elements representing the RETURNING clause of the SQL statement.
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

// SetReturning sets the returning fields for the associated SQL statement using the provided Alias slice.
func (rc *retCache) SetReturning(aliases []op.Alias) {
	rc.container.retOnce.Do(func() {
		rc.container.retM.Lock()
		defer rc.container.retM.Unlock()

		rc.container.ret.SetReturning(aliases)
	})
}

// LimitReturningOne ensures the limit on returned records is set to one, invoking the operation only once using sync.Once.
func (rc *retCache) LimitReturningOne() {
	rc.container.limOnce.Do(func() {
		rc.container.ret.LimitReturningOne()
	})
}

// CounterType retrieves the execution counter type used, differentiating between query and execution counters.
func (rc *retCache) CounterType() op.CounterType {
	return rc.container.ret.CounterType()
}

// Sql generates an SQL query string along with the associated arguments based on the provided SqlOptions settings.
func (rc *retCache) Sql(options *driver.SqlOptions) (string, []any, error) {
	return rc.container.ret.Sql(options)
}

// PreparedSql generates a prepared SQL statement and argument list based on the given SqlOptions, with caching optimization.
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
