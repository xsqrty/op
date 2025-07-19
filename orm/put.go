package orm

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/xsqrty/op"
	"github.com/xsqrty/op/cache"
)

// PutBuilder provides methods to configure and execute an insert or update operation for a given model.
type PutBuilder[T any] interface {
	// Log sets a LoggerHandler to log executed queries.
	Log(handler LoggerHandler) PutBuilder[T]
	// With executes the insert/update operation within the provided context and database.
	With(ctx context.Context, db Queryable) error
}

// put represents a structure for performing PUT operations with a specified table and model item.
type put[T any] struct {
	logger LoggerHandler
	table  string
	item   *T
}

// putCache is a concurrent map used to store and retrieve cached ReturnableContainer instances for specific operations.
var putCache sync.Map

// Put creates a PutBuilder to insert or update a record in the specified table based on the provided model.
func Put[T any](table string, model *T) PutBuilder[T] {
	return &put[T]{
		table: table,
		item:  model,
	}
}

// With executes a query using context and database, updating the item with the result or returning an error if it fails.
func (p *put[T]) With(ctx context.Context, db Queryable) error {
	ret, err := p.getReturnable()
	if err != nil {
		return err
	}

	upd, err := Query[T](ret).Log(p.logger).GetOne(ctx, db)
	if err != nil {
		return err
	}

	*p.item = *upd
	return nil
}

// Log sets the LoggerHandler for the put operation to log queries, arguments, and errors, and returns the PutBuilder.
func (p *put[T]) Log(lh LoggerHandler) PutBuilder[T] {
	p.logger = lh
	return p
}

// getReturnable processes the input item and generates a returnable SQL operation or an error if processing fails.
func (p *put[T]) getReturnable() (op.Returnable, error) {
	md, err := getModelDetails(p.table, p.item)
	if err != nil {
		return nil, err
	}

	if md.primaryAsTag == "" {
		return nil, fmt.Errorf("no primary key for model %s", p.table)
	}

	fields, ok := md.tags[p.table]
	if !ok {
		return nil, fmt.Errorf("no such target for model %s", p.table)
	}

	setters, err := getSettersByTags(md, p.table, fields)
	if err != nil {
		return nil, err
	}

	pointers, err := getKeysPointers(p.item, setters, fields)
	if err != nil {
		return nil, err
	}

	args := cache.Args{}
	usePrimaryKey := true

	for i := range fields {
		if md.tagsDetails[p.table][fields[i]].isAggregated {
			continue
		}

		if fields[i] == md.primaryAsTag && pointers[i] != nil &&
			reflect.ValueOf(pointers[i]).Elem().IsZero() {
			usePrimaryKey = false
			continue
		}

		args[fields[i]] = reflect.ValueOf(pointers[i]).Elem().Interface()
	}

	return p.getCache(md, pointers, fields, usePrimaryKey).Use(args), nil
}

// getCache retrieves or initializes a cached ReturnableContainer based on metadata, fields, pointers, and key usage.
// It manages caching using a unique cache key, ensures thread-safety, and assembles SQL operations for the insert.
func (p *put[T]) getCache(
	md *modelDetails,
	pointers []any,
	fields []string,
	usePrimaryKey bool,
) ReturnableContainer {
	cacheKey := p.table
	if usePrimaryKey {
		cacheKey += "_primary"
	}

	typ := reflect.ValueOf(p.item).Type()
	if cachedMap, ok := putCache.Load(cacheKey); ok {
		if cacheInner, ok := cachedMap.(*sync.Map).Load(typ); ok {
			return cacheInner.(ReturnableContainer)
		}
	}

	inserting := op.Inserting{}
	updates := op.Updates{}
	aliases := make([]op.Alias, 0, len(fields))

	for i := range fields {
		if md.tagsDetails[p.table][fields[i]].isAggregated {
			continue
		}

		aliases = append(aliases, op.ColumnAlias(op.Column(fields[i])))
		if fields[i] == md.primaryAsTag && pointers[i] != nil &&
			reflect.ValueOf(pointers[i]).Elem().IsZero() {
			continue
		}

		inserting[fields[i]] = cache.Arg(fields[i])
		updates[fields[i]] = op.Excluded(fields[i])
	}

	insert := op.Insert(p.table, inserting).OnConflict(md.primaryAsTag, op.DoUpdate(updates))
	insert.SetReturning(aliases)

	result := NewReturnableCache(insert)
	inner, _ := putCache.LoadOrStore(cacheKey, &sync.Map{})
	inner.(*sync.Map).Store(typ, result)

	return result
}
