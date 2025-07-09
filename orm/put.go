package orm

import (
	"context"
	"fmt"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/cache"
	"reflect"
	"sync"
)

type PutBuilder[T any] interface {
	Log(handler LoggerHandler) PutBuilder[T]
	With(ctx context.Context, db Queryable) error
}

type put[T any] struct {
	logger LoggerHandler
	table  string
	item   *T
}

var putCache sync.Map

func Put[T any](table string, model *T) PutBuilder[T] {
	return &put[T]{
		table: table,
		item:  model,
	}
}

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

func (p *put[T]) Log(lh LoggerHandler) PutBuilder[T] {
	p.logger = lh
	return p
}

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

		if fields[i] == md.primaryAsTag && pointers[i] != nil && reflect.ValueOf(pointers[i]).Elem().IsZero() {
			usePrimaryKey = false
			continue
		}

		args[fields[i]] = reflect.ValueOf(pointers[i]).Elem().Interface()
	}

	return p.getCache(md, pointers, fields, usePrimaryKey).Use(args), nil
}

func (p *put[T]) getCache(md *modelDetails, pointers []any, fields []string, usePrimaryKey bool) ReturnableContainer {
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
		if fields[i] == md.primaryAsTag && pointers[i] != nil && reflect.ValueOf(pointers[i]).Elem().IsZero() {
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
