package orm

import (
	"context"
	"fmt"
	"github.com/xsqrty/op"
	"reflect"
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

func Put[T any](table string, model *T) PutBuilder[T] {
	return &put[T]{
		table: table,
		item:  model,
	}
}

func (p *put[T]) With(ctx context.Context, db Queryable) error {
	md, err := getModelDetails(p.table, p.item)
	if err != nil {
		return err
	}

	fields, ok := md.tags[p.table]
	if !ok {
		return fmt.Errorf("no such target for model %s", p.table)
	}

	setters, err := getSettersKeysByTags(md, p.table, fields)
	if err != nil {
		return err
	}

	pointers, err := getPointersByModelSetters(p.item, setters, fields)
	if err != nil {
		return err
	}

	inserting := op.Inserting{}
	updates := op.Updates{}
	for i := range fields {
		if fields[i] == md.primaryAsTag && pointers[i] != nil && reflect.ValueOf(pointers[i]).Elem().IsZero() {
			continue
		}

		inserting[fields[i]] = reflect.ValueOf(pointers[i]).Elem().Interface()
		updates[fields[i]] = op.Excluded(fields[i])
	}

	aliases := make([]op.Alias, len(fields))
	for i := range fields {
		aliases[i] = op.ColumnAlias(op.Column(fields[i]))
	}

	if md.primaryAsTag == "" {
		return fmt.Errorf("no primary key for model %s", p.table)
	}

	insert := op.Insert(p.table, inserting).OnConflict(md.primaryAsTag, op.DoUpdate(updates))
	insert.SetReturningAliases(aliases)
	upd, err := Query[T](insert).Log(p.logger).GetOne(ctx, db)
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
