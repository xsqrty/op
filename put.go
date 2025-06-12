package op

import (
	"context"
	"fmt"
	"reflect"
)

type put[T any] struct {
	table string
	item  *T
}

func Put[T any](table string, model *T) *put[T] {
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
		return fmt.Errorf("no such field for model %s", p.table)
	}

	setters, err := getSettersKeysByTags(md, p.table, fields)
	if err != nil {
		return err
	}

	pointers, err := getPointersByModelSetters(p.item, setters, fields)
	if err != nil {
		return err
	}

	inserting := Inserting{}
	updates := Updates{}
	for i := range fields {
		if fields[i] == md.primaryAsTag && pointers[i] != nil && reflect.ValueOf(pointers[i]).Elem().IsZero() {
			continue
		}

		inserting[fields[i]] = reflect.ValueOf(pointers[i]).Elem().Interface()
		updates[fields[i]] = Excluded(fields[i])
	}

	aliases := make([]alias, len(fields))
	for i := range fields {
		aliases[i] = columnAlias(Column(fields[i]))
	}

	if md.primaryAsTag == "" {
		return fmt.Errorf("no primary key for model %s", p.table)
	}

	insert := Insert(p.table, inserting).OnConflict(md.primaryAsTag, DoUpdate(updates))
	insert.SetReturningAliases(aliases)
	upd, err := Query[T](insert).GetOne(ctx, db)
	if err != nil {
		return err
	}

	*p.item = *upd
	return nil
}
