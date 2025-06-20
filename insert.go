package op

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type InsertBuilder interface {
	Columns(columns ...string) InsertBuilder
	Values(values ...any) InsertBuilder
	OnConflict(target any, do driver.Sqler) InsertBuilder
	Returning(keys ...any) InsertBuilder
	LimitReturningOne()
	With() string
	UsingTables() []string
	GetReturning() []Alias
	SetReturning(keys []any) error
	SetReturningAliases(keys []Alias)
	Sql(options *driver.SqlOptions) (string, []interface{}, error)
}

type Inserting map[string]any

type insertBuilder struct {
	into          Alias
	onConflict    *conflict
	returningKeys []Alias
	insertingKeys []Column
	insertingVals [][]any
	err           error
}

var (
	ErrNoInsertValues = errors.New("no insert values")
)

func InsertMany(into any) InsertBuilder {
	ib := &insertBuilder{}
	ib.setInto(into)

	return ib
}

func Insert(into any, inserting Inserting) InsertBuilder {
	ib := &insertBuilder{}

	ib.setInto(into)
	ib.setInserting(inserting)
	return ib
}

func (ib *insertBuilder) Columns(columns ...string) InsertBuilder {
	ib.insertingKeys = make([]Column, len(columns))
	for i, col := range columns {
		ib.insertingKeys[i] = Column(col)
	}

	return ib
}

func (ib *insertBuilder) Values(values ...any) InsertBuilder {
	ib.insertingVals = append(ib.insertingVals, values)
	return ib
}

func (ib *insertBuilder) OnConflict(target any, do driver.Sqler) InsertBuilder {
	conf := &conflict{expr: do}
	switch val := target.(type) {
	case string:
		conf.target = ColumnAlias(Column(val))
	case Alias:
		conf.target = val
	default:
		ib.err = fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, target)
		return ib
	}

	ib.onConflict = conf
	return ib
}

func (ib *insertBuilder) Returning(keys ...any) InsertBuilder {
	err := ib.setReturning(keys)
	if err != nil {
		ib.err = err
	}

	return ib
}

func (ib *insertBuilder) Sql(options *driver.SqlOptions) (string, []interface{}, error) {
	if ib.err != nil {
		return "", nil, ib.err
	}

	if len(ib.insertingKeys) == 0 {
		return "", nil, fmt.Errorf("insert: %w", ErrFieldsEmpty)
	}

	if len(ib.insertingVals) == 0 {
		return "", nil, fmt.Errorf("insert: %w", ErrNoInsertValues)
	}

	var buf bytes.Buffer
	var args []interface{}

	buf.WriteString("INSERT INTO ")
	sqlInto, intoArgs, err := ib.into.Sql(options)
	if err != nil {
		return "", nil, err
	}

	args = append(args, intoArgs...)
	buf.WriteString(sqlInto)
	buf.WriteString(" (")
	sqlKeys, keysArgs, err := concatFields(ib.insertingKeys, options)
	if err != nil {
		return "", nil, err
	}

	args = append(args, keysArgs...)
	buf.WriteString(sqlKeys)

	buf.WriteString(")")
	if len(ib.insertingVals) > 0 {
		buf.WriteString(" VALUES ")
		for i := range ib.insertingVals {
			buf.WriteByte('(')
			sqlVals, valsArgs, err := list(ib.insertingVals[i]).Sql(options)
			if err != nil {
				return "", nil, err
			}

			args = append(args, valsArgs...)
			buf.WriteString(sqlVals)
			buf.WriteByte(')')

			if i != len(ib.insertingVals)-1 {
				buf.WriteByte(options.FieldsDelim)
			}
		}
	}

	if ib.onConflict != nil {
		buf.WriteString(" ON CONFLICT (")
		sqlTar, tarArgs, err := ib.onConflict.target.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, tarArgs...)
		buf.WriteString(sqlTar)
		buf.WriteString(") DO ")
		sqlExp, expArgs, err := ib.onConflict.expr.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, expArgs...)
		buf.WriteString(sqlExp)
	}

	if len(ib.returningKeys) > 0 {
		buf.WriteString(" RETURNING ")
		sqlRet, retArgs, err := concatFields(ib.returningKeys, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, retArgs...)
		buf.WriteString(sqlRet)
	}

	return buf.String(), args, nil
}

func (ib *insertBuilder) LimitReturningOne() {}

func (ib *insertBuilder) With() string {
	return ib.into.Alias()
}

func (ib *insertBuilder) UsingTables() []string {
	return []string{ib.into.Alias()}
}

func (ib *insertBuilder) GetReturning() []Alias {
	return ib.returningKeys
}

func (ib *insertBuilder) SetReturning(keys []any) error {
	return ib.setReturning(keys)
}

func (ib *insertBuilder) SetReturningAliases(keys []Alias) {
	ib.returningKeys = keys
}

func (ib *insertBuilder) setReturning(keys []any) error {
	ib.returningKeys = nil
	for _, field := range keys {
		switch val := field.(type) {
		case string:
			ib.returningKeys = append(ib.returningKeys, ColumnAlias(Column(val)))
		case Alias:
			ib.returningKeys = append(ib.returningKeys, val)
		default:
			return fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, field)
		}
	}

	return nil
}

func (ib *insertBuilder) setInserting(inserting Inserting) {
	ib.insertingKeys = nil
	ib.insertingVals = nil

	var vals []any
	for key, val := range inserting {
		ib.insertingKeys = append(ib.insertingKeys, Column(key))
		vals = append(vals, val)
	}

	ib.insertingVals = append(ib.insertingVals, vals)
}

func (ib *insertBuilder) setInto(into any) {
	switch val := into.(type) {
	case string:
		ib.into = ColumnAlias(Column(val))
	case Alias:
		ib.into = val
	default:
		ib.err = fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, into)
	}
}
