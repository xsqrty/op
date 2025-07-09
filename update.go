package op

import (
	"fmt"
	"github.com/xsqrty/op/driver"
	"strings"
)

type UpdateBuilder interface {
	Where(exp driver.Sqler) UpdateBuilder
	Returning(keys ...any) UpdateBuilder
	LimitReturningOne()
	With() string
	UsingTables() []string
	GetReturning() []Alias
	SetReturning(keys []Alias)
	CounterType() CounterType
	PreparedSql(options *driver.SqlOptions) (string, []any, error)
	Sql(options *driver.SqlOptions) (string, []any, error)
}

type Updates map[string]any

type updateBuilder struct {
	table         Alias
	returningKeys []Alias
	updatesKeys   []Column
	updatesVals   []driver.Sqler
	where         And
	err           error
}

var _ Returnable = UpdateBuilder(nil)

func Update(table any, updates Updates) UpdateBuilder {
	ub := &updateBuilder{}
	if table != nil {
		switch val := table.(type) {
		case string:
			ub.table = ColumnAlias(Column(val))
		case Alias:
			ub.table = val
		default:
			ub.err = fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, table)
			return ub
		}
	}

	ub.setUpdates(updates)
	return ub
}

func (ub *updateBuilder) Where(exp driver.Sqler) UpdateBuilder {
	if exp != nil {
		ub.where = append(ub.where, exp)
	}

	return ub
}

func (ub *updateBuilder) Returning(keys ...any) UpdateBuilder {
	err := ub.setReturning(keys)
	if err != nil {
		ub.err = err
	}

	return ub
}

func (ub *updateBuilder) Sql(options *driver.SqlOptions) (string, []any, error) {
	if ub.err != nil {
		return "", nil, ub.err
	}

	if len(ub.updatesKeys) == 0 {
		return "", nil, fmt.Errorf("update: %w", ErrFieldsEmpty)
	}

	var buf strings.Builder
	var args []any

	buf.WriteString("UPDATE ")
	if ub.table != nil {
		sqlTable, tableArgs, err := ub.table.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, tableArgs...)
		buf.WriteString(sqlTable)
		buf.WriteByte(' ')
	}

	buf.WriteString("SET ")
	sqlUpdates, updatesArgs, err := concatUpdates(ub.updatesKeys, ub.updatesVals, options)
	if err != nil {
		return "", nil, err
	}

	args = append(args, updatesArgs...)
	buf.WriteString(sqlUpdates)

	if len(ub.where) > 0 {
		buf.WriteString(" WHERE ")
		sql, whereArgs, err := ub.where.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, whereArgs...)
		buf.WriteString(sql)
	}

	if len(ub.returningKeys) > 0 {
		buf.WriteString(" RETURNING ")
		sqlRet, retArgs, err := concatFields(ub.returningKeys, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, retArgs...)
		buf.WriteString(sqlRet)
	}

	return buf.String(), args, nil
}

func (ub *updateBuilder) PreparedSql(options *driver.SqlOptions) (sql string, args []any, err error) {
	return driver.Sql(ub, options)
}

func (ub *updateBuilder) LimitReturningOne() {}

func (ub *updateBuilder) With() string {
	return ub.table.Alias()
}

func (ub *updateBuilder) UsingTables() []string {
	return []string{ub.table.Alias()}
}

func (ub *updateBuilder) GetReturning() []Alias {
	return ub.returningKeys
}

func (ub *updateBuilder) SetReturning(keys []Alias) {
	ub.returningKeys = keys
}

func (ub *updateBuilder) CounterType() CounterType {
	return CounterExec
}

func (ub *updateBuilder) setReturning(keys []any) error {
	ub.returningKeys = nil
	for i := range keys {
		switch val := keys[i].(type) {
		case string:
			ub.returningKeys = append(ub.returningKeys, ColumnAlias(Column(val)))
		case Alias:
			ub.returningKeys = append(ub.returningKeys, val)
		default:
			return fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, keys[i])
		}
	}

	return nil
}

func (ub *updateBuilder) setUpdates(updates Updates) {
	ub.updatesKeys = nil
	ub.updatesVals = nil

	for key, val := range updates {
		ub.updatesKeys = append(ub.updatesKeys, Column(key))
		switch val := val.(type) {
		case driver.Sqler:
			ub.updatesVals = append(ub.updatesVals, val)
		default:
			ub.updatesVals = append(ub.updatesVals, driver.Pure(string(driver.Placeholder), val))
		}
	}
}
