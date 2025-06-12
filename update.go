package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type Updates map[string]any

type UpdateBuilder struct {
	table         *alias
	returningKeys []alias
	updatesKeys   []Column
	updatesVals   []driver.Sqler
	where         And
	err           error
}

func Update(table any, updates Updates) *UpdateBuilder {
	ub := &UpdateBuilder{}
	if table != nil {
		switch val := table.(type) {
		case string:
			al := columnAlias(Column(val))
			ub.table = &al
		case alias:
			ub.table = &val
		default:
			ub.err = fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, table)
			return ub
		}
	}

	ub.setUpdates(updates)
	return ub
}

func (ub *UpdateBuilder) Where(exp driver.Sqler) *UpdateBuilder {
	if exp != nil {
		ub.where = append(ub.where, exp)
	}

	return ub
}

func (ub *UpdateBuilder) Returning(keys ...any) *UpdateBuilder {
	err := ub.setReturning(keys)
	if err != nil {
		ub.err = err
	}

	return ub
}

func (ub *UpdateBuilder) Sql(options *driver.SqlOptions) (string, []interface{}, error) {
	if ub.err != nil {
		return "", nil, ub.err
	}

	if len(ub.updatesKeys) == 0 {
		return "", nil, fmt.Errorf("update: %w", ErrFieldsEmpty)
	}

	var buf bytes.Buffer
	var args []interface{}

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

func (ub *UpdateBuilder) LimitReturningOne() {
	return
}

func (ub *UpdateBuilder) With() string {
	return ub.table.Alias()
}

func (ub *UpdateBuilder) UsingTables() []string {
	return []string{ub.table.Alias()}
}

func (ub *UpdateBuilder) GetReturning() []alias {
	return ub.returningKeys
}

func (ub *UpdateBuilder) SetReturning(keys []any) error {
	return ub.setReturning(keys)
}

func (ub *UpdateBuilder) SetReturningAliases(keys []alias) {
	ub.returningKeys = keys
}

func (ub *UpdateBuilder) setReturning(keys []any) error {
	ub.returningKeys = nil
	for i := range keys {
		switch val := keys[i].(type) {
		case string:
			ub.returningKeys = append(ub.returningKeys, columnAlias(Column(val)))
		case alias:
			ub.returningKeys = append(ub.returningKeys, val)
		default:
			return fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, keys[i])
		}
	}

	return nil
}

func (ub *UpdateBuilder) setUpdates(updates Updates) {
	ub.updatesKeys = nil
	ub.updatesVals = nil

	for key, val := range updates {
		ub.updatesKeys = append(ub.updatesKeys, Column(key))
		switch val := val.(type) {
		case driver.Sqler:
			ub.updatesVals = append(ub.updatesVals, val)
		default:
			ub.updatesVals = append(ub.updatesVals, Pure(string(driver.Placeholder), val))
		}
	}
}
