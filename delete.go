package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type DeleteBuilder struct {
	table         alias
	returningKeys []alias
	where         And
	err           error
}

func Delete(table any) *DeleteBuilder {
	db := &DeleteBuilder{}
	switch val := table.(type) {
	case string:
		db.table = columnAlias(Column(val))
	case alias:
		db.table = val
	default:
		db.err = fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, table)
		return db
	}

	return db
}

func (db *DeleteBuilder) Where(exp ...driver.Sqler) *DeleteBuilder {
	if len(exp) > 0 {
		db.where = append(db.where, append(And{}, exp...))
	}

	return db
}

func (db *DeleteBuilder) Returning(keys ...any) *DeleteBuilder {
	err := db.setReturning(keys)
	if err != nil {
		db.err = err
	}

	return db
}

func (db *DeleteBuilder) Sql(options *driver.SqlOptions) (string, []interface{}, error) {
	if db.err != nil {
		return "", nil, db.err
	}

	var buf bytes.Buffer
	var args []interface{}

	buf.WriteString("DELETE FROM ")
	sqlTable, tableArgs, err := db.table.Sql(options)
	if err != nil {
		return "", nil, err
	}

	args = append(args, tableArgs...)
	buf.WriteString(sqlTable)

	if len(db.where) > 0 {
		buf.WriteString(" WHERE ")
		sql, whereArgs, err := db.where.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, whereArgs...)
		buf.WriteString(sql)
	}

	if len(db.returningKeys) > 0 {
		buf.WriteString(" RETURNING ")
		sqlRet, retArgs, err := concatFields(db.returningKeys, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, retArgs...)
		buf.WriteString(sqlRet)
	}

	return buf.String(), args, nil
}

func (db *DeleteBuilder) LimitReturningOne() {
	return
}

func (db *DeleteBuilder) With() string {
	return db.table.Alias()
}

func (db *DeleteBuilder) UsingTables() []string {
	return []string{db.table.Alias()}
}

func (db *DeleteBuilder) GetReturning() []alias {
	return db.returningKeys
}

func (db *DeleteBuilder) SetReturning(keys []any) error {
	return db.setReturning(keys)
}

func (db *DeleteBuilder) SetReturningAliases(keys []alias) {
	db.returningKeys = keys
}

func (db *DeleteBuilder) setReturning(keys []any) error {
	db.returningKeys = nil
	for _, field := range keys {
		switch val := field.(type) {
		case string:
			db.returningKeys = append(db.returningKeys, columnAlias(Column(val)))
		case alias:
			db.returningKeys = append(db.returningKeys, val)
		default:
			return fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, field)
		}
	}

	return nil
}
