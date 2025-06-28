package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type DeleteBuilder interface {
	Where(exp driver.Sqler) DeleteBuilder
	Returning(keys ...any) DeleteBuilder
	LimitReturningOne()
	With() string
	UsingTables() []string
	GetReturning() []Alias
	SetReturning(keys []any) error
	SetReturningAliases(keys []Alias)
	Sql(options *driver.SqlOptions) (string, []interface{}, error)
}

type deleteBuilder struct {
	table         Alias
	returningKeys []Alias
	where         And
	err           error
}

func Delete(table any) DeleteBuilder {
	db := &deleteBuilder{}
	switch val := table.(type) {
	case string:
		db.table = ColumnAlias(Column(val))
	case Alias:
		db.table = val
	default:
		db.err = fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, table)
		return db
	}

	return db
}

func (db *deleteBuilder) Where(exp driver.Sqler) DeleteBuilder {
	if exp != nil {
		db.where = append(db.where, exp)
	}

	return db
}

func (db *deleteBuilder) Returning(keys ...any) DeleteBuilder {
	err := db.setReturning(keys)
	if err != nil {
		db.err = err
	}

	return db
}

func (db *deleteBuilder) Sql(options *driver.SqlOptions) (string, []interface{}, error) {
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

func (db *deleteBuilder) LimitReturningOne() {}

func (db *deleteBuilder) With() string {
	return db.table.Alias()
}

func (db *deleteBuilder) UsingTables() []string {
	return []string{db.table.Alias()}
}

func (db *deleteBuilder) GetReturning() []Alias {
	return db.returningKeys
}

func (db *deleteBuilder) SetReturning(keys []any) error {
	return db.setReturning(keys)
}

func (db *deleteBuilder) SetReturningAliases(keys []Alias) {
	db.returningKeys = keys
}

func (db *deleteBuilder) setReturning(keys []any) error {
	db.returningKeys = nil
	for _, field := range keys {
		switch val := field.(type) {
		case string:
			db.returningKeys = append(db.returningKeys, ColumnAlias(Column(val)))
		case Alias:
			db.returningKeys = append(db.returningKeys, val)
		default:
			return fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, field)
		}
	}

	return nil
}
