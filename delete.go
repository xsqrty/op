package op

import (
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

// DeleteBuilder defines an interface to construct SQL DELETE statements with customizable conditions and options.
type DeleteBuilder interface {
	// Where adds a conditional expression to the DELETE statement and returns the updated DeleteBuilder.
	Where(exp driver.Sqler) DeleteBuilder
	// Returning adds the specified keys to the list of columns to be returned after executing the DELETE statement.
	Returning(keys ...any) DeleteBuilder
	// LimitReturningOne sets return only one row.
	// The method implements the interface Returnable.
	LimitReturningOne()
	// With retrieves the alias of the table associated with the deleteBuilder.
	With() string
	// UsingTables returns a slice of strings representing the table aliases involved in the current delete operation.
	UsingTables() []string
	// GetReturning retrieves the list of Alias objects specified in the RETURNING clause for the DELETE statement.
	GetReturning() []Alias
	// SetReturning sets the list of columns (as Aliases) to be included in the RETURNING clause of the DELETE statement.
	SetReturning(keys []Alias)
	// CounterType returns the CounterType representing the type of operation or counter associated with the builder.
	CounterType() CounterType
	// PreparedSql generates a prepared SQL query string and arguments using the provided SQL options.
	PreparedSql(options *driver.SqlOptions) (string, []any, error)
	// Sql generates an SQL DELETE statement with optional WHERE and RETURNING clauses based on the provided SqlOptions.
	Sql(options *driver.SqlOptions) (string, []any, error)
}

// deleteBuilder represents a SQL DELETE statement builder with configurable table, conditions, and returning keys.
type deleteBuilder struct {
	table         Alias
	returningKeys []Alias
	where         And
	err           error
}

// is a compile-time assertion ensuring DeleteBuilder satisfies the Returnable interface.
var _ Returnable = DeleteBuilder(nil)

// Delete initializes a DeleteBuilder for creating a SQL DELETE statement for the specified table.
// The table parameter must be a string or an Alias.
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

// Where appends a SQL condition to the deleteBuilder's WHERE clause and returns the updated DeleteBuilder instance.
func (db *deleteBuilder) Where(exp driver.Sqler) DeleteBuilder {
	if exp != nil {
		db.where = append(db.where, exp)
	}

	return db
}

// Returning adds the specified keys to the list of columns to return after the DELETE statement execution.
func (db *deleteBuilder) Returning(keys ...any) DeleteBuilder {
	err := db.setReturning(keys)
	if err != nil {
		db.err = err
	}

	return db
}

// Sql generates the SQL query for a DELETE operation, including WHERE conditions and RETURNING clauses if specified.
func (db *deleteBuilder) Sql(options *driver.SqlOptions) (string, []any, error) {
	if db.err != nil {
		return "", nil, db.err
	}

	var buf strings.Builder
	var args []any

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

// PreparedSql generates a prepared SQL query string and arguments using the provided SQL options.
func (db *deleteBuilder) PreparedSql(
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	return driver.Sql(db, options)
}

// LimitReturningOne sets return only one row.
// The method implements the interface Returnable.
func (db *deleteBuilder) LimitReturningOne() {}

// With retrieves the alias of the table associated with the deleteBuilder.
func (db *deleteBuilder) With() string {
	return db.table.Alias()
}

// UsingTables returns a list of table aliases used in the current delete operation.
func (db *deleteBuilder) UsingTables() []string {
	return []string{db.table.Alias()}
}

// GetReturning returns the list of Alias objects representing the RETURNING clause for the deleteBuilder instance.
func (db *deleteBuilder) GetReturning() []Alias {
	return db.returningKeys
}

// SetReturning sets the list of columns to be returned after the DELETE operation.
func (db *deleteBuilder) SetReturning(keys []Alias) {
	db.returningKeys = keys
}

// CounterType returns the CounterType associated with the deleteBuilder, which is always CounterExec.
func (db *deleteBuilder) CounterType() CounterType {
	return CounterExec
}

// setReturning updates the keys to be returned after a delete query. It accepts strings or Alias types as input.
// Returns an error if any key is not a string or Alias.
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
