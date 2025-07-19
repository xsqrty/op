package op

import (
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

// UpdateBuilder is an interface for building SQL UPDATE statements with optional WHERE and RETURNING clauses.
type UpdateBuilder interface {
	// Where adds a condition to the WHERE clause of the UPDATE statement.
	Where(exp driver.Sqler) UpdateBuilder
	// Returning adds keys to the RETURNING clause of the UPDATE statement.
	Returning(keys ...any) UpdateBuilder
	// LimitReturningOne sets the SELECT statement to return only one row.
	LimitReturningOne()
	// With returns the alias of the primary table in the UPDATE statement.
	With() string
	// UsingTables retrieves the list of tables used in the UPDATE statement.
	UsingTables() []string
	// GetReturning gets the list of aliases in the RETURNING clause.
	GetReturning() []Alias
	// SetReturning sets the aliases for the RETURNING clause.
	SetReturning(keys []Alias)
	// CounterType gets the type of execution counter for the UPDATE operation.
	CounterType() CounterType
	// PreparedSql generates a prepared SQL statement with placeholders and arguments.
	PreparedSql(options *driver.SqlOptions) (string, []any, error)
	// Sql generates the SQL string and associated arguments.
	Sql(options *driver.SqlOptions) (string, []any, error)
}

// Updates is a type alias for a map with string keys and values of any type, used to define update fields and their values.
type Updates map[string]any

// updateBuilder is an internal implementation for building SQL UPDATE statements with support for WHERE and RETURNING clauses.
type updateBuilder struct {
	table         Alias
	returningKeys []Alias
	updatesKeys   []Column
	updatesVals   []driver.Sqler
	where         And
	err           error
}

// enforces that UpdateBuilder implements the Returnable interface at compile-time.
var _ Returnable = UpdateBuilder(nil)

// Update creates an UpdateBuilder for constructing an SQL UPDATE statement with specified table and update fields.
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

// Where adds a condition to the WHERE clause of the SQL UPDATE statement. Returns the UpdateBuilder for chaining.
func (ub *updateBuilder) Where(exp driver.Sqler) UpdateBuilder {
	if exp != nil {
		ub.where = append(ub.where, exp)
	}

	return ub
}

// Returning adds keys to the RETURNING clause of an SQL UPDATE statement and returns the UpdateBuilder for chaining.
func (ub *updateBuilder) Returning(keys ...any) UpdateBuilder {
	err := ub.setReturning(keys)
	if err != nil {
		ub.err = err
	}

	return ub
}

// Sql generates the SQL string, arguments, and error for the constructed UPDATE statement based on the given options.
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

// PreparedSql generates a prepared SQL query string and arguments using the provided SqlOptions. It also returns any error.
func (ub *updateBuilder) PreparedSql(
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	return driver.Sql(ub, options)
}

// LimitReturningOne sets return only one row.
// The method implements the interface Returnable.
func (ub *updateBuilder) LimitReturningOne() {}

// With returns the alias of the table associated with the updateBuilder.
func (ub *updateBuilder) With() string {
	return ub.table.Alias()
}

// UsingTables returns a slice of strings containing the alias of the table associated with the updateBuilder.
func (ub *updateBuilder) UsingTables() []string {
	return []string{ub.table.Alias()}
}

// GetReturning retrieves the list of aliases specified in the RETURNING clause of the SQL UPDATE statement.
func (ub *updateBuilder) GetReturning() []Alias {
	return ub.returningKeys
}

// SetReturning sets the list of aliases to be included in the RETURNING clause of the SQL UPDATE statement.
func (ub *updateBuilder) SetReturning(keys []Alias) {
	ub.returningKeys = keys
}

// CounterType returns the CounterType value representing the execution type, typically used to signal CounterExec.
func (ub *updateBuilder) CounterType() CounterType {
	return CounterExec
}

// setReturning processes the given keys, validates their types, and sets them as aliases in the RETURNING clause.
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

// setUpdates processes the update map and initializes the updateBuilder with update keys and their corresponding values.
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
