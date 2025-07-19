package op

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

// InsertBuilder defines an interface for constructing SQL insert queries with support for columns, values, conflict resolution, and returning.
type InsertBuilder interface {
	// Columns sets the column names for the insert query and returns the InsertBuilder for further chaining.
	Columns(columns ...string) InsertBuilder
	// Values adds a set of values for an SQL insert query, corresponding to the columns specified earlier.
	Values(values ...any) InsertBuilder
	// OnConflict adds conflict resolution behavior to the insert operation using a target and a specified action.
	OnConflict(target any, do driver.Sqler) InsertBuilder
	// Returning specifies the columns or expressions to be returned after an INSERT operation and returns the updated InsertBuilder.
	Returning(keys ...any) InsertBuilder
	// LimitReturningOne restricts the SQL query to return only a single row.
	LimitReturningOne()
	// With returns the alias of the table associated with the updateBuilder.
	With() string
	// UsingTables returns a slice of strings representing the tables being used in the current SQL operation.
	UsingTables() []string
	// GetReturning retrieves the list of Alias objects representing the RETURNING keys for the SQL statement.
	GetReturning() []Alias
	// SetReturning sets the list of returning keys for the SQL insert statement using the provided slice of Alias values.
	SetReturning(keys []Alias)
	// CounterType returns the CounterType of the insert operation, indicating whether it involves execution or querying.
	CounterType() CounterType
	// PreparedSql generates a SQL query string and arguments for execution, based on the provided SQL options. It can return errors.
	PreparedSql(options *driver.SqlOptions) (string, []any, error)
	// Sql generates the final SQL query string along with its arguments using the provided SQL options.
	Sql(options *driver.SqlOptions) (string, []any, error)
}

// Inserting represents a map where keys are column names, and values are the corresponding data to be inserted.
type Inserting map[string]any

// insertBuilder is a type for constructing SQL INSERT statements.
type insertBuilder struct {
	into          Alias
	many          bool
	onConflict    *conflict
	returningKeys []Alias
	insertingKeys []Column
	insertingVals [][]any
	err           error
}

var (
	ErrNoInsertValues = errors.New("no insert values")
	ErrForInsertMany  = errors.New("Values/Columns available only for InsertMany")
)

// is a compile-time assertion ensuring InsertBuilder implements the Returnable interface.
var _ Returnable = InsertBuilder(nil)

// InsertMany initializes an InsertBuilder for inserting multiple rows into a table or entity specified by the `into` parameter.
func InsertMany(into any) InsertBuilder {
	ib := &insertBuilder{many: true}
	ib.setInto(into)

	return ib
}

// Insert initializes and returns an InsertBuilder to construct an insert SQL operation with specified table and values.
func Insert(into any, inserting Inserting) InsertBuilder {
	ib := &insertBuilder{}

	ib.setInto(into)
	ib.setInserting(inserting)
	return ib
}

// Columns specify the column names for the insert operation.
// If the operation is not an InsertMany, sets an error in the builder.
func (ib *insertBuilder) Columns(columns ...string) InsertBuilder {
	if !ib.many {
		ib.err = ErrForInsertMany
		return ib
	}

	ib.insertingKeys = make([]Column, len(columns))
	for i, col := range columns {
		ib.insertingKeys[i] = Column(col)
	}

	return ib
}

// Values append the provided values for insertion and return the InsertBuilder. Sets an error if not in InsertMany mode.
func (ib *insertBuilder) Values(values ...any) InsertBuilder {
	if !ib.many {
		ib.err = ErrForInsertMany
		return ib
	}

	ib.insertingVals = append(ib.insertingVals, values)
	return ib
}

// OnConflict configures the "ON CONFLICT" clause for the insert query, specifying the target and action to perform.
// The target must be a string or an Alias, while the action is defined by a driver.Sqler implementation.
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

// Returning sets the returning keys for the SQL insert statement, using provided string or Alias values.
func (ib *insertBuilder) Returning(keys ...any) InsertBuilder {
	err := ib.setReturning(keys)
	if err != nil {
		ib.err = err
	}

	return ib
}

// Sql generates the SQL query string, arguments, and error based on the configuration of the InsertBuilder instance.
func (ib *insertBuilder) Sql(options *driver.SqlOptions) (string, []any, error) {
	if ib.err != nil {
		return "", nil, ib.err
	}

	if len(ib.insertingKeys) == 0 {
		return "", nil, fmt.Errorf("insert: %w", ErrFieldsEmpty)
	}

	if len(ib.insertingVals) == 0 {
		return "", nil, fmt.Errorf("insert: %w", ErrNoInsertValues)
	}

	var buf strings.Builder
	var args []any

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

// PreparedSql generates a SQL query string and arguments with placeholders pre-applied using the provided SqlOptions.
func (ib *insertBuilder) PreparedSql(
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	return driver.Sql(ib, options)
}

// LimitReturningOne adjusts the builder to limit the SQL query to return only one row from the resulting data.
func (ib *insertBuilder) LimitReturningOne() {}

// With returns the alias of the `into` field in the `insertBuilder`.
func (ib *insertBuilder) With() string {
	return ib.into.Alias()
}

// UsingTables returns a slice of strings containing the alias of the table being inserted into.
func (ib *insertBuilder) UsingTables() []string {
	return []string{ib.into.Alias()}
}

// GetReturning retrieves the list of aliases defined for the RETURNING clause in the SQL INSERT statement.
func (ib *insertBuilder) GetReturning() []Alias {
	return ib.returningKeys
}

// SetReturning sets the returningKeys field to the provided slice of Alias for configuring the SQL RETURNING clause.
func (ib *insertBuilder) SetReturning(keys []Alias) {
	ib.returningKeys = keys
}

// CounterType returns the CounterType for the insertBuilder, which defines the type of SQL counter to execute.
func (ib *insertBuilder) CounterType() CounterType {
	return CounterExec
}

// setReturning processes the provided keys and sets them as returningKeys in the insertBuilder.
// Keys must be of type string or Alias; otherwise, an error is returned.
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

// setInserting initializes insertingKeys and insertingVals with keys and values from the given Inserting map.
func (ib *insertBuilder) setInserting(inserting Inserting) {
	ib.insertingKeys = nil
	ib.insertingVals = nil

	vals := make([]any, 0, len(inserting))
	for key, val := range inserting {
		ib.insertingKeys = append(ib.insertingKeys, Column(key))
		vals = append(vals, val)
	}

	ib.insertingVals = append(ib.insertingVals, vals)
}

// setInto sets the target table for the insert operation. It accepts a string or Alias, otherwise returns an error.
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
