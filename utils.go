package op

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

var (
	ErrUnsupportedType = errors.New("unknown type")
	ErrFieldsEmpty     = errors.New("fields is empty")
)

// exprOrCol processes an input value as either a column name or a SQL expression and generates its SQL representation.
// It accepts a value of any type and an optional SqlOptions for SQL customization, returning the constructed SQL, its arguments, and any error.
// Supported value types include string (interpreted as a column name) and types implementing the Sqler interface.
// Returns an error for unsupported input types.
func exprOrCol(v any, options *driver.SqlOptions) (sql string, args []any, err error) {
	switch val := v.(type) {
	case string:
		sql, args, err = Column(val).Sql(options)
	case driver.Sqler:
		sql, args, err = val.Sql(options)
	default:
		return "", nil, fmt.Errorf("%w: %T", ErrUnsupportedType, v)
	}

	return
}

// exprOrVal processes a value as either a custom SQL expression or a placeholder with arguments.
// If the value implements the Sqler interface, it calls the Sql method to generate the SQL string and arguments.
// Otherwise, it treats the value as a literal and generates a placeholder with the value as its argument.
// Returns the constructed SQL string, arguments, and any error encountered.
func exprOrVal(v any, options *driver.SqlOptions) (sql string, args []any, err error) {
	if b, ok := v.(driver.Sqler); ok {
		sql, args, err = b.Sql(options)
		return
	}

	return string(driver.Placeholder), []any{v}, nil
}

// concatUpdates generates an SQL string and argument list for update statements from keys and values using given options.
// Returns the SQL string, arguments for placeholders, and an error if any key or value is invalid.
func concatUpdates(
	keys []Column,
	values []driver.Sqler,
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	var buf strings.Builder
	for i, key := range keys {
		sqlKey, keyArgs, err := key.Sql(options)
		if err != nil {
			return "", nil, err
		}

		sqlVal, valArgs, err := values[i].Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, keyArgs...)
		args = append(args, valArgs...)

		buf.WriteString(sqlKey)
		buf.WriteByte('=')
		buf.WriteString(sqlVal)

		if i != len(keys)-1 {
			buf.WriteByte(options.FieldsDelim)
		}
	}

	return buf.String(), args, nil
}

// concatFields concatenates multiple SQL fields and their arguments into a single SQL string with arguments and delimiter.
func concatFields[T driver.Sqler](
	fields []T,
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	var buf strings.Builder
	for i := range fields {
		sql, fieldArgs, err := fields[i].Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, fieldArgs...)
		buf.WriteString(sql)

		if i != len(fields)-1 {
			buf.WriteByte(options.FieldsDelim)
		}
	}

	return buf.String(), args, nil
}
