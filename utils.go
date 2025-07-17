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

func exprOrVal(v any, options *driver.SqlOptions) (sql string, args []any, err error) {
	if b, ok := v.(driver.Sqler); ok {
		sql, args, err = b.Sql(options)
		return
	}

	return string(driver.Placeholder), []any{v}, nil
}

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
