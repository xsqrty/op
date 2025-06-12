package op

import (
	"bytes"
	"github.com/xsqrty/op/driver"
)

type list []any

func (l list) Sql(options *driver.SqlOptions) (string, []any, error) {
	return joinList(options.FieldsDelim, l, options)
}

func joinList(joiner byte, list []any, options *driver.SqlOptions) (string, []any, error) {
	var args []any
	var buf bytes.Buffer

	for i := range list {
		if field, ok := list[i].(driver.Sqler); ok {
			sql, fieldArgs, err := field.Sql(options)
			if err != nil {
				return "", nil, err
			}

			args = append(args, fieldArgs...)
			buf.WriteString(sql)
		} else {
			args = append(args, list[i])
			buf.WriteByte(driver.Placeholder)
		}

		if i != len(list)-1 {
			buf.WriteByte(joiner)
		}
	}

	return buf.String(), args, nil
}
