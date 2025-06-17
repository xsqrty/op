package op

import (
	"bytes"
	"github.com/xsqrty/op/driver"
)

type list []any

func (l list) Sql(options *driver.SqlOptions) (string, []any, error) {
	return joinList(options.FieldsDelim, l, false, options)
}

func joinList(joiner byte, list []any, strAsCol bool, options *driver.SqlOptions) (string, []any, error) {
	var args []any
	var buf bytes.Buffer

	for i := range list {
		item := list[i]
		if strAsCol {
			if value, ok := item.(string); ok {
				item = Column(value)
			}
		}

		switch field := item.(type) {
		case driver.Sqler:
			sql, fieldArgs, err := field.Sql(options)
			if err != nil {
				return "", nil, err
			}

			args = append(args, fieldArgs...)
			buf.WriteString(sql)
		default:
			args = append(args, list[i])
			buf.WriteByte(driver.Placeholder)
		}

		if i != len(list)-1 {
			buf.WriteByte(joiner)
		}
	}

	return buf.String(), args, nil
}
