package op

import (
	"strings"

	"github.com/xsqrty/op/driver"
)

// list represents a slice of any type, commonly used for dynamic SQL generation and argument handling.
type list []any

// Sql generates the SQL string, arguments, and possible error by joining the elements in the list based on SqlOptions.
func (l list) Sql(options *driver.SqlOptions) (string, []any, error) {
	return joinList(options.FieldsDelim, l, false, options)
}

// joinList generates an SQL string by joining elements of a list with a specified byte delimiter.
// The strAsCol parameter defines whether string elements are treated as SQL column names.
// It returns the constructed SQL string, a slice of arguments, and an error if encountered during processing.
func joinList(
	joiner byte,
	list []any,
	strAsCol bool,
	options *driver.SqlOptions,
) (string, []any, error) {
	var args []any
	var buf strings.Builder

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
