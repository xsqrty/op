package op

import (
	"strings"

	"github.com/xsqrty/op/driver"
)

const (
	concatAnd = " AND "
	concatOr  = " OR "
)

type (
	// Group represents a collection of driver.Sqler elements combined without a specific logical operator.
	Group []driver.Sqler
	// And represents a collection of driver.Sqler elements combined using the logical AND operator.
	And []driver.Sqler
	// Or represents a collection of driver.Sqler elements combined using the logical OR operator.
	Or []driver.Sqler
)

// Sql generates a SQL string, its corresponding arguments, and an error, concatenating components using "AND".
func (l And) Sql(options *driver.SqlOptions) (string, []any, error) {
	return buildConcat(concatAnd, l, options)
}

// Sql generates an SQL string using OR conjunction, along with corresponding arguments, based on the provided options.
func (l Or) Sql(options *driver.SqlOptions) (string, []any, error) {
	return buildConcat(concatOr, l, options)
}

// buildConcat constructs a SQL string by concatenating multiple Sqler elements using a specified joiner string.
// It optionally wraps the result in parentheses if there are multiple elements.
// Returns the concatenated SQL string, a combined slice of arguments, and an error if any occurs during SQL generation.
func buildConcat(
	joiner string,
	l []driver.Sqler,
	options *driver.SqlOptions,
) (string, []any, error) {
	var buf strings.Builder
	var resultArgs []any
	wrap := len(l) > 1

	if wrap {
		buf.WriteByte('(')
	}

	for i, b := range l {
		sql, args, err := b.Sql(options)
		if err != nil {
			return "", nil, err
		}

		buf.WriteString(sql)
		if i != len(l)-1 {
			buf.WriteString(joiner)
		}

		resultArgs = append(resultArgs, args...)
	}

	if wrap {
		buf.WriteByte(')')
	}

	return buf.String(), resultArgs, nil
}
