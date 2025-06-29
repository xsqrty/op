package op

import (
	"github.com/xsqrty/op/driver"
	"strings"
)

const (
	concatAnd = " AND "
	concatOr  = " OR "
)

type Group []driver.Sqler
type And []driver.Sqler
type Or []driver.Sqler

func (l And) Sql(options *driver.SqlOptions) (string, []any, error) {
	return buildConcat(concatAnd, l, options)
}

func (l Or) Sql(options *driver.SqlOptions) (string, []any, error) {
	return buildConcat(concatOr, l, options)
}

func buildConcat(joiner string, l []driver.Sqler, options *driver.SqlOptions) (string, []any, error) {
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
