package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type Column string
type alias struct {
	pure  bool
	alias string
	expr  driver.Sqler
}

const delimByte = '.'

func As(name string, expr driver.Sqler) alias {
	return alias{alias: name, pure: false, expr: expr}
}

func (c Column) Sql(options *driver.SqlOptions) (string, []any, error) {
	var buf bytes.Buffer
	val := []byte(c)
	if options.IsWrapColumn {
		buf.WriteByte(options.WrapColumnBegin)
	}

	for i := 0; i < len(val); i++ {
		var b = val[i]
		if options.SafeColumns && !isAllowedColumnByte(b) {
			return "", nil, fmt.Errorf("column %q contains illegal character '%c'", c, b)
		}

		if b == delimByte {
			if options.IsWrapColumn {
				buf.WriteByte(options.WrapColumnEnd)
			}

			if options.IsColumnPartDelim {
				buf.WriteByte(options.ColumnPartDelim)
			}

			if options.IsWrapColumn {
				buf.WriteByte(options.WrapColumnBegin)
			}
		} else {
			buf.WriteByte(b)
		}
	}

	if options.IsWrapColumn {
		buf.WriteByte(options.WrapColumnEnd)
	}
	return buf.String(), nil, nil
}

func (a alias) Alias() string {
	return a.alias
}

func (a alias) Sql(options *driver.SqlOptions) (string, []any, error) {
	if a.pure {
		if col, ok := a.expr.(Column); ok {
			return col.Sql(options)
		} else {
			return "", nil, fmt.Errorf("no column found in alias")
		}
	}

	sql, args, err := a.expr.Sql(options)
	if err != nil {
		return "", nil, err
	}

	aSql, aArgs, err := wrapAlias(&a, options)
	if err != nil {
		return "", nil, err
	}

	sql = "(" + sql + ")"
	sql += " AS " + aSql
	args = append(args, aArgs...)
	return sql, args, nil
}

func wrapAlias(al *alias, options *driver.SqlOptions) (string, []any, error) {
	var buf bytes.Buffer
	if options.IsWrapAlias {
		buf.WriteByte(options.WrapAliasBegin)
	}

	for i := 0; i < len(al.alias); i++ {
		var b = al.alias[i]
		if options.SafeColumns && !isAllowedColumnByte(b) {
			return "", nil, fmt.Errorf("alias %q contains illegal character '%c'", al.alias, b)
		}

		buf.WriteByte(b)
	}

	if options.IsWrapAlias {
		buf.WriteByte(options.WrapAliasEnd)
	}

	return buf.String(), nil, nil
}

func setAliasColumn(al *alias, col Column) {
	al.expr = col
}

func columnAlias(col Column) alias {
	return alias{pure: true, expr: col, alias: string(col)}
}

func isAllowedColumnByte(b byte) bool {
	if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' || b == '$' {
		return true
	}

	return false
}
