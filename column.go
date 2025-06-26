package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type Column string

type Alias interface {
	Alias() string
	IsPureColumn() bool
	Rename(name string)
	Sql(*driver.SqlOptions) (string, []any, error)
}

type alias struct {
	name         string
	expr         driver.Sqler
	isPureColumn bool
}

const delimByte = '.'

func As(name string, expr driver.Sqler) Alias {
	return &alias{name: name, expr: expr}
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
			return "", nil, fmt.Errorf("target %q contains illegal character '%c'", c, b)
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

func (c Column) IsZero() bool {
	return len(c) == 0
}

func (a *alias) Alias() string {
	return a.name
}

func (a *alias) IsPureColumn() bool {
	return a.isPureColumn
}

func (a *alias) Rename(name string) {
	a.name = name
	if a.isPureColumn {
		a.expr = Column(a.name)
	}
}

func (a *alias) Sql(options *driver.SqlOptions) (string, []any, error) {
	if a.isPureColumn {
		return a.expr.Sql(options)
	}

	sql, args, err := a.expr.Sql(options)
	if err != nil {
		return "", nil, err
	}

	aSql, aArgs, err := wrapAlias(a, options)
	if err != nil {
		return "", nil, err
	}

	sql = "(" + sql + ")"
	sql += " AS " + aSql
	args = append(args, aArgs...)
	return sql, args, nil
}

func columnAlias(col Column) Alias {
	return &alias{isPureColumn: true, expr: col, name: string(col)}
}

func wrapAlias(al *alias, options *driver.SqlOptions) (string, []any, error) {
	var buf bytes.Buffer
	if options.IsWrapAlias {
		buf.WriteByte(options.WrapAliasBegin)
	}

	for i := 0; i < len(al.name); i++ {
		var b = al.name[i]
		if options.SafeColumns && (!isAllowedColumnByte(b) || b == delimByte) {
			return "", nil, fmt.Errorf("alias %q contains illegal character '%c'", al.name, b)
		}

		buf.WriteByte(b)
	}

	if options.IsWrapAlias {
		buf.WriteByte(options.WrapAliasEnd)
	}

	return buf.String(), nil, nil
}

func isAllowedColumnByte(b byte) bool {
	if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' || b == '$' {
		return true
	}

	return false
}
