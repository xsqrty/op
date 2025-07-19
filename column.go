package op

import (
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

// Column represents a SQL column name or identifier.
type Column string

// Alias represents a SQL aliasing interface for renaming expressions or columns in SQL queries.
type Alias interface {
	Alias() string
	IsPureColumn() bool
	Rename(name string)
	Clone() Alias
	Sql(*driver.SqlOptions) (string, []any, error)
}

// alias represents a SQL alias for renaming columns or expressions in queries.
type alias struct {
	name         string
	expr         driver.Sqler
	isPureColumn bool
}

const delimByte = '.'

// As creates a new alias with the given name and SQL expression.
func As(name string, expr driver.Sqler) Alias {
	return &alias{name: name, expr: expr}
}

// Sql generates a SQL representation of the Column using the given SqlOptions. Returns the SQL string, arguments, and error.
func (c Column) Sql(options *driver.SqlOptions) (string, []any, error) {
	var buf strings.Builder
	val := []byte(c)
	if options.IsWrapColumn {
		buf.WriteByte(options.WrapColumnBegin)
	}

	for i := 0; i < len(val); i++ {
		b := val[i]
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

// IsZero returns true if the Column is empty (zero value).
func (c Column) IsZero() bool {
	return len(c) == 0
}

// Alias returns the alias name associated with the object.
func (a *alias) Alias() string {
	return a.name
}

// IsPureColumn checks if the alias represents a pure column without any associated expression or transformation.
func (a *alias) IsPureColumn() bool {
	return a.isPureColumn
}

// Rename changes the alias name to the provided value.
func (a *alias) Rename(name string) {
	a.name = name
	if a.isPureColumn {
		a.expr = Column(a.name)
	}
}

// Clone creates a deep copy of the alias object.
func (a *alias) Clone() Alias {
	return &alias{
		name:         a.name,
		expr:         a.expr,
		isPureColumn: a.isPureColumn,
	}
}

// Sql generates the SQL string, arguments, and error for the alias, handling expression wrapping and alias formatting.
func (a *alias) Sql(options *driver.SqlOptions) (string, []any, error) {
	if a.isPureColumn {
		return a.expr.Sql(options)
	}

	sql, args, err := a.expr.Sql(options)
	if err != nil {
		return "", nil, err
	}

	aSql, err := wrapAlias(a, options)
	if err != nil {
		return "", nil, err
	}

	sql = "(" + sql + ")"
	sql += " AS " + aSql
	return sql, args, nil
}

// ColumnAlias creates a new alias for a simple column reference.
func ColumnAlias(col Column) Alias {
	return &alias{isPureColumn: true, expr: col, name: string(col)}
}

// wrapAlias generates a SQL string representation of an alias, applying wrapping and validation based on provided options.
func wrapAlias(al *alias, options *driver.SqlOptions) (string, error) {
	var buf strings.Builder
	if options.IsWrapAlias {
		buf.WriteByte(options.WrapAliasBegin)
	}

	for i := 0; i < len(al.name); i++ {
		b := al.name[i]
		if options.SafeColumns && (!isAllowedColumnByte(b) || b == delimByte) {
			return "", fmt.Errorf("alias %q contains illegal character '%c'", al.name, b)
		}

		buf.WriteByte(b)
	}

	if options.IsWrapAlias {
		buf.WriteByte(options.WrapAliasEnd)
	}

	return buf.String(), nil
}

// isAllowedColumnByte checks if the given byte is a valid character for a column name.
func isAllowedColumnByte(b byte) bool {
	if b >= 'a' && b <= 'z' ||
		b >= 'A' && b <= 'Z' ||
		b >= '0' && b <= '9' ||
		b == '_' || b == '-' ||
		b == '.' || b == '$' {
		return true
	}

	return false
}
