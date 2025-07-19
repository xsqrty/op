package driver

import (
	"strings"
)

const (
	Placeholder = '?'
)

// sqlOption defines a functional option for configuring a SqlOptions instance.
type sqlOption func(options *SqlOptions)

// SqlOptions defines configuration options for customizing SQL generation behavior.
type SqlOptions struct {
	WrapAliasBegin    byte
	WrapAliasEnd      byte
	WrapColumnBegin   byte
	WrapColumnEnd     byte
	ColumnPartDelim   byte
	FieldsDelim       byte
	IsWrapColumn      bool
	IsWrapAlias       bool
	IsColumnPartDelim bool
	SafeColumns       bool
	CastFormat        func(val string, typ string) string
	PlaceholderFormat func(number int) string
}

// Sqler defines the interface for generating SQL strings with arguments.
type Sqler interface {
	Sql(*SqlOptions) (string, []any, error)
}

// PreparedSqler defines the interface for generating prepared SQL statements with arguments.
// PreparedSql should return a prepared statement query.
type PreparedSqler interface {
	PreparedSql(*SqlOptions) (string, []any, error)
}

// NewSqlOptions creates a new SqlOptions instance configured using the provided sqlOption functional options.
func NewSqlOptions(option ...sqlOption) *SqlOptions {
	options := &SqlOptions{}
	for _, opt := range option {
		opt(options)
	}

	return options
}

// WithWrapAlias enables alias wrapping, configure the alias wrapping characters.
func WithWrapAlias(begin byte, end byte) sqlOption {
	return func(options *SqlOptions) {
		options.WrapAliasBegin = begin
		options.WrapAliasEnd = end
		options.IsWrapAlias = true
	}
}

// WithFieldsDelim sets the delimiter used to separate fields in SQL statement.
func WithFieldsDelim(delim byte) sqlOption {
	return func(options *SqlOptions) {
		options.FieldsDelim = delim
	}
}

// WithSafeColumns enables checking for invalid characters for the fields
func WithSafeColumns() sqlOption {
	return func(options *SqlOptions) {
		options.SafeColumns = true
	}
}

// WithColumnsDelim sets the delimiter used to separate parts of a column name in SQL statements.
func WithColumnsDelim(delim byte) sqlOption {
	return func(options *SqlOptions) {
		options.ColumnPartDelim = delim
		options.IsColumnPartDelim = true
	}
}

// WithWrapColumn configures the characters used to wrap column names in SQL statements and enables column wrapping.
func WithWrapColumn(begin byte, end byte) sqlOption {
	return func(options *SqlOptions) {
		options.WrapColumnBegin = begin
		options.WrapColumnEnd = end
		options.IsWrapColumn = true
	}
}

// WithPlaceholderFormat sets a custom placeholder format function used to generate placeholders for SQL arguments.
func WithPlaceholderFormat(format func(int) string) sqlOption {
	return func(options *SqlOptions) {
		options.PlaceholderFormat = format
	}
}

// WithCastFormat sets a custom cast format function to define how values are cast to specific types in SQL statements.
func WithCastFormat(format func(val string, typ string) string) sqlOption {
	return func(options *SqlOptions) {
		options.CastFormat = format
	}
}

// Sql generates an SQL query string and arguments.
func Sql(b Sqler, options *SqlOptions) (string, []any, error) {
	sql, args, err := b.Sql(options)
	if err != nil {
		return "", nil, err
	}

	if options.PlaceholderFormat != nil {
		var buf strings.Builder
		buf.Grow(len(sql) + len(args))

		pos := 0
		index := 1

		for i := 0; i < len(sql); i++ {
			r := sql[i]
			if r == Placeholder {
				if i+1 < len(sql) {
					switch sql[i+1] {
					case '#', '|', '>', '&', '?':
						i++
						continue
					}
				}

				buf.WriteString(sql[pos:i])
				buf.WriteString(options.PlaceholderFormat(index))

				pos = i + 1
				index++
			}
		}

		buf.WriteString(sql[pos:])
		return buf.String(), args, nil
	}

	return sql, args, err
}
