package driver

import (
	"strings"
)

const (
	Placeholder = '?'
)

type sqlOption func(options *SqlOptions)

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

type Sqler interface {
	Sql(*SqlOptions) (string, []any, error)
}

type PreparedSqler interface {
	PreparedSql(*SqlOptions) (string, []any, error)
}

func NewSqlOptions(option ...sqlOption) *SqlOptions {
	options := &SqlOptions{}
	for _, opt := range option {
		opt(options)
	}

	return options
}

func WithWrapAlias(begin byte, end byte) sqlOption {
	return func(options *SqlOptions) {
		options.WrapAliasBegin = begin
		options.WrapAliasEnd = end
		options.IsWrapAlias = true
	}
}

func WithFieldsDelim(delim byte) sqlOption {
	return func(options *SqlOptions) {
		options.FieldsDelim = delim
	}
}

func WithSafeColumns() sqlOption {
	return func(options *SqlOptions) {
		options.SafeColumns = true
	}
}

func WithColumnsDelim(delim byte) sqlOption {
	return func(options *SqlOptions) {
		options.ColumnPartDelim = delim
		options.IsColumnPartDelim = true
	}
}

func WithWrapColumn(begin byte, end byte) sqlOption {
	return func(options *SqlOptions) {
		options.WrapColumnBegin = begin
		options.WrapColumnEnd = end
		options.IsWrapColumn = true
	}
}

func WithPlaceholderFormat(format func(int) string) sqlOption {
	return func(options *SqlOptions) {
		options.PlaceholderFormat = format
	}
}

func WithCastFormat(format func(val string, typ string) string) sqlOption {
	return func(options *SqlOptions) {
		options.CastFormat = format
	}
}

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
