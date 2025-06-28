package driver

import (
	"strconv"
)

func NewPostgresSqlOptions() *SqlOptions {
	return NewSqlOptions(
		WithSafeColumns(),
		WithColumnsDelim('.'),
		WithFieldsDelim(','),
		WithWrapColumn('"', '"'),
		WithWrapAlias('"', '"'),
		WithCastFormat(func(val string, typ string) string {
			return val + "::" + typ
		}),
		WithPlaceholderFormat(func(n int) string {
			return "$" + strconv.Itoa(n)
		}),
	)
}

func NewSqliteSqlOptions() *SqlOptions {
	return NewSqlOptions(
		WithSafeColumns(),
		WithColumnsDelim('.'),
		WithFieldsDelim(','),
		WithCastFormat(func(val string, typ string) string {
			return "CAST(" + val + " AS " + typ + ")"
		}),
		WithPlaceholderFormat(func(n int) string {
			return "$" + strconv.Itoa(n)
		}),
	)
}
