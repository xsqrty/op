package postgres

import (
	"strconv"

	"github.com/xsqrty/op/driver"
)

// NewSqlOptions creates a new instance of SqlOptions with predefined configurations for postgres.
func NewSqlOptions() *driver.SqlOptions {
	return driver.NewSqlOptions(
		driver.WithSafeColumns(),
		driver.WithColumnsDelim('.'),
		driver.WithFieldsDelim(','),
		driver.WithWrapColumn('"', '"'),
		driver.WithWrapAlias('"', '"'),
		driver.WithCastFormat(func(val string, typ string) string {
			return val + "::" + typ
		}),
		driver.WithPlaceholderFormat(func(n int) string {
			return "$" + strconv.Itoa(n)
		}),
	)
}
