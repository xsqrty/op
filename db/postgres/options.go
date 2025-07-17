package postgres

import (
	"strconv"

	"github.com/xsqrty/op/driver"
)

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
