package sqlite

import (
	"strconv"

	"github.com/xsqrty/op/driver"
)

func NewSqlOptions() *driver.SqlOptions {
	return driver.NewSqlOptions(
		driver.WithSafeColumns(),
		driver.WithColumnsDelim('.'),
		driver.WithFieldsDelim(','),
		driver.WithCastFormat(func(val string, typ string) string {
			return "CAST(" + val + " AS " + typ + ")"
		}),
		driver.WithPlaceholderFormat(func(n int) string {
			return "$" + strconv.Itoa(n)
		}),
	)
}
