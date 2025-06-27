package testutil

import (
	"github.com/xsqrty/op/driver"
)

func NewDefaultOptions() *driver.SqlOptions {
	return driver.NewSqlOptions(
		driver.WithSafeColumns(),
		driver.WithColumnsDelim('.'),
		driver.WithFieldsDelim(','),
		driver.WithWrapColumn('"', '"'),
		driver.WithWrapAlias('"', '"'),
		driver.WithCastFormat(func(val string, typ string) string {
			return "CAST(" + val + " AS " + typ + ")"
		}),
	)
}
