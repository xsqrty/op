package testutil

import (
	"fmt"
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
			return fmt.Sprintf("CAST(%s AS %s)", val, typ)
		}),
	)
}
