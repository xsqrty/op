package testutil

import (
	"github.com/xsqrty/op/driver"
)

// NewDefaultOptions returns a default configuration of SqlOptions with standard settings for SQL generation behavior.
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
