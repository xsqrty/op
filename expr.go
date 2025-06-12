package op

import "github.com/xsqrty/op/driver"

type expr struct {
	sqlText string
	args    []any
}

func Pure(sqlValue string, args ...any) driver.Sqler {
	return expr{sqlValue, args}
}

func Value(value any) driver.Sqler {
	return expr{string(driver.Placeholder), []any{value}}
}

func (e expr) Sql(_ *driver.SqlOptions) (string, []any, error) {
	return e.sqlText, e.args, nil
}
