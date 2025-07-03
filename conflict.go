package op

import "github.com/xsqrty/op/driver"

type conflict struct {
	target Alias
	expr   driver.Sqler
}

type Excluded Column

func DoNothing() driver.Sqler {
	return driver.Pure("NOTHING")
}

func DoUpdate(updates Updates) UpdateBuilder {
	return Update(nil, updates)
}

func (ex Excluded) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := Column(ex).Sql(options)
	if err != nil {
		return "", nil, err
	}

	return "EXCLUDED." + sql, args, nil
}
