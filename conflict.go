package op

import "github.com/xsqrty/op/driver"

type conflict struct {
	target alias
	expr   driver.Sqler
}

type Excluded Column

func DoNothing() driver.Sqler {
	return Pure("NOTHING")
}

func DoUpdate(updates Updates) *UpdateBuilder {
	return Update(nil, updates)
}

func (ex Excluded) Sql(options *driver.SqlOptions) (string, []interface{}, error) {
	sql, args, err := Column(ex).Sql(options)
	if err != nil {
		return "", nil, err
	}

	return "EXCLUDED." + sql, args, nil
}
