package op

import (
	"github.com/xsqrty/op/driver"
)

type array []any

func Array(args ...any) driver.Sqler {
	return array(args)
}

func ArrayLength(arg any) driver.Sqler {
	return manyArgsColumn("ARRAY_LENGTH", []any{arg, 1})
}

func ArrayConcat(arg1, arg2 any) driver.Sqler {
	return manyArgsColumn("ARRAY_CAT", []any{arg1, arg2})
}

func ArrayUnnest(arg any) driver.Sqler {
	return oneArgColumn("UNNEST", arg)
}

func (a array) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := list(a).Sql(options)
	if err != nil {
		return "", nil, err
	}

	return "ARRAY[" + sql + "]", args, nil
}
