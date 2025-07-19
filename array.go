package op

import (
	"github.com/xsqrty/op/driver"
)

// array represents a collection of elements of any type used for SQL generation and manipulation.
type array []any

// Array creates a PostgreSQL array from the provided arguments and generates its SQL string representation.
func Array(args ...any) driver.Sqler {
	return array(args)
}

// ArrayLength generates a SQL `ARRAY_LENGTH` function call for the given array argument with a default dimension of 1.
func ArrayLength(arg any) driver.Sqler {
	return manyArgsColumn("ARRAY_LENGTH", []any{arg, 1})
}

// ArrayConcat concatenates two SQL arrays into a single array using the ARRAY_CAT function.
func ArrayConcat(arg1, arg2 any) driver.Sqler {
	return manyArgsColumn("ARRAY_CAT", []any{arg1, arg2})
}

// ArrayUnnest returns a SQL representation of the UNNEST function applied to the given argument.
func ArrayUnnest(arg any) driver.Sqler {
	return oneArgColumn("UNNEST", arg)
}

// Sql generates the SQL representation of the array along with any required arguments and potential errors.
func (a array) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := list(a).Sql(options)
	if err != nil {
		return "", nil, err
	}

	return "ARRAY[" + sql + "]", args, nil
}
