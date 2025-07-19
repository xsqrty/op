package op

import "github.com/xsqrty/op/driver"

// conflict represents a definition for handling SQL "ON CONFLICT" clauses, containing the target and the action to perform.
type conflict struct {
	target Alias
	expr   driver.Sqler
}

// Excluded is a type alias for Column, representing a reference to the special SQL `EXCLUDED` table for upsert operations.
type Excluded Column

// DoNothing returns a driver.Sqler that generates the SQL expression "NOTHING".
func DoNothing() driver.Sqler {
	return driver.Pure("NOTHING")
}

// DoUpdate constructs an UpdateBuilder to define an SQL UPDATE statement using the provided update fields and values.
func DoUpdate(updates Updates) UpdateBuilder {
	return Update(nil, updates)
}

// Sql generates a SQL string prefixed with "EXCLUDED." using the given SqlOptions, returning the string, arguments, and error.
func (ex Excluded) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := Column(ex).Sql(options)
	if err != nil {
		return "", nil, err
	}

	return "EXCLUDED." + sql, args, nil
}
