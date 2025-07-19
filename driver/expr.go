package driver

// expr represents a SQL expression.
type expr struct {
	sqlText string
	args    []any
}

// Pure creates a Sqler expression with the given SQL string and arguments.
func Pure(sqlValue string, args ...any) Sqler {
	return &expr{sqlValue, args}
}

// Value wraps the provided value into a Sqler implementation.
func Value(value any) Sqler {
	return &expr{string(Placeholder), []any{value}}
}

// Sql generates the SQL query string.
func (e *expr) Sql(_ *SqlOptions) (string, []any, error) {
	return e.sqlText, e.args, nil
}
