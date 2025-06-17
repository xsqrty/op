package driver

type expr struct {
	sqlText string
	args    []any
}

func Pure(sqlValue string, args ...any) Sqler {
	return &expr{sqlValue, args}
}

func Value(value any) Sqler {
	return &expr{string(Placeholder), []any{value}}
}

func (e *expr) Sql(_ *SqlOptions) (string, []any, error) {
	return e.sqlText, e.args, nil
}
