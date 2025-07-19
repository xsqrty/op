package orm

// LoggerHandler defines a function type for logging SQL queries, their arguments, and any errors encountered.
type LoggerHandler func(sql string, args []any, err error)
