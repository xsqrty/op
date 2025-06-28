package orm

type LoggerHandler func(sql string, args []any, err error)
