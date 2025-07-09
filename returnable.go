package op

import "github.com/xsqrty/op/driver"

type CounterType uint8

const (
	CounterQuery CounterType = iota
	CounterExec
)

type Returnable interface {
	UsingTables() []string
	With() string
	GetReturning() []Alias
	SetReturning([]Alias)
	Sql(options *driver.SqlOptions) (string, []any, error)
	PreparedSql(options *driver.SqlOptions) (string, []any, error)
	LimitReturningOne()
	CounterType() CounterType
}
