package op

import "github.com/xsqrty/op/driver"

// CounterType represents a type used to differentiate execution counters, such as CounterQuery and CounterExec.
type CounterType uint8

const (
	// CounterQuery represents a query operation for the CounterType enum.
	CounterQuery CounterType = iota
	// CounterExec represents an execution operation for the CounterType enum.
	CounterExec
)

// Returnable defines a generic interface for SELECT, DELETE, INSERT, UPDATE statements
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
