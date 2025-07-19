package op

import (
	"github.com/xsqrty/op/driver"
)

// mathOperator represents a mathematical operation with an operator and a list of arguments.
type mathOperator struct {
	operator byte
	args     []any
}

// Add creates a SQL-compatible addition expression with the provided arguments. Returns a driver.Sqler implementation.
func Add(args ...any) driver.Sqler {
	return &mathOperator{operator: '+', args: args}
}

// Sub creates a subtraction SQL expression from the provided arguments.
// It returns an implementation of driver.Sqler to generate the corresponding SQL string.
func Sub(args ...any) driver.Sqler {
	return &mathOperator{operator: '-', args: args}
}

// Div creates a SQL-compatible division expression with the provided arguments.
// It returns a Sqler to generate the corresponding SQL string and arguments.
func Div(args ...any) driver.Sqler {
	return &mathOperator{operator: '/', args: args}
}

// Mul creates a SQL multiplication expression by joining the provided arguments with the `*` operator.
func Mul(args ...any) driver.Sqler {
	return &mathOperator{operator: '*', args: args}
}

// Sql generates a SQL string with placeholders and arguments using the given SqlOptions. Returns the formatted SQL, arguments, and any error.
func (m *mathOperator) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := joinList(m.operator, m.args, true, options)
	return "(" + sql + ")", args, err
}
