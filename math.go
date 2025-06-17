package op

import (
	"fmt"
	"github.com/xsqrty/op/driver"
)

type mathOperator struct {
	operator byte
	args     []any
}

func Add(args ...any) driver.Sqler {
	return &mathOperator{operator: '+', args: args}
}

func Sub(args ...any) driver.Sqler {
	return &mathOperator{operator: '-', args: args}
}

func Div(args ...any) driver.Sqler {
	return &mathOperator{operator: '/', args: args}
}

func Mul(args ...any) driver.Sqler {
	return &mathOperator{operator: '*', args: args}
}

func (m *mathOperator) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := joinList(m.operator, m.args, true, options)
	return fmt.Sprintf("(%s)", sql), args, err
}
