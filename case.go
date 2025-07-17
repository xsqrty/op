package op

import (
	"strings"

	"github.com/xsqrty/op/driver"
)

type Cases interface {
	ElseIf(condition driver.Sqler, then driver.Sqler) Cases
	Else(elseCase driver.Sqler) driver.Sqler
}

type caseItem struct {
	condition driver.Sqler
	then      driver.Sqler
}

type cases []caseItem

type caseWhen struct {
	cases    cases
	elseCase driver.Sqler
}

func If(condition driver.Sqler, then driver.Sqler) Cases {
	return cases{caseItem{condition, then}}
}

func (cs cases) ElseIf(condition driver.Sqler, then driver.Sqler) Cases {
	return append(cs, caseItem{condition: condition, then: then})
}

func (cs cases) Else(elseCase driver.Sqler) driver.Sqler {
	return &caseWhen{
		cases:    cs,
		elseCase: elseCase,
	}
}

func (cw *caseWhen) Sql(options *driver.SqlOptions) (string, []any, error) {
	var args []any
	var buf strings.Builder

	buf.WriteString("CASE")
	for _, item := range cw.cases {
		buf.WriteString(" WHEN ")
		sql, whenArgs, err := item.condition.Sql(options)
		if err != nil {
			return "", nil, err
		}

		buf.WriteString(sql)
		args = append(args, whenArgs...)
		buf.WriteString(" THEN ")

		sql, thenArgs, err := item.then.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, thenArgs...)
		buf.WriteString(sql)
	}

	buf.WriteString(" ELSE ")
	sql, elseArgs, err := cw.elseCase.Sql(options)
	if err != nil {
		return "", nil, err
	}

	args = append(args, elseArgs...)
	buf.WriteString(sql)
	buf.WriteString(" END")

	return buf.String(), args, nil
}
