package op

import (
	"strings"

	"github.com/xsqrty/op/driver"
)

// Cases represent a conditional SQL case structure, supporting multiple conditional branches and a final else clause.
type Cases interface {
	// ElseIf adds a new condition and corresponding outcome to the case structure.
	ElseIf(condition driver.Sqler, then driver.Sqler) Cases
	// Else sets the final fallback outcome for the case structure if no other conditions match.
	Else(elseCase driver.Sqler) driver.Sqler
}

// caseItem represents a condition and its corresponding result in a SQL CASE expression.
type caseItem struct {
	condition driver.Sqler
	then      driver.Sqler
}

// cases define a slice of caseItem, representing conditional branches for CASE SQL expressions.
type cases []caseItem

// caseWhen represents a SQL CASE WHEN structure with multiple cases and an optional ELSE clause.
type caseWhen struct {
	cases    cases
	elseCase driver.Sqler
}

// If creates a Cases instance with an initial condition and its corresponding result for SQL generation.
func If(condition driver.Sqler, then driver.Sqler) Cases {
	return cases{caseItem{condition, then}}
}

// ElseIf appends a conditional case with a corresponding result (then) to the existing cases sequence.
func (cs cases) ElseIf(condition driver.Sqler, then driver.Sqler) Cases {
	return append(cs, caseItem{condition: condition, then: then})
}

// Else appends an ELSE clause to the CASE statement with the provided driver.Sqler as the default expression.
func (cs cases) Else(elseCase driver.Sqler) driver.Sqler {
	return &caseWhen{
		cases:    cs,
		elseCase: elseCase,
	}
}

// Sql generates a SQL 'CASE' statement string, collects associated arguments, and handles errors during generation.
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
