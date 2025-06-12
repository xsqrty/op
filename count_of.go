package op

import (
	"context"
	"github.com/xsqrty/op/driver"
)

const totalCountColumn = "total_count"

type countOfResult struct {
	Count uint64 `op:"total_count,aggregated"`
}

type countOf struct {
	sb         *SelectBuilder
	table      string
	where      And
	byColumn   *Column
	byDistinct bool
}

func CountOf(table string) *countOf {
	return &countOf{
		table: table,
		sb:    &SelectBuilder{},
	}
}

func (co *countOf) By(key string) *countOf {
	col := Column(key)
	co.byDistinct = false
	co.byColumn = &col

	return co
}

func (co *countOf) ByDistinct(key string) *countOf {
	col := Column(key)
	co.byDistinct = true
	co.byColumn = &col

	return co
}

func (co *countOf) Where(exp driver.Sqler) *countOf {
	if exp != nil {
		co.where = append(co.where, append(And{}, exp))
	}

	return co
}

func (co *countOf) With(ctx context.Context, db Queryable) (uint64, error) {
	co.sb.From(co.table)
	if len(co.where) > 0 {
		co.sb.Where(co.where)
	}

	if co.byColumn != nil && co.byDistinct {
		co.sb.SetReturningAliases([]alias{As(totalCountColumn, CountDistinct(co.byColumn))})
	} else if co.byColumn != nil {
		co.sb.SetReturningAliases([]alias{As(totalCountColumn, Count(co.byColumn))})
	} else {
		co.sb.SetReturningAliases([]alias{As(totalCountColumn, Count(Pure("*")))})
	}

	result, err := Query[countOfResult](co.sb).GetOne(ctx, db)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
}
