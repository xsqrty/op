package op

import (
	"context"
	"errors"
	"fmt"
	"github.com/xsqrty/op/driver"
	"math"
)

type Paginator[T any] interface {
	With(ctx context.Context, db Queryable) (*PaginateResult[T], error)
	Fields(fields ...any) Paginator[T]
	MaxFilterDepth(depth int64) Paginator[T]
	MaxLimit(limit int64) Paginator[T]
	MinLimit(limit int64) Paginator[T]
	Having(exp driver.Sqler) Paginator[T]
	Where(exp driver.Sqler) Paginator[T]
	Join(table any, on ...driver.Sqler) Paginator[T]
	LeftJoin(table any, on ...driver.Sqler) Paginator[T]
	RightJoin(table any, on ...driver.Sqler) Paginator[T]
	InnerJoin(table any, on ...driver.Sqler) Paginator[T]
	CrossJoin(table any, on ...driver.Sqler) Paginator[T]
	GroupBy(groups ...any) Paginator[T]
}

type PaginateResult[T any] struct {
	TotalRows int64
	Rows      []*T
}

type PaginateRequest struct {
	Orders  []PaginateOrder `json:"orders,omitempty"`
	Filters *FilterGroup    `json:"filters,omitempty"`
	Limit   int64           `json:"limit,omitempty"`
	Offset  int64           `json:"offset,omitempty"`
}

type FilterGroup struct {
	Group   string        `json:"group,omitempty"`
	Filters []Filter      `json:"filters,omitempty"`
	Groups  []FilterGroup `json:"groups,omitempty"`
}

type Filter struct {
	Operator string      `json:"op"`
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
}

type PaginateOrder struct {
	Key  string `json:"key"`
	Desc bool   `json:"desc"`
}

type paginate[T any] struct {
	fieldsAllowed []string
	request       *PaginateRequest
	rowsSb        SelectBuilder
	countSb       SelectBuilder
	countSbWrap   SelectBuilder
	minLimit      int64
	maxLimit      int64
	maxDepth      int64
}

var (
	ErrFilterDepthExceeded = errors.New("filter depth exceeded")
)

const (
	defaultMinLimit    = 1
	defaultMaxLimit    = math.MaxInt64
	defaultFilterDepth = 5
)

func Paginate[T any](table string, request *PaginateRequest, fieldsAllowed []string) Paginator[T] {
	return &paginate[T]{
		request:       request,
		rowsSb:        Select().From(table),
		countSb:       Select().From(table),
		countSbWrap:   Select(As(totalCountColumn, Count(driver.Pure("*")))),
		fieldsAllowed: fieldsAllowed,
		maxLimit:      defaultMaxLimit,
		minLimit:      defaultMinLimit,
		maxDepth:      defaultFilterDepth,
	}
}

func (pg *paginate[T]) With(ctx context.Context, db Queryable) (*PaginateResult[T], error) {
	limit := pg.request.Limit
	offset := pg.request.Offset

	if pg.request.Limit > pg.maxLimit {
		limit = pg.maxLimit
	} else if pg.request.Limit < pg.minLimit {
		limit = pg.minLimit
	}

	where, err := pg.parseFilters(pg.request.Filters, 0)
	if err != nil {
		return nil, err
	}

	orders, err := pg.parseOrders(pg.request.Orders)
	if err != nil {
		return nil, err
	}

	pg.rowsSb.Where(where)
	pg.countSb.Where(where)

	pg.rowsSb.OrderBy(orders...)
	pg.rowsSb.Limit(limit)
	pg.rowsSb.Offset(offset)

	rows, err := Query[T](pg.rowsSb).GetMany(ctx, db)
	if err != nil {
		return nil, err
	}

	var totalCount int64
	sql, args, err := db.Sql(pg.countSbWrap.From(As("result", pg.countSb)))
	if err != nil {
		return nil, err
	}

	err = db.QueryRow(ctx, sql, args...).Scan(&totalCount)
	if err != nil {
		return nil, err
	}

	return &PaginateResult[T]{
		Rows:      rows,
		TotalRows: totalCount,
	}, nil
}

func (pg *paginate[T]) Fields(fields ...any) Paginator[T] {
	pg.rowsSb.SetReturning(fields)
	pg.countSb.SetReturning(fields)

	return pg
}

func (pg *paginate[T]) MaxFilterDepth(depth int64) Paginator[T] {
	pg.maxDepth = depth
	return pg
}

func (pg *paginate[T]) MaxLimit(limit int64) Paginator[T] {
	pg.maxLimit = limit
	return pg
}

func (pg *paginate[T]) MinLimit(limit int64) Paginator[T] {
	pg.minLimit = limit
	return pg
}

func (pg *paginate[T]) Having(exp driver.Sqler) Paginator[T] {
	pg.rowsSb.Having(exp)
	pg.countSb.Having(exp)

	return pg
}

func (pg *paginate[T]) Where(exp driver.Sqler) Paginator[T] {
	pg.rowsSb.Where(exp)
	pg.countSb.Where(exp)

	return pg
}

func (pg *paginate[T]) Join(table any, on ...driver.Sqler) Paginator[T] {
	pg.rowsSb.Join(table, on...)
	pg.countSb.Join(table, on...)

	return pg
}

func (pg *paginate[T]) LeftJoin(table any, on ...driver.Sqler) Paginator[T] {
	pg.rowsSb.LeftJoin(table, on...)
	pg.countSb.LeftJoin(table, on...)

	return pg
}

func (pg *paginate[T]) RightJoin(table any, on ...driver.Sqler) Paginator[T] {
	pg.rowsSb.RightJoin(table, on...)
	pg.countSb.RightJoin(table, on...)

	return pg
}

func (pg *paginate[T]) InnerJoin(table any, on ...driver.Sqler) Paginator[T] {
	pg.rowsSb.InnerJoin(table, on...)
	pg.countSb.InnerJoin(table, on...)

	return pg
}

func (pg *paginate[T]) CrossJoin(table any, on ...driver.Sqler) Paginator[T] {
	pg.rowsSb.CrossJoin(table, on...)
	pg.countSb.CrossJoin(table, on...)

	return pg
}

func (pg *paginate[T]) GroupBy(groups ...any) Paginator[T] {
	pg.rowsSb.GroupBy(groups...)
	pg.countSb.GroupBy(groups...)
	
	return pg
}

func (pg *paginate[T]) parseFilters(filters *FilterGroup, depth int64) (driver.Sqler, error) {
	if filters == nil {
		return nil, nil
	}

	if depth >= pg.maxDepth {
		return nil, fmt.Errorf("paginate: %w, max depth %d", ErrFilterDepthExceeded, pg.maxDepth)
	}

	var result []driver.Sqler
	for i := range filters.Filters {
		if !pg.isFieldAllowed(filters.Filters[i].Key) {
			return nil, fmt.Errorf("paginate: target %q is not allowed", filters.Filters[i].Key)
		}

		operator, err := getFilterOperator(&filters.Filters[i])
		if err != nil {
			return nil, err
		}

		result = append(result, operator)
	}

	for i := range filters.Groups {
		computedGroup, err := pg.parseFilters(&filters.Groups[i], depth+1)
		if err != nil {
			return nil, err
		}

		if computedGroup != nil {
			result = append(result, computedGroup)
		}
	}

	if len(result) == 0 {
		return nil, nil
	}

	if filters.Group == "or" {
		return Or(result), nil
	}

	return And(result), nil
}

func (pg *paginate[T]) parseOrders(orders []PaginateOrder) ([]Order, error) {
	result := make([]Order, 0, len(orders))
	for i := range orders {
		if !pg.isFieldAllowed(orders[i].Key) {
			return nil, fmt.Errorf("paginate: target %q is not allowed", orders[i].Key)
		}

		if orders[i].Desc {
			result = append(result, Desc(orders[i].Key))
		} else {
			result = append(result, Asc(orders[i].Key))
		}
	}

	return result, nil
}

func (pg *paginate[T]) isFieldAllowed(key string) bool {
	for _, k := range pg.fieldsAllowed {
		if k == key {
			return true
		}
	}

	return false
}

func getFilterOperator(filter *Filter) (driver.Sqler, error) {
	switch filter.Operator {
	case "eq":
		return Eq(filter.Key, filter.Value), nil
	case "ne":
		return Ne(filter.Key, filter.Value), nil
	case "lt":
		return Lt(filter.Key, filter.Value), nil
	case "gt":
		return Gt(filter.Key, filter.Value), nil
	case "lte":
		return Lte(filter.Key, filter.Value), nil
	case "gte":
		return Gte(filter.Key, filter.Value), nil
	case "in":
		if v, ok := filter.Value.([]any); ok {
			return In(filter.Key, v...), nil
		}

		return nil, fmt.Errorf("invalid value for %q operator", filter.Operator)
	case "like":
		return ILike(filter.Key, "%"+fmt.Sprint(filter.Value)+"%"), nil
	case "leftLike":
		return ILike(filter.Key, fmt.Sprint(filter.Value)+"%"), nil
	case "rightLight":
		return ILike(filter.Key, "%"+fmt.Sprint(filter.Value)), nil
	}

	return nil, fmt.Errorf("invalid filter operator: %s", filter.Operator)
}
