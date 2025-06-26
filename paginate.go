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
	WhiteList(whitelist ...string) Paginator[T]
	Fields(fields ...Alias) Paginator[T]
	MaxFilterDepth(depth uint64) Paginator[T]
	MaxSliceLen(maxLen uint64) Paginator[T]
	MaxLimit(limit uint64) Paginator[T]
	MinLimit(limit uint64) Paginator[T]
	Where(exp driver.Sqler) Paginator[T]
	Join(table any, on driver.Sqler) Paginator[T]
	LeftJoin(table any, on driver.Sqler) Paginator[T]
	RightJoin(table any, on driver.Sqler) Paginator[T]
	InnerJoin(table any, on driver.Sqler) Paginator[T]
	CrossJoin(table any, on driver.Sqler) Paginator[T]
	GroupBy(groups ...any) Paginator[T]
}

type PaginateResult[T any] struct {
	TotalRows uint64
	Rows      []*T
}

type PaginateRequest struct {
	Orders  []PaginateOrder `json:"orders,omitempty"`
	Filters PaginateFilters `json:"filters,omitempty"`
	Limit   uint64          `json:"limit,omitempty"`
	Offset  uint64          `json:"offset,omitempty"`
}

type PaginateFilters map[string]any

type PaginateOrder struct {
	Key  string `json:"key"`
	Desc bool   `json:"desc"`
}

type paginate[T any] struct {
	whitelist   []string
	fields      []Alias
	request     *PaginateRequest
	rowsSb      SelectBuilder
	rowsSbWrap  SelectBuilder
	countSbWrap SelectBuilder
	minLimit    uint64
	maxLimit    uint64
	maxDepth    uint64
	maxSliceLen uint64
}

const (
	PaginateOr        = "$or"
	PaginateAnd       = "$and"
	PaginateIn        = "$in"
	PaginateNotIn     = "$nin"
	PaginateEq        = "$eq"
	PaginateNe        = "$ne"
	PaginateLt        = "$lt"
	PaginateGt        = "$gt"
	PaginateLte       = "$lte"
	PaginateGte       = "$gte"
	PaginateLike      = "$like"
	PaginateLeftLike  = "$llike"
	PaginateRightLike = "$rlike"
)

var (
	ErrFilterSliceExceeded = errors.New("filter array length exceeded")
	ErrFilterDepthExceeded = errors.New("filter depth exceeded")
	ErrDisallowedKey       = errors.New("disallowed key")
	ErrFilterInvalid       = errors.New("filter invalid")
)

const (
	defaultMinLimit    = uint64(1)
	defaultMaxLimit    = math.MaxUint64
	defaultFilterDepth = uint64(5)
	defaultMaxSliceLen = uint64(10)
)

func Paginate[T any](table string, request *PaginateRequest) Paginator[T] {
	return &paginate[T]{
		request:     request,
		rowsSb:      Select().From(table),
		rowsSbWrap:  Select(),
		countSbWrap: Select(As(totalCountColumn, Count(driver.Pure("*")))),
		maxLimit:    defaultMaxLimit,
		minLimit:    defaultMinLimit,
		maxDepth:    defaultFilterDepth,
		maxSliceLen: defaultMaxSliceLen,
	}
}

func (pg *paginate[T]) With(ctx context.Context, db Queryable) (*PaginateResult[T], error) {
	if len(pg.fields) == 0 {
		return nil, fmt.Errorf("fields is empty. Please specify returning by .Fields()")
	}

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

	pg.rowsSb.SetReturningAliases(pg.fields)

	if len(where) > 0 {
		pg.rowsSbWrap.Where(where)
		pg.countSbWrap.Where(where)
	}

	pg.rowsSbWrap.OrderBy(orders...)
	pg.rowsSbWrap.Limit(limit)
	pg.rowsSbWrap.Offset(offset)

	rows, err := Query[T](pg.rowsSb).Wrap("result", pg.rowsSbWrap).GetMany(ctx, db)
	if err != nil {
		return nil, err
	}

	var totalCount uint64
	sql, args, err := db.Sql(pg.countSbWrap.From(As("result", pg.rowsSb)))
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

func (pg *paginate[T]) WhiteList(whitelist ...string) Paginator[T] {
	pg.whitelist = whitelist
	return pg
}

func (pg *paginate[T]) Fields(fields ...Alias) Paginator[T] {
	pg.fields = fields
	return pg
}

func (pg *paginate[T]) MaxFilterDepth(depth uint64) Paginator[T] {
	pg.maxDepth = depth
	return pg
}

func (pg *paginate[T]) MaxSliceLen(maxLen uint64) Paginator[T] {
	pg.maxSliceLen = maxLen
	return pg
}

func (pg *paginate[T]) MaxLimit(limit uint64) Paginator[T] {
	pg.maxLimit = limit
	return pg
}

func (pg *paginate[T]) MinLimit(limit uint64) Paginator[T] {
	pg.minLimit = limit
	return pg
}

func (pg *paginate[T]) Where(exp driver.Sqler) Paginator[T] {
	pg.rowsSb.Where(exp)
	return pg
}

func (pg *paginate[T]) Join(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.Join(table, on)
	return pg
}

func (pg *paginate[T]) LeftJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.LeftJoin(table, on)
	return pg
}

func (pg *paginate[T]) RightJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.RightJoin(table, on)
	return pg
}

func (pg *paginate[T]) InnerJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.InnerJoin(table, on)
	return pg
}

func (pg *paginate[T]) CrossJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.CrossJoin(table, on)
	return pg
}

func (pg *paginate[T]) GroupBy(groups ...any) Paginator[T] {
	pg.rowsSb.GroupBy(groups...)
	return pg
}

func (pg *paginate[T]) parseOrders(orders []PaginateOrder) ([]Order, error) {
	result := make([]Order, 0, len(orders))
	for i := range orders {
		if !pg.isAllowedKey(orders[i].Key) {
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

func (pg *paginate[T]) isAllowedKey(key string) bool {
	for _, k := range pg.whitelist {
		if k == key {
			return true
		}
	}

	return false
}

func (pg *paginate[T]) parseFilters(filters PaginateFilters, depth uint64) (And, error) {
	if filters == nil {
		return nil, nil
	}

	if depth > pg.maxDepth {
		return nil, fmt.Errorf("paginate: %w, max depth %d", ErrFilterDepthExceeded, pg.maxDepth)
	}

	var and And
	for k, v := range filters {
		if k == PaginateAnd || k == PaginateOr {
			list, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("paginate: group %q must be an array: %w", k, ErrFilterInvalid)
			}

			if len(list) > int(pg.maxSliceLen) {
				return nil, fmt.Errorf("paginate: group %q value is too long: %w", k, ErrFilterSliceExceeded)
			}

			var group And
			for i, item := range list {
				itemMap, ok := item.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("paginate: group %q[%d] is invalid: %w", k, i, ErrFilterInvalid)
				}

				parsed, err := pg.parseFilters(itemMap, depth+1)
				if err != nil {
					return nil, err
				}

				group = appendToAndGroup(group, parsed)
			}

			if len(group) == 0 {
				continue
			}

			if k == PaginateAnd {
				and = appendToAndGroup[And](and, group)
			} else if k == PaginateOr {
				and = appendToAndGroup[Or](and, Or(group))
			}

		} else if isPrimitiveValue(v) {
			if !pg.isAllowedKey(k) {
				return nil, fmt.Errorf("paginate: filter key %q is not allowed: %w", k, ErrDisallowedKey)
			}

			and = append(and, Eq(k, v))
		} else if group, ok := v.(map[string]any); ok {
			if !pg.isAllowedKey(k) {
				return nil, fmt.Errorf("paginate: filter key %q is not allowed: %w", k, ErrDisallowedKey)
			}

			var groupAnd And
			for operator, val := range group {
				err := pg.checkOperatorValue(operator, k, val)
				if err != nil {
					return nil, err
				}

				sqler, err := getFilterOperator(operator, k, val)
				if err != nil {
					return nil, err
				}

				groupAnd = append(groupAnd, sqler)
			}

			and = append(and, groupAnd...)
		} else {
			return nil, fmt.Errorf("paginate: invalid value: %q: %w", k, ErrFilterInvalid)
		}
	}

	return and, nil
}

func (pg *paginate[T]) checkOperatorValue(operator, key string, value any) error {
	if operator == PaginateIn || operator == PaginateNotIn {
		if s, ok := value.([]any); ok {
			if len(s) > int(pg.maxSliceLen) {
				return fmt.Errorf("paginate: %q operator %q value is too long: %w", key, operator, ErrFilterSliceExceeded)
			}

			if isPrimitiveSlice(s) {
				return nil
			}
		}

		return fmt.Errorf("paginate: invalid value: %q operator %q: %w", key, operator, ErrFilterInvalid)
	}

	if isPrimitiveValue(value) {
		return nil
	}

	return fmt.Errorf("paginate: invalid value: %q operator %q: %w", key, operator, ErrFilterInvalid)
}

func getFilterOperator(operator, key string, value any) (driver.Sqler, error) {
	switch operator {
	case PaginateEq:
		return Eq(key, value), nil
	case PaginateNe:
		return Ne(key, value), nil
	case PaginateLt:
		return Lt(key, value), nil
	case PaginateGt:
		return Gt(key, value), nil
	case PaginateLte:
		return Lte(key, value), nil
	case PaginateGte:
		return Gte(key, value), nil
	case PaginateIn, PaginateNotIn:
		if v, ok := value.([]any); ok {
			if operator == PaginateIn {
				return In(key, v...), nil
			} else {
				return Nin(key, v...), nil
			}
		}

		return nil, fmt.Errorf("paginate: invalid value for %q operator: %w", operator, ErrFilterInvalid)
	case PaginateLike:
		return Like(Upper(key), "%"+fmt.Sprint(value)+"%"), nil
	case PaginateLeftLike:
		return Like(Upper(key), fmt.Sprint(value)+"%"), nil
	case PaginateRightLike:
		return Like(Upper(key), "%"+fmt.Sprint(value)), nil
	}

	return nil, fmt.Errorf("invalid filter operator: %s", operator)
}

func appendToAndGroup[T interface{ Or | And }](and And, group T) And {
	if len(group) == 0 {
		return and
	} else if len(group) > 1 {
		return append(and, driver.Sqler(group))
	}

	return append(and, group[0])
}

func isPrimitiveValue(val any) bool {
	if val == nil {
		return true
	}

	switch val.(type) {
	case bool, string, float64:
		return true
	}

	return false
}

func isPrimitiveSlice(s []any) bool {
	for _, item := range s {
		if !isPrimitiveValue(item) {
			return false
		}
	}

	return true
}
