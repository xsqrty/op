package orm

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
)

// Paginator defines methods for handling SQL query pagination with advanced configurations and result processing.
type Paginator[T any] interface {
	// WhiteList restricts the selectable fields in the query to the specified whitelist of field names.
	WhiteList(whitelist ...string) Paginator[T]
	// Fields sets the SQL fields or expressions to be selected in the query with optional aliases for the result set.
	Fields(fields ...op.Alias) Paginator[T]
	// MaxFilterDepth sets the maximum pagination depth for the filter and returns a Paginator with the specified depth.
	MaxFilterDepth(depth uint64) Paginator[T]
	// MaxSliceLen sets the maximum number of items allowed in the slice for the paginator and returns the updated paginator.
	MaxSliceLen(maxLen uint64) Paginator[T]
	// MaxLimit sets the maximum number of items per page for the pagination. Returns the updated Paginator instance.
	MaxLimit(limit uint64) Paginator[T]
	// MinLimit sets the minimum limit for pagination and ensures it does not exceed the specified value. Returns the updated Paginator.
	MinLimit(limit uint64) Paginator[T]
	// Where applies a filtering condition to the query based on the provided Sqler expression and returns an updated paginator.
	Where(exp driver.Sqler) Paginator[T]
	// Join adds an SQL JOIN clause to the query with the specified table and join condition.
	Join(table any, on driver.Sqler) Paginator[T]
	// LeftJoin adds a LEFT JOIN clause to the query using the specified table and join condition.
	LeftJoin(table any, on driver.Sqler) Paginator[T]
	// RightJoin adds a RIGHT JOIN clause to the SQL query with the specified table and join condition.
	RightJoin(table any, on driver.Sqler) Paginator[T]
	// InnerJoin adds an INNER JOIN clause to the query using the specified table and condition.
	InnerJoin(table any, on driver.Sqler) Paginator[T]
	// CrossJoin adds a CROSS JOIN clause to the query with the specified table and ON condition, returning the Paginator instance.
	CrossJoin(table any, on driver.Sqler) Paginator[T]
	// GroupBy groups the items in the pagination based on the specified grouping criteria provided as arguments.
	GroupBy(groups ...any) Paginator[T]
	// LogQuery executes a query with logging by utilizing the provided LoggerHandler and returns a paginated result.
	LogQuery(handler LoggerHandler) Paginator[T]
	// LogCounter returns a Paginator instance of type T that logs information through the provided LoggerHandler.
	LogCounter(handler LoggerHandler) Paginator[T]
	// With executes the paginated query using the provided context and database, returning the results or an error.
	With(ctx context.Context, db Queryable) (*PaginateResult[T], error)
}

// PaginateResult represents the result of a paginated query containing the total row count and the fetched rows.
type PaginateResult[T any] struct {
	TotalRows uint64
	Rows      []*T
}

// PaginateRequest defines pagination parameters including ordering, filters, limit, and offset for data retrieval.
type PaginateRequest struct {
	Orders  []PaginateOrder `json:"orders,omitempty"`
	Filters PaginateFilters `json:"filters,omitempty"`
	Limit   uint64          `json:"limit,omitempty"`
	Offset  uint64          `json:"offset,omitempty"`
}

// PaginateFilters represents a nested collection of key-value pairs to define filter conditions for pagination queries.
type PaginateFilters map[string]any

// PaginateOrder represents an ordering rule for paginated queries, including the key to order by and the sorting direction.
type PaginateOrder struct {
	Key  string `json:"key"`
	Desc bool   `json:"desc"`
}

// paginate is a generic struct for managing database pagination with configurable options and SQL building capabilities.
type paginate[T any] struct {
	whitelist     []string
	fields        []op.Alias
	request       *PaginateRequest
	rowsSb        op.SelectBuilder
	rowsSbWrap    op.SelectBuilder
	countSbWrap   op.SelectBuilder
	loggerQuery   LoggerHandler
	loggerCounter LoggerHandler
	minLimit      uint64
	maxLimit      uint64
	maxDepth      uint64
	maxSliceLen   uint64
}

// Constants representing query operators for pagination filters.
const (
	// PaginateOr represents the logical OR operator.
	PaginateOr = "$or"
	// PaginateAnd represents the logical AND operator.
	PaginateAnd = "$and"
	// PaginateIn represents the inclusion operator.
	PaginateIn = "$in"
	// PaginateNotIn represents the exclusion operator.
	PaginateNotIn = "$nin"
	// PaginateEq represents the equality operator.
	PaginateEq = "$eq"
	// PaginateNe represents the not equal operator.
	PaginateNe = "$ne"
	// PaginateLt represents the less than operator.
	PaginateLt = "$lt"
	// PaginateGt represents the greater than operator.
	PaginateGt = "$gt"
	// PaginateLte represents the less than or equal operator.
	PaginateLte = "$lte"
	// PaginateGte represents the greater than or equal operator.
	PaginateGte = "$gte"
	// PaginateLike represents the LIKE operator for pattern matching.
	PaginateLike = "$like"
	// PaginateLeftLike represents the LEFT LIKE operator for patterns starting with a wildcard.
	PaginateLeftLike = "$llike"
	// PaginateRightLike represents the RIGHT LIKE operator for patterns ending with a wildcard.
	PaginateRightLike = "$rlike"
)

var (
	ErrFilterSliceExceeded = errors.New("filter array length exceeded")
	ErrFilterDepthExceeded = errors.New("filter depth exceeded")
	ErrDisallowedKey       = errors.New("disallowed key")
	ErrFilterInvalid       = errors.New("filter invalid")
)

const (
	// defaultMinLimit defines the default minimum limit as 1.
	defaultMinLimit = uint64(1)
	// defaultMaxLimit defines the default maximum limit as the maximum unsigned 64-bit integer value.
	defaultMaxLimit = math.MaxUint64
	// defaultFilterDepth specifies the default depth for filters, set to 5.
	defaultFilterDepth = uint64(5)
	// defaultMaxSliceLen specifies the default maximum length for slices, set to 10.
	defaultMaxSliceLen = uint64(10)
)

// Paginate creates a Paginator instance for querying and paginating rows from the specified SQL table.
func Paginate[T any](table string, request *PaginateRequest) Paginator[T] {
	return &paginate[T]{
		request:     request,
		rowsSb:      op.Select().From(table),
		rowsSbWrap:  op.Select(),
		countSbWrap: op.Select(op.As(totalCountColumn, op.Count(driver.Pure("*")))),
		maxLimit:    defaultMaxLimit,
		minLimit:    defaultMinLimit,
		maxDepth:    defaultFilterDepth,
		maxSliceLen: defaultMaxSliceLen,
	}
}

// With executes the pagination query with the provided context and database, returning the result or an error.
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

	pg.rowsSb.SetReturning(pg.fields)

	if len(where) > 0 {
		pg.rowsSbWrap.Where(where)
		pg.countSbWrap.Where(where)
	}

	pg.rowsSbWrap.OrderBy(orders...)
	pg.rowsSbWrap.Limit(limit)
	pg.rowsSbWrap.Offset(offset)

	rows, err := Query[T](
		pg.rowsSb,
	).Log(pg.loggerQuery).
		Wrap("result", pg.rowsSbWrap).
		GetMany(ctx, db)
	if err != nil {
		return nil, err
	}

	var totalCount uint64
	sql, args, err := driver.Sql(pg.countSbWrap.From(op.As("result", pg.rowsSb)), db.SqlOptions())
	if pg.loggerCounter != nil {
		pg.loggerCounter(sql, args, err)
	}
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

// WhiteList sets list of allowed keys for filtering or ordering and returns the updated Paginator instance.
func (pg *paginate[T]) WhiteList(whitelist ...string) Paginator[T] {
	pg.whitelist = whitelist
	return pg
}

// Fields sets the selected fields for the pagination and returns the updated Paginator instance.
func (pg *paginate[T]) Fields(fields ...op.Alias) Paginator[T] {
	pg.fields = fields
	return pg
}

// MaxFilterDepth sets the maximum allowed depth for nested filters in a pagination query and returns the updated paginator.
func (pg *paginate[T]) MaxFilterDepth(depth uint64) Paginator[T] {
	pg.maxDepth = depth
	return pg
}

// MaxSliceLen sets the maximum allowed length for filter slice values and returns the updated Paginator instance.
func (pg *paginate[T]) MaxSliceLen(maxLen uint64) Paginator[T] {
	pg.maxSliceLen = maxLen
	return pg
}

// MaxLimit sets the maximum allowable limit for pagination and updates the paginator instance.
func (pg *paginate[T]) MaxLimit(limit uint64) Paginator[T] {
	pg.maxLimit = limit
	return pg
}

// MinLimit sets the minimum limit for paginated query results and returns the Paginator instance.
func (pg *paginate[T]) MinLimit(limit uint64) Paginator[T] {
	pg.minLimit = limit
	return pg
}

// Where adds a condition to the SQL query using the provided Sqler expression and returns the updated Paginator.
func (pg *paginate[T]) Where(exp driver.Sqler) Paginator[T] {
	pg.rowsSb.Where(exp)
	return pg
}

// Join adds a JOIN clause to the query using the specified table and ON condition, returning the updated Paginator instance.
func (pg *paginate[T]) Join(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.Join(table, on)
	return pg
}

// LeftJoin adds a LEFT JOIN clause to the query using the specified table and join condition.
// It updates the SelectBuilder instance and returns the current paginator instance.
func (pg *paginate[T]) LeftJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.LeftJoin(table, on)
	return pg
}

// RightJoin adds a RIGHT JOIN clause to the query using the specified table and ON condition.
func (pg *paginate[T]) RightJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.RightJoin(table, on)
	return pg
}

// InnerJoin adds an INNER JOIN clause to the query with the specified table and condition, returning the modified Paginator.
func (pg *paginate[T]) InnerJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.InnerJoin(table, on)
	return pg
}

// CrossJoin adds a CROSS JOIN clause to the query with the specified table and ON condition.
// It returns the paginator instance for chaining other query modifications.
func (pg *paginate[T]) CrossJoin(table any, on driver.Sqler) Paginator[T] {
	pg.rowsSb.CrossJoin(table, on)
	return pg
}

// GroupBy sets the GROUP BY clause for the query using the provided grouping columns. It returns the updated Paginator.
func (pg *paginate[T]) GroupBy(groups ...any) Paginator[T] {
	pg.rowsSb.GroupBy(groups...)
	return pg
}

// LogQuery sets a LoggerHandler to log SQL query strings, their arguments, and any errors encountered during execution.
func (pg *paginate[T]) LogQuery(lh LoggerHandler) Paginator[T] {
	pg.loggerQuery = lh
	return pg
}

// LogCounter sets a logger function to handle SQL counting queries and associated data or errors during pagination.
func (pg *paginate[T]) LogCounter(lh LoggerHandler) Paginator[T] {
	pg.loggerCounter = lh
	return pg
}

// parseOrders validates and converts a slice of PaginateOrder into a slice of op.Order based on allowed keys and sort direction.
func (pg *paginate[T]) parseOrders(orders []PaginateOrder) ([]op.Order, error) {
	result := make([]op.Order, 0, len(orders))
	for i := range orders {
		if !pg.isAllowedKey(orders[i].Key) {
			return nil, fmt.Errorf("paginate: target %q is not allowed", orders[i].Key)
		}

		if orders[i].Desc {
			result = append(result, op.Desc(orders[i].Key))
		} else {
			result = append(result, op.Asc(orders[i].Key))
		}
	}

	return result, nil
}

// isAllowedKey checks if the given key exists in the whitelist, returning true if it is allowed or false otherwise.
func (pg *paginate[T]) isAllowedKey(key string) bool {
	for _, k := range pg.whitelist {
		if k == key {
			return true
		}
	}

	return false
}

// parseFilters processes nested filter conditions into a logical AND expression while enforcing depth and key restrictions.
// It validates the input structure, ensures allowed keys are used, and prevents exceeding maximum recursion depth or slice length.
func (pg *paginate[T]) parseFilters(filters PaginateFilters, depth uint64) (op.And, error) {
	if filters == nil {
		return nil, nil
	}

	if depth > pg.maxDepth {
		return nil, fmt.Errorf("paginate: %w, max depth %d", ErrFilterDepthExceeded, pg.maxDepth)
	}

	var and op.And
	for k, v := range filters {
		if k == PaginateAnd || k == PaginateOr {
			list, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf(
					"paginate: group %q must be an array: %w",
					k,
					ErrFilterInvalid,
				)
			}

			if uint64(len(list)) > pg.maxSliceLen {
				return nil, fmt.Errorf(
					"paginate: group %q value is too long: %w",
					k,
					ErrFilterSliceExceeded,
				)
			}

			var group op.And
			for i, item := range list {
				itemMap, ok := item.(map[string]any)
				if !ok {
					return nil, fmt.Errorf(
						"paginate: group %q[%d] is invalid: %w",
						k,
						i,
						ErrFilterInvalid,
					)
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

			switch k {
			case PaginateAnd:
				and = appendToAndGroup[op.And](and, group)
			default:
				and = appendToAndGroup[op.Or](and, op.Or(group))
			}
		} else if isPrimitiveValue(v) {
			if !pg.isAllowedKey(k) {
				return nil, fmt.Errorf("paginate: filter key %q is not allowed: %w", k, ErrDisallowedKey)
			}

			and = append(and, op.Eq(k, v))
		} else if group, ok := v.(map[string]any); ok {
			if !pg.isAllowedKey(k) {
				return nil, fmt.Errorf("paginate: filter key %q is not allowed: %w", k, ErrDisallowedKey)
			}

			var groupAnd op.And
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

// checkOperatorValue validates the operator and value combination for a filter, returning an error if invalid.
func (pg *paginate[T]) checkOperatorValue(operator, key string, value any) error {
	if operator == PaginateIn || operator == PaginateNotIn {
		if s, ok := value.([]any); ok {
			if uint64(len(s)) > pg.maxSliceLen {
				return fmt.Errorf(
					"paginate: %q operator %q value is too long: %w",
					key,
					operator,
					ErrFilterSliceExceeded,
				)
			}

			if isPrimitiveSlice(s) {
				return nil
			}
		}

		return fmt.Errorf(
			"paginate: invalid value: %q operator %q: %w",
			key,
			operator,
			ErrFilterInvalid,
		)
	}

	if isPrimitiveValue(value) {
		return nil
	}

	return fmt.Errorf(
		"paginate: invalid value: %q operator %q: %w",
		key,
		operator,
		ErrFilterInvalid,
	)
}

// getFilterOperator maps a filter operator to the corresponding SQL condition based on the key and value provided.
// Returns a driver.Sqler instance representing the SQL expression or an error if the operator or value is invalid.
func getFilterOperator(operator, key string, value any) (driver.Sqler, error) {
	switch operator {
	case PaginateEq:
		return op.Eq(key, value), nil
	case PaginateNe:
		return op.Ne(key, value), nil
	case PaginateLt:
		return op.Lt(key, value), nil
	case PaginateGt:
		return op.Gt(key, value), nil
	case PaginateLte:
		return op.Lte(key, value), nil
	case PaginateGte:
		return op.Gte(key, value), nil
	case PaginateIn, PaginateNotIn:
		if v, ok := value.([]any); ok {
			if operator == PaginateIn {
				return op.In(key, v...), nil
			} else {
				return op.Nin(key, v...), nil
			}
		}

		return nil, fmt.Errorf(
			"paginate: invalid value for %q operator: %w",
			operator,
			ErrFilterInvalid,
		)
	case PaginateLike:
		return op.Like(op.Upper(key), "%"+fmt.Sprint(value)+"%"), nil
	case PaginateLeftLike:
		return op.Like(op.Upper(key), fmt.Sprint(value)+"%"), nil
	case PaginateRightLike:
		return op.Like(op.Upper(key), "%"+fmt.Sprint(value)), nil
	}

	return nil, fmt.Errorf("invalid filter operator: %s", operator)
}

// appendToAndGroup appends a logical group (AND/OR) to an existing AND collection if the group is not empty.
// Returns the updated AND collection.
func appendToAndGroup[T interface{ op.Or | op.And }](and op.And, group T) op.And {
	if len(group) == 0 {
		return and
	} else if len(group) > 1 {
		return append(and, driver.Sqler(group))
	}

	return append(and, group[0])
}

// isPrimitiveValue checks if a given value is a primitive type (bool, string, or float64) or nil, returning true if it is.
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

// isPrimitiveSlice checks if all elements in the provided slice are primitive types (bool, string, or float64) or nil.
func isPrimitiveSlice(s []any) bool {
	for _, item := range s {
		if !isPrimitiveValue(item) {
			return false
		}
	}

	return true
}
