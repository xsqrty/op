package op

import (
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

// SelectBuilder provides an interface for building SQL SELECT queries with methods for setting clauses and options.
type SelectBuilder interface {
	// Distinct adds a DISTINCT clause to the query, optionally for the specified columns or expressions.
	Distinct(args ...string) SelectBuilder
	// All resets the SELECT clause of the query to include all columns (*) in the result set.
	All() SelectBuilder
	// From sets the FROM clause of the SQL query with the specified table or subquery.
	From(from any) SelectBuilder
	// Where adds a WHERE clause to the SQL query using the provided expression. Allows filtering based on conditions.
	Where(exp driver.Sqler) SelectBuilder
	// Having adds a HAVING clause to the SQL query with the specified condition expressed as a driver.Sqler.
	Having(exp driver.Sqler) SelectBuilder
	// Join adds a JOIN clause to the SQL query, specifying the table to join and the condition for the join.
	Join(table any, on driver.Sqler) SelectBuilder
	// LeftJoin adds a LEFT JOIN clause to the SQL query using the specified table and ON condition.
	LeftJoin(table any, on driver.Sqler) SelectBuilder
	// RightJoin adds a RIGHT JOIN clause to the query with the specified table and ON condition.
	RightJoin(table any, on driver.Sqler) SelectBuilder
	// InnerJoin adds an INNER JOIN clause to the query with the specified table and ON condition.
	InnerJoin(table any, on driver.Sqler) SelectBuilder
	// CrossJoin adds a CROSS JOIN clause to the query with the specified table and join condition.
	CrossJoin(table any, on driver.Sqler) SelectBuilder
	// Limit sets the maximum number of rows to return in the SQL SELECT statement. It accepts a positive integer value.
	Limit(limit uint64) SelectBuilder
	// Offset sets the OFFSET clause for the SQL query to skip a specified number of rows and returns the updated SelectBuilder.
	Offset(offset uint64) SelectBuilder
	// GroupBy adds a GROUP BY clause to the SQL query, allowing for aggregation based on the specified columns or expressions.
	GroupBy(groups ...any) SelectBuilder
	// OrderBy adds one or more ordering criteria to the SELECT query, specifying the sort order of the result set.
	OrderBy(orders ...Order) SelectBuilder
	// LimitReturningOne sets the SELECT statement to return only one row
	LimitReturningOne()
	// With returns the name of the table used in the current SELECT statement.
	With() string
	// UsingTables retrieves the list of table names referenced in the SQL query being constructed.
	UsingTables() []string
	// GetReturning returns a slice of Alias objects representing the columns or expressions marked for returning in a query.
	GetReturning() []Alias
	// SetReturning sets the list of fields to be returned in the SQL query, defined as an array of Alias objects.
	SetReturning(keys []Alias)
	// CounterType determines the type of counter used in the SQL query, such as CounterQuery or CounterExec.
	CounterType() CounterType
	// PreparedSql constructs a prepared SQL statement with placeholders and arguments based on the provided SqlOptions.
	PreparedSql(options *driver.SqlOptions) (sql string, args []any, err error)
	// Sql generates a SQL query string, its associated arguments, and an error if any, based on the provided SqlOptions.
	Sql(options *driver.SqlOptions) (sql string, args []any, err error)
}

// Order represents an interface for defining SQL ORDER BY clause functionality, including target, ordering type, and nulls handling.
type Order interface {
	// Target returns the target value or column used for ordering in the SQL ORDER BY clause.
	Target() any
	// OrderType returns the type of ordering applied, such as ascending (ASC) or descending (DESC), for the query or operation.
	OrderType() orderType
	// NullsType returns the nullsOrderType, indicating how NULL values are ordered in the sorting (e.g., NULLS FIRST or NULLS LAST).
	NullsType() nullsOrderType
	// Sql generates the SQL string for the ORDER BY clause, its arguments, and any error encountered during generation.
	Sql(options *driver.SqlOptions) (string, []any, error)
}

type (
	// orderType represents the type of ordering used in a query or operation.
	orderType int
	// joinType defines the type of join operation used in data processing or queries.
	joinType int
	// nullsOrderType specifies how NULL values are ordered in a sorting operation.
	nullsOrderType int
)

// order represents an SQL sorting configuration including column, order type, and nulls handling.
type order struct {
	target    any
	orderType orderType
	nullsType nullsOrderType
}

// join represents a SQL join operation with associated table, join type, and ON clause for join conditions.
type join struct {
	table    Alias
	joinType joinType
	on       driver.Sqler
}

// distinctOn represents an internal structure to handle DISTINCT ON clauses with specific columns in SQL queries.
type distinctOn struct {
	columns []Column
}

// orderDesc represents a descending order type.
// orderAsc represents an ascending order type.
// orderNone represents no specific order type.
const (
	orderDesc orderType = iota
	orderAsc
	orderNone
)

// nullsLast represents ordering with NULL values positioned last in the sorted result.
// nullsFirst represents ordering with NULL values positioned first in the sorted result.
// nullsNone represents no specific ordering for NULL values in the sorted result.
const (
	nullsLast nullsOrderType = iota
	nullsFirst
	nullsNone
)

// joinDefault represents the default join type.
// joinLeft represents a LEFT JOIN in SQL context.
// joinRight represents a RIGHT JOIN in SQL context.
// joinInner represents an INNER JOIN in SQL context.
// joinCross represents a CROSS JOIN in SQL context.
const (
	joinDefault joinType = iota
	joinLeft
	joinRight
	joinInner
	joinCross
)

// selectBuilder is a structure for constructing SQL SELECT queries programmatically.
// It includes fields for managing query components such as FROM, WHERE, HAVING, JOIN, GROUP BY, ORDER BY, and limits.
// The fields allow fine-grained customization of the query logic and structure.
type selectBuilder struct {
	from    Alias
	where   And
	having  And
	joins   []join
	fields  []Alias
	orders  []Order
	groupBy []driver.Sqler
	fp      driver.Sqler
	err     error
	limit   uint64
	offset  uint64
}

// _ ensures that SelectBuilder implements the Returnable interface at compile-time.
var _ Returnable = SelectBuilder(nil)

// Select creates a new SelectBuilder for constructing SQL SELECT queries with specified fields.
func Select(fields ...any) SelectBuilder {
	sb := &selectBuilder{}
	sb.err = sb.setFields(fields)

	return sb
}

// Distinct applies the DISTINCT clause to the query. Accepts optional column names for "DISTINCT ON" functionality.
func (sb *selectBuilder) Distinct(args ...string) SelectBuilder {
	if len(args) == 0 {
		sb.fp = driver.Pure("DISTINCT")
		return sb
	}

	columns := make([]Column, len(args))
	for i, arg := range args {
		columns[i] = Column(arg)
	}

	sb.fp = &distinctOn{columns: columns}
	return sb
}

// All sets the SELECT modifier to ALL, allowing duplicate rows in the query results, and returns the updated SelectBuilder.
func (sb *selectBuilder) All() SelectBuilder {
	sb.fp = driver.Pure("ALL")
	return sb
}

// From sets the source table or alias for the SELECT statement. Accepts either a string or an Alias as the input.
func (sb *selectBuilder) From(from any) SelectBuilder {
	switch val := from.(type) {
	case string:
		al := ColumnAlias(Column(val))
		sb.from = al
		return sb
	case Alias:
		sb.from = val
		return sb
	}

	sb.err = fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, from)
	return sb
}

// Where adds a condition to the WHERE clause of the query using the provided driver.Sqler expression.
// Returns the updated SelectBuilder instance.
func (sb *selectBuilder) Where(exp driver.Sqler) SelectBuilder {
	if exp != nil {
		sb.where = append(sb.where, exp)
	}

	return sb
}

// Having adds a condition to the HAVING clause of the SQL statement using the provided driver.Sqler expression.
func (sb *selectBuilder) Having(exp driver.Sqler) SelectBuilder {
	if exp != nil {
		sb.having = append(sb.having, exp)
	}

	return sb
}

// Join adds a new join clause to the current query with the specified table and condition.
func (sb *selectBuilder) Join(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(
		sb.joins,
		join{table: sb.parseJoinTable(table), on: on, joinType: joinDefault},
	)
	return sb
}

// LeftJoin adds a LEFT JOIN clause to the query with the specified table and ON condition.
func (sb *selectBuilder) LeftJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinLeft})
	return sb
}

// RightJoin adds a RIGHT JOIN clause with the specified table and ON condition to the SELECT query construction.
func (sb *selectBuilder) RightJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinRight})
	return sb
}

// InnerJoin adds an inner join clause to the query, specifying the table and join condition.
func (sb *selectBuilder) InnerJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinInner})
	return sb
}

// CrossJoin adds a CROSS JOIN clause to the SQL query with the specified table and condition.
func (sb *selectBuilder) CrossJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinCross})
	return sb
}

// Limit sets the maximum number of rows to retrieve in the query and returns the updated SelectBuilder instance.
func (sb *selectBuilder) Limit(limit uint64) SelectBuilder {
	sb.limit = limit
	return sb
}

// Offset sets the offset for the SELECT query, specifying the number of rows to skip before starting to return rows.
func (sb *selectBuilder) Offset(offset uint64) SelectBuilder {
	sb.offset = offset
	return sb
}

// GroupBy adds one or more columns or expressions to the GROUP BY clause. Accepts strings or objects implementing Sqler.
func (sb *selectBuilder) GroupBy(groups ...any) SelectBuilder {
	for i := range groups {
		switch g := groups[i].(type) {
		case string:
			sb.groupBy = append(sb.groupBy, Column(g))
		case driver.Sqler:
			sb.groupBy = append(sb.groupBy, g)
		default:
			sb.err = fmt.Errorf("%w: %T must be a string or driver.Sqler", ErrUnsupportedType, groups[i])
			return sb
		}
	}
	return sb
}

// OrderBy appends one or more ordering conditions to the query and returns the updated SelectBuilder instance.
func (sb *selectBuilder) OrderBy(orders ...Order) SelectBuilder {
	sb.orders = append(sb.orders, orders...)
	return sb
}

// Sql generates an SQL query string along with its arguments and any encountered error.
// It assembles the SELECT statement with fields, tables, joins, conditions, groups, orders, limits, and offsets.
func (sb *selectBuilder) Sql(options *driver.SqlOptions) (sql string, args []any, err error) {
	if sb.err != nil {
		err = sb.err
		return
	}

	var buf strings.Builder
	buf.WriteString("SELECT")
	if sb.fp != nil {
		sql, fpArgs, err := sb.fp.Sql(options)
		if err != nil {
			return "", nil, err
		}

		buf.WriteByte(' ')
		buf.WriteString(sql)
		args = append(args, fpArgs...)
	}

	if len(sb.fields) > 0 {
		sql, fieldsArgs, err := concatFields(sb.fields, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, fieldsArgs...)
		buf.WriteByte(' ')
		buf.WriteString(sql)
	} else {
		buf.WriteString(" *")
	}

	if sb.from != nil {
		buf.WriteString(" FROM ")
		sql, fromArgs, err := exprOrCol(sb.from, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, fromArgs...)
		buf.WriteString(sql)
	}

	if len(sb.joins) > 0 {
		for i := range sb.joins {
			buf.WriteString(" " + sb.joins[i].joinType.String() + " ")
			sql, tableArgs, err := sb.joins[i].table.Sql(options)
			if err != nil {
				return "", nil, err
			}

			args = append(args, tableArgs...)
			buf.WriteString(sql)

			if sb.joins[i].on == nil {
				return "", nil, fmt.Errorf(
					"%s operation requires an ON clause to specify join condition",
					sb.joins[i].joinType,
				)
			}

			buf.WriteString(" ON ")
			sql, onArgs, err := sb.joins[i].on.Sql(options)
			if err != nil {
				return "", nil, err
			}

			args = append(args, onArgs...)
			buf.WriteString(sql)
		}
	}

	if len(sb.where) > 0 {
		buf.WriteString(" WHERE ")
		sql, whereArgs, err := sb.where.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, whereArgs...)
		buf.WriteString(sql)
	}

	if len(sb.groupBy) > 0 {
		buf.WriteString(" GROUP BY ")
		sql, groupsArgs, err := concatFields(sb.groupBy, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, groupsArgs...)
		buf.WriteString(sql)
	}

	if len(sb.having) > 0 {
		buf.WriteString(" HAVING ")
		sql, havingArgs, err := sb.having.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, havingArgs...)
		buf.WriteString(sql)
	}

	if len(sb.orders) > 0 {
		buf.WriteString(" ORDER BY ")
		sql, ordersArgs, err := concatFields[Order](sb.orders, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, ordersArgs...)
		buf.WriteString(sql)
	}

	if sb.limit > 0 {
		sql, limitArgs, err := driver.Pure(" LIMIT ?", sb.limit).Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, limitArgs...)
		buf.WriteString(sql)
	}

	if sb.offset > 0 {
		sql, offsetArgs, err := driver.Pure(" OFFSET ?", sb.offset).Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, offsetArgs...)
		buf.WriteString(sql)
	}

	return buf.String(), args, nil
}

// PreparedSql generates a parameterized SQL string and corresponding arguments based on the provided SqlOptions.
func (sb *selectBuilder) PreparedSql(
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	return driver.Sql(sb, options)
}

// LimitReturningOne sets the query to return only one result by limiting the result set to a single record.
func (sb *selectBuilder) LimitReturningOne() {
	sb.Limit(1)
}

// With returns the alias of the current "from" table in the selectBuilder.
func (sb *selectBuilder) With() string {
	return sb.from.Alias()
}

// UsingTables returns a list of table aliases used in the query, including the primary table and any joined tables.
func (sb *selectBuilder) UsingTables() []string {
	from := sb.from.Alias()
	usingTables := make([]string, 0, len(sb.joins)+1)
	usingTables = append(usingTables, from)

	for i := range sb.joins {
		usingTables = append(usingTables, sb.joins[i].table.Alias())
	}

	return usingTables
}

// GetReturning retrieves the list of fields (aliases) currently set for the SELECT query's RETURNING clause.
func (sb *selectBuilder) GetReturning() []Alias {
	return sb.fields
}

// SetReturning sets the fields to be returned in the query by assigning the provided slice of Alias to sb.fields.
func (sb *selectBuilder) SetReturning(keys []Alias) {
	sb.fields = keys
}

// CounterType returns the CounterType associated with the selectBuilder, indicating a query operation.
func (sb *selectBuilder) CounterType() CounterType {
	return CounterQuery
}

// setFields assigns and validates fields for the SELECT query, ensuring each field is a string or Alias. Returns an error if invalid.
func (sb *selectBuilder) setFields(fields []any) error {
	sb.fields = nil
	for i := range fields {
		switch val := fields[i].(type) {
		case string:
			sb.fields = append(sb.fields, ColumnAlias(Column(val)))
		case Alias:
			sb.fields = append(sb.fields, val)
		default:
			return fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, fields[i])
		}
	}

	return nil
}

// parseJoinTable handles the parsing of a table input, validating and converting it into an Alias or setting an error.
func (sb *selectBuilder) parseJoinTable(table any) Alias {
	switch val := table.(type) {
	case string:
		return ColumnAlias(Column(val))
	case Alias:
		return val
	default:
		sb.err = fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, val)
		return nil
	}
}

// Desc creates a descending order instance for the specified column, determining SQL sorting logic.
func Desc(column any) Order {
	return &order{
		target:    column,
		orderType: orderDesc,
		nullsType: nullsNone,
	}
}

// DescNullsLast returns an Order that sorts the specified column in descending order with null values placed last.
func DescNullsLast(column any) Order {
	return &order{
		target:    column,
		orderType: orderDesc,
		nullsType: nullsLast,
	}
}

// DescNullsFirst creates a descending order with NULL values sorted first for the specified column.
func DescNullsFirst(column any) Order {
	return &order{
		target:    column,
		orderType: orderDesc,
		nullsType: nullsFirst,
	}
}

// Asc creates an ascending order directive for the specified column or expression and returns an implementation of Order.
func Asc(column any) Order {
	return &order{
		target:    column,
		orderType: orderAsc,
		nullsType: nullsNone,
	}
}

// AscNullsLast returns an Order that arranges a column in ascending order with null values placed at the end.
func AscNullsLast(column any) Order {
	return &order{
		target:    column,
		orderType: orderAsc,
		nullsType: nullsLast,
	}
}

// AscNullsFirst creates an ascending order expression for the specified column with `NULL` values sorted first.
func AscNullsFirst(column any) Order {
	return &order{
		target:    column,
		orderType: orderAsc,
		nullsType: nullsFirst,
	}
}

// Sql generates a SQL string along with its arguments for the order clause, considering the specified SqlOptions.
func (o *order) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := exprOrCol(o.target, options)
	if err != nil {
		return "", nil, err
	}

	if o.orderType == orderAsc || o.orderType == orderDesc {
		sql += " " + o.orderType.String()
	}

	if o.nullsType != nullsNone {
		sql += " " + o.nullsType.String()
	}

	return sql, args, nil
}

// Target returns the target value associated with the order instance.
func (o *order) Target() any {
	return o.target
}

// NullsType returns the nullsOrderType value associated with the order criteria.
func (o *order) NullsType() nullsOrderType {
	return o.nullsType
}

// OrderType returns the orderType value associated with the order instance.
func (o *order) OrderType() orderType {
	return o.orderType
}

// Sql generates an SQL string with arguments for the DISTINCT ON clause, using the provided options for customization.
func (d *distinctOn) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := concatFields(d.columns, options)
	if err != nil {
		return "", nil, err
	}

	return "DISTINCT ON (" + sql + ")", args, nil
}

// String converts a nullsOrderType constant to its corresponding string representation. Returns an empty string for unknown values.
func (j nullsOrderType) String() string {
	switch j {
	case nullsLast:
		return "NULLS LAST"
	case nullsFirst:
		return "NULLS FIRST"
	}

	return ""
}

// String returns the string representation of the orderType, such as "ASC" or "DESC", based on its value.
func (j orderType) String() string {
	switch j {
	case orderAsc:
		return "ASC"
	case orderDesc:
		return "DESC"
	}

	return ""
}

// String returns the string representation of the joinType, mapping to specific SQL join types or a default "JOIN".
func (j joinType) String() string {
	switch j {
	case joinLeft:
		return "LEFT JOIN"
	case joinRight:
		return "RIGHT JOIN"
	case joinInner:
		return "INNER JOIN"
	case joinCross:
		return "CROSS JOIN"
	}

	return "JOIN"
}
