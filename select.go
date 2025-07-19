package op

import (
	"fmt"
	"strings"

	"github.com/xsqrty/op/driver"
)

// SelectBuilder defines an interface for constructing SQL SELECT statements programmatically.
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

type Order interface {
	Target() any
	OrderType() orderType
	NullsType() nullsOrderType
	Sql(options *driver.SqlOptions) (string, []any, error)
}

type (
	orderType      int
	joinType       int
	nullsOrderType int
)

type order struct {
	target    any
	orderType orderType
	nullsType nullsOrderType
}

type join struct {
	table    Alias
	joinType joinType
	on       driver.Sqler
}

type distinctOn struct {
	columns []Column
}

const (
	orderDesc orderType = iota
	orderAsc
	orderNone
)

const (
	nullsLast nullsOrderType = iota
	nullsFirst
	nullsNone
)

const (
	joinDefault joinType = iota
	joinLeft
	joinRight
	joinInner
	joinCross
)

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

var _ Returnable = SelectBuilder(nil)

func Select(fields ...any) SelectBuilder {
	sb := &selectBuilder{}
	sb.err = sb.setFields(fields)

	return sb
}

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

func (sb *selectBuilder) All() SelectBuilder {
	sb.fp = driver.Pure("ALL")
	return sb
}

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

func (sb *selectBuilder) Where(exp driver.Sqler) SelectBuilder {
	if exp != nil {
		sb.where = append(sb.where, exp)
	}

	return sb
}

func (sb *selectBuilder) Having(exp driver.Sqler) SelectBuilder {
	if exp != nil {
		sb.having = append(sb.having, exp)
	}

	return sb
}

func (sb *selectBuilder) Join(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(
		sb.joins,
		join{table: sb.parseJoinTable(table), on: on, joinType: joinDefault},
	)
	return sb
}

func (sb *selectBuilder) LeftJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinLeft})
	return sb
}

func (sb *selectBuilder) RightJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinRight})
	return sb
}

func (sb *selectBuilder) InnerJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinInner})
	return sb
}

func (sb *selectBuilder) CrossJoin(table any, on driver.Sqler) SelectBuilder {
	sb.joins = append(sb.joins, join{table: sb.parseJoinTable(table), on: on, joinType: joinCross})
	return sb
}

func (sb *selectBuilder) Limit(limit uint64) SelectBuilder {
	sb.limit = limit
	return sb
}

func (sb *selectBuilder) Offset(offset uint64) SelectBuilder {
	sb.offset = offset
	return sb
}

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

func (sb *selectBuilder) OrderBy(orders ...Order) SelectBuilder {
	sb.orders = append(sb.orders, orders...)
	return sb
}

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

func (sb *selectBuilder) PreparedSql(
	options *driver.SqlOptions,
) (sql string, args []any, err error) {
	return driver.Sql(sb, options)
}

func (sb *selectBuilder) LimitReturningOne() {
	sb.Limit(1)
}

func (sb *selectBuilder) With() string {
	return sb.from.Alias()
}

func (sb *selectBuilder) UsingTables() []string {
	from := sb.from.Alias()
	usingTables := make([]string, 0, len(sb.joins)+1)
	usingTables = append(usingTables, from)

	for i := range sb.joins {
		usingTables = append(usingTables, sb.joins[i].table.Alias())
	}

	return usingTables
}

func (sb *selectBuilder) GetReturning() []Alias {
	return sb.fields
}

func (sb *selectBuilder) SetReturning(keys []Alias) {
	sb.fields = keys
}

func (sb *selectBuilder) CounterType() CounterType {
	return CounterQuery
}

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

func Desc(column any) Order {
	return &order{
		target:    column,
		orderType: orderDesc,
		nullsType: nullsNone,
	}
}

func DescNullsLast(column any) Order {
	return &order{
		target:    column,
		orderType: orderDesc,
		nullsType: nullsLast,
	}
}

func DescNullsFirst(column any) Order {
	return &order{
		target:    column,
		orderType: orderDesc,
		nullsType: nullsFirst,
	}
}

func Asc(column any) Order {
	return &order{
		target:    column,
		orderType: orderAsc,
		nullsType: nullsNone,
	}
}

func AscNullsLast(column any) Order {
	return &order{
		target:    column,
		orderType: orderAsc,
		nullsType: nullsLast,
	}
}

func AscNullsFirst(column any) Order {
	return &order{
		target:    column,
		orderType: orderAsc,
		nullsType: nullsFirst,
	}
}

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

func (o *order) Target() any {
	return o.target
}

func (o *order) NullsType() nullsOrderType {
	return o.nullsType
}

func (o *order) OrderType() orderType {
	return o.orderType
}

func (d *distinctOn) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := concatFields(d.columns, options)
	if err != nil {
		return "", nil, err
	}

	return "DISTINCT ON (" + sql + ")", args, nil
}

func (j nullsOrderType) String() string {
	switch j {
	case nullsLast:
		return "NULLS LAST"
	case nullsFirst:
		return "NULLS FIRST"
	}

	return ""
}

func (j orderType) String() string {
	switch j {
	case orderAsc:
		return "ASC"
	case orderDesc:
		return "DESC"
	}

	return ""
}

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
