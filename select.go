package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type SelectBuilder interface {
	Distinct() SelectBuilder
	All() SelectBuilder
	FieldsPrefix(fieldsPrefix string) SelectBuilder
	From(from any) SelectBuilder
	Where(exp driver.Sqler) SelectBuilder
	Having(exp driver.Sqler) SelectBuilder
	Join(table any, on ...driver.Sqler) SelectBuilder
	LeftJoin(table any, on ...driver.Sqler) SelectBuilder
	RightJoin(table any, on ...driver.Sqler) SelectBuilder
	InnerJoin(table any, on ...driver.Sqler) SelectBuilder
	CrossJoin(table any, on ...driver.Sqler) SelectBuilder
	Limit(limit uint64) SelectBuilder
	Offset(offset uint64) SelectBuilder
	GroupBy(groups ...any) SelectBuilder
	OrderBy(orders ...Order) SelectBuilder
	LimitReturningOne()
	With() string
	UsingTables() []string
	GetReturning() []Alias
	SetReturning(keys []any) error
	SetReturningAliases(keys []Alias)
	Sql(options *driver.SqlOptions) (sql string, args []any, err error)
}

type Order interface {
	Target() any
	OrderFormat() orderType
	NullsFormat() nullsOrderType
	Sql(options *driver.SqlOptions) (string, []any, error)
}

type orderType int
type joinType int
type nullsOrderType int

type order struct {
	target      any
	orderFormat orderType
	nullsFormat nullsOrderType
}

type join struct {
	table    Alias
	joinType joinType
	on       And
}

const (
	orderDesc orderType = iota
	orderAsc
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
	fieldsPrefix string
	from         Alias
	where        And
	having       And
	joins        []join
	fields       []Alias
	orders       []Order
	groupBy      []driver.Sqler
	err          error
	limit        uint64
	offset       uint64
}

func Select(fields ...any) SelectBuilder {
	sb := &selectBuilder{}
	sb.err = sb.setFields(fields)

	return sb
}

func (sb *selectBuilder) Distinct() SelectBuilder {
	return sb.FieldsPrefix("DISTINCT")
}

func (sb *selectBuilder) All() SelectBuilder {
	return sb.FieldsPrefix("ALL")
}

func (sb *selectBuilder) FieldsPrefix(fieldsPrefix string) SelectBuilder {
	sb.fieldsPrefix = fieldsPrefix
	return sb
}

func (sb *selectBuilder) From(from any) SelectBuilder {
	switch val := from.(type) {
	case string:
		al := columnAlias(Column(val))
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

func (sb *selectBuilder) Join(table any, on ...driver.Sqler) SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinDefault})
	return sb
}

func (sb *selectBuilder) LeftJoin(table any, on ...driver.Sqler) SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinLeft})
	return sb
}

func (sb *selectBuilder) RightJoin(table any, on ...driver.Sqler) SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinRight})
	return sb
}

func (sb *selectBuilder) InnerJoin(table any, on ...driver.Sqler) SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinInner})
	return sb
}

func (sb *selectBuilder) CrossJoin(table any, on ...driver.Sqler) SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinCross})
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
			sb.err = fmt.Errorf("%w: %T", ErrUnsupportedType, groups[i])
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

	var buf bytes.Buffer
	buf.WriteString("SELECT")
	if sb.fieldsPrefix != "" {
		buf.WriteByte(' ')
		buf.WriteString(sb.fieldsPrefix)
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
			buf.WriteString(fmt.Sprintf(" %s ", sb.joins[i].joinType.String()))
			sql, tableArgs, err := sb.joins[i].table.Sql(options)
			if err != nil {
				return "", nil, err
			}

			args = append(args, tableArgs...)
			buf.WriteString(sql)

			if len(sb.joins[i].on) == 0 {
				return "", nil, fmt.Errorf("%s operation requires an ON clause to specify join condition", sb.joins[i].joinType)
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
		sql, limitArgs, err := Pure(" LIMIT ?", sb.limit).Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, limitArgs...)
		buf.WriteString(sql)
	}

	if sb.offset > 0 {
		sql, offsetArgs, err := Pure(" OFFSET ?", sb.offset).Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, offsetArgs...)
		buf.WriteString(sql)
	}

	return buf.String(), args, nil
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

func (sb *selectBuilder) SetReturning(keys []any) error {
	return sb.setFields(keys)
}

func (sb *selectBuilder) SetReturningAliases(keys []Alias) {
	sb.fields = keys
}

func (sb *selectBuilder) setFields(fields []any) error {
	sb.fields = nil
	for i := range fields {
		switch val := fields[i].(type) {
		case string:
			sb.fields = append(sb.fields, columnAlias(Column(val)))
		case Alias:
			sb.fields = append(sb.fields, val)
		default:
			return fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, fields[i])
		}
	}

	return nil
}

func (sb *selectBuilder) parseJoinTable(table any) (Alias, error) {
	switch val := table.(type) {
	case string:
		return columnAlias(Column(val)), nil
	case Alias:
		return val, nil
	default:
		return nil, fmt.Errorf("%w: %T must be a string or Alias", ErrUnsupportedType, val)
	}
}

func Desc(column any) Order {
	return &order{
		target:      column,
		orderFormat: orderDesc,
		nullsFormat: nullsNone,
	}
}

func DescNullsLast(column any) Order {
	return &order{
		target:      column,
		orderFormat: orderDesc,
		nullsFormat: nullsLast,
	}
}

func DescNullsFirst(column any) Order {
	return &order{
		target:      column,
		orderFormat: orderDesc,
		nullsFormat: nullsFirst,
	}
}

func Asc(column any) Order {
	return &order{
		target:      column,
		orderFormat: orderAsc,
		nullsFormat: nullsNone,
	}
}

func AscNullsLast(column any) Order {
	return &order{
		target:      column,
		orderFormat: orderAsc,
		nullsFormat: nullsLast,
	}
}

func AscNullsFirst(column any) Order {
	return &order{
		target:      column,
		orderFormat: orderAsc,
		nullsFormat: nullsFirst,
	}
}

func (o *order) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := exprOrCol(o.target, options)
	if err != nil {
		return "", nil, err
	}

	sql += " " + o.orderFormat.String()
	if o.nullsFormat != nullsNone {
		sql += " " + o.nullsFormat.String()
	}

	return sql, args, nil
}

func (o *order) Target() any {
	return o.target
}

func (o *order) NullsFormat() nullsOrderType {
	return o.nullsFormat
}

func (o *order) OrderFormat() orderType {
	return o.orderFormat
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
