package op

import (
	"bytes"
	"fmt"
	"github.com/xsqrty/op/driver"
)

type orderType int
type joinType int
type nullsOrderType int

type order struct {
	column      any
	orderFormat orderType
	nullsFormat nullsOrderType
}

type join struct {
	table    alias
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

type SelectBuilder struct {
	fieldsPrefix string
	from         *alias
	where        And
	having       And
	joins        []join
	fields       []alias
	orders       []order
	groupBy      []driver.Sqler
	err          error
	limit        uint64
	offset       uint64
}

func Select(fields ...any) *SelectBuilder {
	sb := &SelectBuilder{}
	sb.err = sb.setFields(fields)

	return sb
}

func (sb *SelectBuilder) Distinct() *SelectBuilder {
	return sb.FieldsPrefix("DISTINCT")
}

func (sb *SelectBuilder) All() *SelectBuilder {
	return sb.FieldsPrefix("ALL")
}

func (sb *SelectBuilder) FieldsPrefix(fieldsPrefix string) *SelectBuilder {
	sb.fieldsPrefix = fieldsPrefix
	return sb
}

func (sb *SelectBuilder) From(from any) *SelectBuilder {
	switch val := from.(type) {
	case string:
		al := columnAlias(Column(val))
		sb.from = &al
		return sb
	case alias:
		sb.from = &val
		return sb
	}

	sb.err = fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, from)
	return sb
}

func (sb *SelectBuilder) Where(exp driver.Sqler) *SelectBuilder {
	if exp != nil {
		sb.where = append(sb.where, exp)
	}

	return sb
}

func (sb *SelectBuilder) Having(exp driver.Sqler) *SelectBuilder {
	if exp != nil {
		sb.having = append(sb.having, exp)
	}

	return sb
}

func (sb *SelectBuilder) Join(table any, on ...driver.Sqler) *SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinDefault})
	return sb
}

func (sb *SelectBuilder) LeftJoin(table any, on ...driver.Sqler) *SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinLeft})
	return sb
}

func (sb *SelectBuilder) RightJoin(table any, on ...driver.Sqler) *SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinRight})
	return sb
}

func (sb *SelectBuilder) InnerJoin(table any, on ...driver.Sqler) *SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinInner})
	return sb
}

func (sb *SelectBuilder) CrossJoin(table any, on ...driver.Sqler) *SelectBuilder {
	al, err := sb.parseJoinTable(table)
	if err != nil {
		sb.err = err
	}

	sb.joins = append(sb.joins, join{table: al, on: on, joinType: joinCross})
	return sb
}

func (sb *SelectBuilder) Limit(limit uint64) *SelectBuilder {
	sb.limit = limit
	return sb
}

func (sb *SelectBuilder) Offset(offset uint64) *SelectBuilder {
	sb.offset = offset
	return sb
}

func (sb *SelectBuilder) GroupBy(groups ...any) *SelectBuilder {
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

func (sb *SelectBuilder) OrderBy(orders ...order) *SelectBuilder {
	sb.orders = append(sb.orders, orders...)
	return sb
}

func (sb *SelectBuilder) Sql(options *driver.SqlOptions) (sql string, args []any, err error) {
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
		sql, ordersArgs, err := concatFields[order](sb.orders, options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, ordersArgs...)
		buf.WriteString(sql)
	}

	if sb.limit > 0 {
		sql, limitArgs, err := expr{" LIMIT " + string(driver.Placeholder), []any{sb.limit}}.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, limitArgs...)
		buf.WriteString(sql)
	}

	if sb.offset > 0 {
		sql, offsetArgs, err := expr{" OFFSET " + string(driver.Placeholder), []any{sb.offset}}.Sql(options)
		if err != nil {
			return "", nil, err
		}

		args = append(args, offsetArgs...)
		buf.WriteString(sql)
	}

	return buf.String(), args, nil
}

func (sb *SelectBuilder) LimitReturningOne() {
	sb.Limit(1)
}

func (sb *SelectBuilder) With() string {
	return sb.from.Alias()
}

func (sb *SelectBuilder) UsingTables() []string {
	from := sb.from.Alias()
	usingTables := make([]string, 0, len(sb.joins)+1)
	usingTables = append(usingTables, from)

	for i := range sb.joins {
		usingTables = append(usingTables, sb.joins[i].table.Alias())
	}

	return usingTables
}

func (sb *SelectBuilder) GetReturning() []alias {
	return sb.fields
}

func (sb *SelectBuilder) SetReturning(keys []any) error {
	return sb.setFields(keys)
}

func (sb *SelectBuilder) SetReturningAliases(keys []alias) {
	sb.fields = keys
}

func (sb *SelectBuilder) setFields(fields []any) error {
	sb.fields = nil
	for i := range fields {
		switch val := fields[i].(type) {
		case string:
			sb.fields = append(sb.fields, columnAlias(Column(val)))
		case alias:
			sb.fields = append(sb.fields, val)
		default:
			return fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, fields[i])
		}
	}

	return nil
}

func (sb *SelectBuilder) parseJoinTable(table any) (alias, error) {
	switch val := table.(type) {
	case string:
		return columnAlias(Column(val)), nil
	case alias:
		return val, nil
	default:
		return alias{}, fmt.Errorf("%w: %T. Must be a string or alias", ErrUnsupportedType, val)
	}
}

func Desc(column any) order {
	return order{
		column:      column,
		orderFormat: orderDesc,
		nullsFormat: nullsNone,
	}
}

func DescNullsLast(column any) order {
	return order{
		column:      column,
		orderFormat: orderDesc,
		nullsFormat: nullsLast,
	}
}

func DescNullsFirst(column any) order {
	return order{
		column:      column,
		orderFormat: orderDesc,
		nullsFormat: nullsFirst,
	}
}

func Asc(column any) order {
	return order{
		column:      column,
		orderFormat: orderAsc,
		nullsFormat: nullsNone,
	}
}

func AscNullsLast(column any) order {
	return order{
		column:      column,
		orderFormat: orderAsc,
		nullsFormat: nullsLast,
	}
}

func AscNullsFirst(column any) order {
	return order{
		column:      column,
		orderFormat: orderAsc,
		nullsFormat: nullsFirst,
	}
}

func (o order) Sql(options *driver.SqlOptions) (string, []any, error) {
	return o.order(options)
}

func (o order) order(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := exprOrCol(o.column, options)
	if err != nil {
		return "", nil, err
	}

	sql += " " + o.orderFormat.String()
	if o.nullsFormat != nullsNone {
		sql += " " + o.nullsFormat.String()
	}

	return sql, args, nil
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
