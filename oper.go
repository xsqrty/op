package op

import (
	"github.com/xsqrty/op/driver"
)

// operator represents a SQL operation with a key, operator string, and value, supporting optional value wrapping.
type operator struct {
	key       any
	operator  string
	value     any
	wrapValue bool
}

// Like returns a Sqler that generates a SQL "LIKE" condition with the specified key and value.
func Like(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "LIKE", value: val}
}

// NotLike creates a SQL condition using the "NOT LIKE" operator for the specified key and value.
// Returns a driver.Sqler interface for SQL generation.
func NotLike(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "NOT LIKE", value: val}
}

// ILike creates a case-insensitive "ILIKE" SQL operator expression with the given key and value.
// It returns a Sqler interface for SQL generation.
func ILike(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "ILIKE", value: val}
}

// NotILike returns a Sqler instance representing a "NOT ILIKE" SQL operator for a given key and value.
func NotILike(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "NOT ILIKE", value: val}
}

// Lt creates a SQL "<" operator for comparing a column or expression (key) with a value (val).
func Lt(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "<", value: val}
}

// Gt constructs a SQL "greater than" (>) condition with the given key and value.
func Gt(key any, val any) driver.Sqler {
	return &operator{key: key, operator: ">", value: val}
}

// Gte constructs an SQL query component representing a "greater than or equal to" (>=) comparison with the specified key and value.
func Gte(key any, val any) driver.Sqler {
	return &operator{key: key, operator: ">=", value: val}
}

// Lte creates an SQL operator for "less than or equal to" comparison with the given key and value.
func Lte(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "<=", value: val}
}

// Eq creates a SQL equality condition with the specified key and value.
// If the value is nil, it generates an "IS NULL" condition.
// Returns a driver.Sqler implementation for constructing SQL statements.
func Eq(key any, val any) driver.Sqler {
	if val == nil {
		return &operator{key: key, operator: "IS NULL"}
	}

	return &operator{key: key, operator: "=", value: val}
}

// Ne creates a SQL operator for "not equal to" (!=) or "IS NOT NULL" if the value provided is nil.
// Returns an implementation of the driver.Sqler interface.
func Ne(key any, val any) driver.Sqler {
	if val == nil {
		return &operator{key: key, operator: "IS NOT NULL"}
	}

	return &operator{key: key, operator: "!=", value: val}
}

// Lc represents a PostgreSQL contains (`@>`) operator, checking if a key contains a given value.
// Returns a driver.Sqler that can generate the SQL string and arguments.
func Lc(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "@>", value: val}
}

// Rc creates a new SQL operator representing the "contains by" operation (`<@`) between a key and a value.
func Rc(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "<@", value: val}
}

// ExtractText generates a SQL expression to extract text from a JSON column at specified path elements.
func ExtractText(arg any, path ...any) driver.Sqler {
	return &operator{key: arg, operator: "#>>", value: Array(path...)}
}

// ExtractObject generates a SQL operator "#>" for extracting a JSONB object using an array of keys.
func ExtractObject(arg any, path ...any) driver.Sqler {
	return &operator{key: arg, operator: "#>", value: Array(path...)}
}

// HasProp creates an operator to check if a key contains any specified properties using the "?|" SQL operator.
func HasProp(key any, args ...any) driver.Sqler {
	return &operator{key: key, operator: "?|", value: Array(args...)}
}

// HasProps constructs an operator to check if a JSON/array key contains all specified elements using `?&`.
func HasProps(key any, args ...any) driver.Sqler {
	return &operator{key: key, operator: "?&", value: Array(args...)}
}

// In constructs an SQL "IN" operator expression with the given key and values, returning a Sqler for query building.
func In(key any, values ...any) driver.Sqler {
	return &operator{key: key, operator: "IN", value: list(values), wrapValue: true}
}

// Nin creates a SQL condition for the "NOT IN" operator with a key and a variadic list of values.
func Nin(key any, values ...any) driver.Sqler {
	return &operator{key: key, operator: "NOT IN", value: list(values), wrapValue: true}
}

// Sql generates a SQL query string, its arguments, and handles errors based on the operator's configuration and options provided.
func (op *operator) Sql(options *driver.SqlOptions) (string, []any, error) {
	keySql, argsKey, err := exprOrCol(op.key, options)
	if err != nil {
		return "", nil, err
	}

	if op.value == nil {
		return driver.Pure(keySql+" "+op.operator, argsKey...).Sql(options)
	}

	valSql, argsVal, err := exprOrVal(op.value, options)
	if err != nil {
		return "", nil, err
	}

	sqlValue := ""
	if op.wrapValue {
		sqlValue = keySql + " " + op.operator + " (" + valSql + ")"
	} else {
		sqlValue = keySql + " " + op.operator + " " + valSql
	}

	return driver.Pure(sqlValue, append(argsKey, argsVal...)...).Sql(options)
}
