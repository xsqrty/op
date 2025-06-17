package op

import (
	"fmt"
	"github.com/xsqrty/op/driver"
)

type operator struct {
	key       any
	operator  string
	value     any
	wrapValue bool
}

func Like(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "LIKE", value: val}
}

func NotLike(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "NOT LIKE", value: val}
}

func ILike(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "ILIKE", value: val}
}

func NotILike(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "NOT ILIKE", value: val}
}

func Lt(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "<", value: val}
}

func Gt(key any, val any) driver.Sqler {
	return &operator{key: key, operator: ">", value: val}
}

func Gte(key any, val any) driver.Sqler {
	return &operator{key: key, operator: ">=", value: val}
}

func Lte(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "<=", value: val}
}

func Eq(key any, val any) driver.Sqler {
	if val == nil {
		return &operator{key: key, operator: "IS NULL"}
	}

	return &operator{key: key, operator: "=", value: val}
}

func Ne(key any, val any) driver.Sqler {
	if val == nil {
		return &operator{key: key, operator: "IS NOT NULL"}
	}

	return &operator{key: key, operator: "!=", value: val}
}

func Lc(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "@>", value: val}
}

func Rc(key any, val any) driver.Sqler {
	return &operator{key: key, operator: "<@", value: val}
}

func ExtractText(arg any, path ...any) driver.Sqler {
	return &operator{key: arg, operator: "#>>", value: Array(path...)}
}

func ExtractObject(arg any, path ...any) driver.Sqler {
	return &operator{key: arg, operator: "#>", value: Array(path...)}
}

func HasProp(key any, args ...any) driver.Sqler {
	return &operator{key: key, operator: "?|", value: Array(args...)}
}

func HasProps(key any, args ...any) driver.Sqler {
	return &operator{key: key, operator: "?&", value: Array(args...)}
}

func In(key any, values ...any) driver.Sqler {
	return &operator{key: key, operator: "IN", value: list(values), wrapValue: true}
}

func Nin(key any, values ...any) driver.Sqler {
	return &operator{key: key, operator: "NOT IN", value: list(values), wrapValue: true}
}

func (op *operator) Sql(options *driver.SqlOptions) (string, []any, error) {
	keySql, argsKey, err := exprOrCol(op.key, options)
	if err != nil {
		return "", nil, err
	}

	if op.value == nil {
		return driver.Pure(fmt.Sprintf("%s %s", keySql, op.operator), argsKey...).Sql(options)
	}

	valSql, argsVal, err := exprOrVal(op.value, options)
	if err != nil {
		return "", nil, err
	}

	format := "%s %s %s"
	if op.wrapValue {
		format = "%s %s (%s)"
	}

	return driver.Pure(fmt.Sprintf(format, keySql, op.operator, valSql), append(argsKey, argsVal...)...).Sql(options)
}
