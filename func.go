package op

import (
	"fmt"
	"github.com/xsqrty/op/driver"
)

type fun struct {
	name   string
	prefix string
	args   []any
}

type cast struct {
	arg any
	typ string
}

func Func(name string, args ...any) driver.Sqler {
	return &fun{name, "", args}
}

func FuncPrefix(name string, prefix string, args ...any) driver.Sqler {
	return &fun{name, prefix, args}
}

func Cast(arg any, typ string) driver.Sqler {
	return &cast{arg: arg, typ: typ}
}

func Any(arg driver.Sqler) driver.Sqler {
	return Func("ANY", arg)
}

func All(arg driver.Sqler) driver.Sqler {
	return Func("ALL", arg)
}

func Concat(args ...any) driver.Sqler {
	return manyArgsColumn("CONCAT", args)
}

func Coalesce(args ...any) driver.Sqler {
	return manyArgsColumn("COALESCE", args)
}

func Lower(arg any) driver.Sqler {
	return oneArgColumn("LOWER", arg)
}

func Upper(arg any) driver.Sqler {
	return oneArgColumn("LOWER", arg)
}

func Max(arg any) driver.Sqler {
	return oneArgColumn("MAX", arg)
}

func Min(arg any) driver.Sqler {
	return oneArgColumn("MIN", arg)
}

func Sum(arg any) driver.Sqler {
	return oneArgColumn("SUM", arg)
}

func Avg(arg any) driver.Sqler {
	return oneArgColumn("AVG", arg)
}

func Abs(arg any) driver.Sqler {
	return oneArgColumn("ABS", arg)
}

func Count(arg any) driver.Sqler {
	return oneArgColumn("COUNT", arg)
}

func CountDistinct(arg any) driver.Sqler {
	return oneArgPrefixColumn("COUNT", "DISTINCT", arg)
}

func (f fun) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := list(f.args).Sql(options)
	if err != nil {
		return "", nil, err
	}

	prefix := ""
	if f.prefix != "" {
		prefix = f.prefix + " "
	}

	return fmt.Sprintf("%s(%s%s)", f.name, prefix, sql), args, nil
}

func (c cast) Sql(options *driver.SqlOptions) (string, []any, error) {
	if options.CastFormat == nil {
		return "", nil, fmt.Errorf("cast format is not described in sql options")
	}

	var sqler driver.Sqler
	switch v := c.arg.(type) {
	case string:
		sqler = Column(v)
	case driver.Sqler:
		sqler = v
	default:
		sqler = driver.Value(v)
	}

	sql, args, err := sqler.Sql(options)
	if err != nil {
		return "", nil, err
	}

	return options.CastFormat(sql, c.typ), args, nil
}

func oneArgPrefixColumn(name string, prefix string, arg any) driver.Sqler {
	if str, ok := arg.(string); ok {
		arg = Column(str)
	}

	return FuncPrefix(name, prefix, arg)
}

func oneArgColumn(name string, arg any) driver.Sqler {
	if str, ok := arg.(string); ok {
		arg = Column(str)
	}

	return Func(name, arg)
}

func manyArgsColumn(name string, args []any) driver.Sqler {
	for i, arg := range args {
		if str, ok := arg.(string); ok {
			args[i] = Column(str)
		}
	}

	return Func(name, args...)
}
