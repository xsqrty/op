package op

import (
	"fmt"

	"github.com/xsqrty/op/driver"
)

// fun defines a structure for representing SQL functions with optional prefixes and arguments.
type fun struct {
	name   string
	prefix string
	args   []any
}

// cast is a type used for casting a value to a specific SQL type when generating SQL statements.
type cast struct {
	// arg holds the value to be cast, which can be any type.
	arg any
	// typ specifies the target SQL data type for the cast operation.
	typ string
}

// Func creates a SQL function using the specified name and arguments, returning a Sqler for SQL generation.
func Func(name string, args ...any) driver.Sqler {
	return &fun{name, "", args}
}

// FuncPrefix creates an SQL function with a given name, optional prefix, and arguments, returning a Sqler instance.
func FuncPrefix(name string, prefix string, args ...any) driver.Sqler {
	return &fun{name, prefix, args}
}

// Cast creates a SQL CAST expression for converting `arg` into the specified SQL type `typ`.
func Cast(arg any, typ string) driver.Sqler {
	return &cast{arg: arg, typ: typ}
}

// Any wraps the given Sqler argument with the SQL ANY function, returning a new Sqler interface.
func Any(arg driver.Sqler) driver.Sqler {
	return Func("ANY", arg)
}

// All constructs an SQL ALL function with the given argument and returns a Sqler interface for SQL generation.
func All(arg driver.Sqler) driver.Sqler {
	return Func("ALL", arg)
}

// Concat generates a SQL CONCAT function call with the provided arguments. Returns a driver.Sqler for SQL generation.
func Concat(args ...any) driver.Sqler {
	return manyArgsColumn("CONCAT", args)
}

// Coalesce generates a SQL COALESCE function with the provided arguments and returns a Sqler interface.
func Coalesce(args ...any) driver.Sqler {
	return manyArgsColumn("COALESCE", args)
}

// Lower generates a SQL LOWER function call for the given argument, typically used to convert text to lowercase.
func Lower(arg any) driver.Sqler {
	return oneArgColumn("LOWER", arg)
}

// Upper wraps the SQL function LOWER around a given argument to create a case-insensitive column or value reference.
func Upper(arg any) driver.Sqler {
	return oneArgColumn("LOWER", arg)
}

// Max generates a SQL MAX function for the given argument. It returns an implementation of the Sqler interface.
func Max(arg any) driver.Sqler {
	return oneArgColumn("MAX", arg)
}

// Min creates a SQL MIN function call for the given argument and returns it as a driver.Sqler.
func Min(arg any) driver.Sqler {
	return oneArgColumn("MIN", arg)
}

// Sum generates a SQL SUM function for the given argument and returns a driver.Sqler for SQL string construction.
func Sum(arg any) driver.Sqler {
	return oneArgColumn("SUM", arg)
}

// Avg generates a SQL AVG function expression for the given argument, returning a Sqler interface.
func Avg(arg any) driver.Sqler {
	return oneArgColumn("AVG", arg)
}

// Abs applies the SQL ABS function to the given argument and returns a Sqler for generating the SQL representation.
func Abs(arg any) driver.Sqler {
	return oneArgColumn("ABS", arg)
}

// Count generates a SQL COUNT function for the specified argument and returns it as a driver.Sqler interface.
func Count(arg any) driver.Sqler {
	return oneArgColumn("COUNT", arg)
}

// CountDistinct generates a SQL COUNT expression with a DISTINCT modifier for the given argument.
func CountDistinct(arg any) driver.Sqler {
	return oneArgPrefixColumn("COUNT", "DISTINCT", arg)
}

// Sql generates a SQL representation of the fun type, combining its name, prefix, and arguments with proper formatting.
func (f fun) Sql(options *driver.SqlOptions) (string, []any, error) {
	sql, args, err := list(f.args).Sql(options)
	if err != nil {
		return "", nil, err
	}

	prefix := ""
	if f.prefix != "" {
		prefix = f.prefix + " "
	}

	return f.name + "(" + prefix + sql + ")", args, nil
}

// Sql generates an SQL string for the current cast instance using the provided SqlOptions and returns the query, arguments, and error.
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

// oneArgPrefixColumn creates a new SQL function call with a name, a prefix, and a single argument converted to a Column if it's a string.
func oneArgPrefixColumn(name string, prefix string, arg any) driver.Sqler {
	if str, ok := arg.(string); ok {
		arg = Column(str)
	}

	return FuncPrefix(name, prefix, arg)
}

// oneArgColumn creates an SQL function with one argument, converting string arguments to a Column type if applicable.
func oneArgColumn(name string, arg any) driver.Sqler {
	if str, ok := arg.(string); ok {
		arg = Column(str)
	}

	return Func(name, arg)
}

// manyArgsColumn constructs a SQL function call with the specified name and argument list, converting string arguments to columns.
func manyArgsColumn(name string, args []any) driver.Sqler {
	for i, arg := range args {
		if str, ok := arg.(string); ok {
			args[i] = Column(str)
		}
	}

	return Func(name, args...)
}
