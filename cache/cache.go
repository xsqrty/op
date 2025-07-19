package cache

import (
	"fmt"
	"sync"

	"github.com/xsqrty/op/driver"
)

// Container is an interface that provides a method to use SQL generation with specified arguments.
type Container interface {
	Use(args Args) driver.Sqler
}

// Result represents the outcome of processing SQL queries, including the SQL string, arguments, and error state.
type Result struct {
	Sql         string
	Args        []any
	Err         error
	ArgsIndexes map[string]int
}

// Args represents a map of argument keys to their associated values, used for parameterized operations and queries.
type Args map[string]any

// ArgLabel represents a labeled argument with an associated key used for SQL query construction.
type ArgLabel struct {
	Key string
}

// cache is a struct used to handle SQL caching by leveraging a container and input arguments.
type cache struct {
	args      Args
	container *container
}

// container is a structure that holds a Result, a driver.Sqler implementation, and ensures res is set only once.
type container struct {
	res     Result
	input   driver.Sqler
	resOnce sync.Once
}

// Arg creates and returns a pointer to an ArgLabel struct initialized with the provided key value.
func Arg(key string) *ArgLabel {
	return &ArgLabel{Key: key}
}

// New creates and returns a new Container instance initialized with the given Sqler implementation.
func New(input driver.Sqler) Container {
	return &container{
		input: input,
	}
}

// Use initializes a cache with the given arguments and binds it to the calling container.
func (c *container) Use(args Args) driver.Sqler {
	return &cache{
		container: c,
		args:      args,
	}
}

// Sql generates the SQL query string and its arguments based on the provided driver.SqlOptions configuration.
func (c *cache) Sql(options *driver.SqlOptions) (string, []any, error) {
	c.container.resOnce.Do(func() {
		sql, args, err := c.container.input.Sql(options)
		if err != nil {
			c.container.res.Err = err
			return
		}

		PrepareArgs(&c.container.res, sql, args)
	})

	args, err := GetArgs(&c.container.res, c.args)
	if err != nil {
		return "", nil, err
	}

	return c.container.res.Sql, args, nil
}

// PrepareArgs initializes the Result object with the provided SQL string and arguments.
// It also maps keys from ArgLabel elements within args to their respective indexes in the Result.ArgIndexes map.
func PrepareArgs(res *Result, sql string, args []any) {
	res.Sql = sql
	res.Args = args
	res.ArgsIndexes = make(map[string]int)

	for i := range res.Args {
		if arg, ok := res.Args[i].(*ArgLabel); ok {
			res.ArgsIndexes[arg.Key] = i
		}
	}
}

// GetArgs retrieves and prepares a list of arguments for SQL execution based on provided input arguments and a result container.
// It returns an error if the result has a non-nil error, if the argument count mismatches, or if an argument key is invalid.
func GetArgs(res *Result, inputArgs Args) ([]any, error) {
	if res.Err != nil {
		return nil, res.Err
	}

	if len(res.ArgsIndexes) != len(inputArgs) {
		return nil, fmt.Errorf("incorrect argument count")
	}

	args := make([]any, len(res.Args))
	copy(args, res.Args)

	for k, v := range inputArgs {
		index, ok := res.ArgsIndexes[k]
		if !ok {
			return nil, fmt.Errorf("no such arg %q inside container", k)
		}

		args[index] = v
	}

	return args, nil
}
