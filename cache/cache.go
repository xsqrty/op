package cache

import (
	"fmt"
	"sync"

	"github.com/xsqrty/op/driver"
)

type Container interface {
	Use(args Args) driver.Sqler
}

type Result struct {
	Sql         string
	Args        []any
	Err         error
	ArgsIndexes map[string]int
}

type Args map[string]any

type ArgData struct {
	Key string
}

type cache struct {
	args      Args
	container *container
}

type container struct {
	res     Result
	input   driver.Sqler
	resOnce sync.Once
}

func Arg(key string) *ArgData {
	return &ArgData{Key: key}
}

func New(input driver.Sqler) Container {
	return &container{
		input: input,
	}
}

func (c *container) Use(args Args) driver.Sqler {
	return &cache{
		container: c,
		args:      args,
	}
}

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

func PrepareArgs(res *Result, sql string, args []any) {
	res.Sql = sql
	res.Args = args
	res.ArgsIndexes = make(map[string]int)

	for i := range res.Args {
		if arg, ok := res.Args[i].(*ArgData); ok {
			res.ArgsIndexes[arg.Key] = i
		}
	}
}

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
