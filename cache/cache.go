package cache

type arg struct {
	key   string
	value any
}

type result struct {
	sql         string
	args        []any
	err         error
	argsIndexes map[string]int
}

type Args map[string]any

func Arg(key string) *arg {
	return &arg{key: key}
}
