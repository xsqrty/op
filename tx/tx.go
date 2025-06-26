package tx

import (
	"context"
)

type Manager interface {
	Do(ctx context.Context, handler Handler) error
}

type Handler func(ctx context.Context) error

type txk string

var txKey = txk("tx")
