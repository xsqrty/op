package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xsqrty/op/db"
)

// OpenOption is a function type that applies modifications to a pgxpool.Config instance.
type OpenOption func(options *pgxpool.Config)

// Open initializes a connection pool to a database using the provided DSN and optional configuration options.
func Open(ctx context.Context, dsn string, options ...OpenOption) (db.ConnPool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	for _, option := range options {
		option(config)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return &pgxAdapter{pool: pool, options: NewSqlOptions()}, nil
}

// WithBeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func WithBeforeConnect(f func(context.Context, *pgx.ConnConfig) error) OpenOption {
	return func(options *pgxpool.Config) {
		options.BeforeConnect = f
	}
}

// WithAfterConnect is called after a connection is established, but before it is added to the pool.
func WithAfterConnect(f func(context.Context, *pgx.Conn) error) OpenOption {
	return func(options *pgxpool.Config) {
		options.AfterConnect = f
	}
}

// WithBeforeAcquire is called before a connection is acquired from the pool. It must return true to allow the
// acquisition or false to indicate that the connection should be destroyed and a different connection should be
// acquired.
func WithBeforeAcquire(f func(context.Context, *pgx.Conn) bool) OpenOption {
	return func(options *pgxpool.Config) {
		options.BeforeAcquire = f
	}
}

// WithAfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
// return the connection to the pool or false to destroy the connection.
func WithAfterRelease(f func(*pgx.Conn) bool) OpenOption {
	return func(options *pgxpool.Config) {
		options.AfterRelease = f
	}
}

// WithBeforeClose is called right before a connection is closed and removed from the pool.
func WithBeforeClose(f func(*pgx.Conn)) OpenOption {
	return func(options *pgxpool.Config) {
		options.BeforeClose = f
	}
}

// WithMaxConnLifetime duration since creation after which a connection will be automatically closed.
func WithMaxConnLifetime(d time.Duration) OpenOption {
	return func(options *pgxpool.Config) {
		options.MaxConnLifetime = d
	}
}

// WithMaxConnLifetimeJitter is the duration after MaxConnLifetime to randomly decide to close a connection.
// This helps prevent all connections from being closed at the exact same time, starving the pool.
func WithMaxConnLifetimeJitter(d time.Duration) OpenOption {
	return func(options *pgxpool.Config) {
		options.MaxConnLifetimeJitter = d
	}
}

// WithMaxConnIdleTime is the duration after which the health check will automatically close an idle connection.
func WithMaxConnIdleTime(d time.Duration) OpenOption {
	return func(options *pgxpool.Config) {
		options.MaxConnIdleTime = d
	}
}

// WithMaxConns is the maximum size of the pool. The default is the greater of 4 or runtime.NumCPU().
func WithMaxConns(n int32) OpenOption {
	return func(options *pgxpool.Config) {
		options.MaxConns = n
	}
}

// WithMinConns is the minimum size of the pool. After the connection closes, the pool might dip below MinConns. A low
// number of MinConns might mean the pool is empty after MaxConnLifetime until the health check has a chance
// to create new connections.
func WithMinConns(n int32) OpenOption {
	return func(options *pgxpool.Config) {
		options.MinConns = n
	}
}

// WithMinIdleConns is the minimum number of idle connections in the pool. You can increase this to ensure that
// there are always idle connections available. This can help reduce tail latencies during request processing,
// as you can avoid the latency of establishing a new connection while handling requests. It is superior
// to MinConns for this purpose.
// Similar to MinConns, the pool might temporarily dip below MinIdleConns after the connection closes.
func WithMinIdleConns(n int32) OpenOption {
	return func(options *pgxpool.Config) {
		options.MinIdleConns = n
	}
}

// WithHealthCheckPeriod is the duration between checks of the health of idle connections.
func WithHealthCheckPeriod(d time.Duration) OpenOption {
	return func(options *pgxpool.Config) {
		options.HealthCheckPeriod = d
	}
}
