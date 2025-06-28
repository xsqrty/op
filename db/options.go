package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type OpenOption func(options *openOptions)
type OpenPgxOption func(options *pgxpool.Config)

type openOptions struct {
	maxIdleCount int
	maxOpen      int
	maxLifetime  time.Duration
	maxIdleTime  time.Duration
}

// WithPgxBeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func WithPgxBeforeConnect(f func(context.Context, *pgx.ConnConfig) error) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.BeforeConnect = f
	}
}

// WithPgxAfterConnect is called after a connection is established, but before it is added to the pool.
func WithPgxAfterConnect(f func(context.Context, *pgx.Conn) error) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.AfterConnect = f
	}
}

// WithPgxBeforeAcquire is called before a connection is acquired from the pool. It must return true to allow the
// acquisition or false to indicate that the connection should be destroyed and a different connection should be
// acquired.
func WithPgxBeforeAcquire(f func(context.Context, *pgx.Conn) bool) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.BeforeAcquire = f
	}
}

// WithPgxAfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to
// return the connection to the pool or false to destroy the connection.
func WithPgxAfterRelease(f func(*pgx.Conn) bool) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.AfterRelease = f
	}
}

// WithPgxBeforeClose is called right before a connection is closed and removed from the pool.
func WithPgxBeforeClose(f func(*pgx.Conn)) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.BeforeClose = f
	}
}

// WithPgxMaxConnLifetime is the duration since creation after which a connection will be automatically closed.
func WithPgxMaxConnLifetime(d time.Duration) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.MaxConnLifetime = d
	}
}

// WithPgxMaxConnLifetimeJitter is the duration after MaxConnLifetime to randomly decide to close a connection.
// This helps prevent all connections from being closed at the exact same time, starving the pool.
func WithPgxMaxConnLifetimeJitter(d time.Duration) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.MaxConnLifetimeJitter = d
	}
}

// WithPgxMaxConnIdleTime is the duration after which an idle connection will be automatically closed by the health check.
func WithPgxMaxConnIdleTime(d time.Duration) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.MaxConnIdleTime = d
	}
}

// WithPgxMaxConns is the maximum size of the pool. The default is the greater of 4 or runtime.NumCPU().
func WithPgxMaxConns(n int32) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.MaxConns = n
	}
}

// WithPgxMinConns is the minimum size of the pool. After connection closes, the pool might dip below MinConns. A low
// number of MinConns might mean the pool is empty after MaxConnLifetime until the health check has a chance
// to create new connections.
func WithPgxMinConns(n int32) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.MinConns = n
	}
}

// WithPgxMinIdleConns is the minimum number of idle connections in the pool. You can increase this to ensure that
// there are always idle connections available. This can help reduce tail latencies during request processing,
// as you can avoid the latency of establishing a new connection while handling requests. It is superior
// to MinConns for this purpose.
// Similar to MinConns, the pool might temporarily dip below MinIdleConns after connection closes.
func WithPgxMinIdleConns(n int32) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.MinIdleConns = n
	}
}

// WithPgxHealthCheckPeriod is the duration between checks of the health of idle connections.
func WithPgxHealthCheckPeriod(d time.Duration) OpenPgxOption {
	return func(options *pgxpool.Config) {
		options.HealthCheckPeriod = d
	}
}

// WithMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections is currently 2. This may change in
// a future release.
func WithMaxIdleConns(n int) OpenOption {
	return func(options *openOptions) {
		options.maxIdleCount = n
	}
}

// WithMaxOpenConns sets the maximum number of open connections to the database.
//
// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than
// MaxIdleConns, then MaxIdleConns will be reduced to match the new
// MaxOpenConns limit.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func WithMaxOpenConns(n int) OpenOption {
	return func(options *openOptions) {
		options.maxOpen = n
	}
}

// WithConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are not closed due to a connection's age.
func WithConnMaxLifetime(d time.Duration) OpenOption {
	return func(options *openOptions) {
		options.maxLifetime = d
	}
}

// WithConnMaxIdleTime sets the maximum amount of time a connection may be idle.
//
// Expired connections may be closed lazily before reuse.
//
// If d <= 0, connections are not closed due to a connection's idle time.
func WithConnMaxIdleTime(d time.Duration) OpenOption {
	return func(options *openOptions) {
		options.maxIdleTime = d
	}
}
