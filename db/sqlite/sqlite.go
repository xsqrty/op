package sqlite

import (
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xsqrty/op/db"
)

type (
	OpenOption func(options *openOptions)
)

// openOptions sqlite pool configuration
type openOptions struct {
	maxIdleCount int
	maxOpen      int
	maxLifetime  time.Duration
	maxIdleTime  time.Duration
}

// Open establishes a connection to the sqlite database.
func Open(dsn string, options ...OpenOption) (db.ConnPool, error) {
	pool, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	config := openOptions{}
	for _, option := range options {
		option(&config)
	}

	if config.maxIdleCount > 0 {
		pool.SetMaxIdleConns(config.maxIdleCount)
	}

	if config.maxOpen > 0 {
		pool.SetMaxOpenConns(config.maxOpen)
	}

	if config.maxLifetime > 0 {
		pool.SetConnMaxLifetime(config.maxLifetime)
	}

	if config.maxIdleTime > 0 {
		pool.SetConnMaxIdleTime(config.maxIdleTime)
	}

	if err = pool.Ping(); err != nil {
		if err := pool.Close(); err != nil {
			return nil, err
		}

		return nil, err
	}

	return db.NewConnPool(pool, NewSqlOptions()), nil
}

// WithMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If MaxOpenConns are greater than 0 but less than the new MaxIdleConns,
// then the new MaxIdleConns will be reduced to match the MaxOpenConns limit.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections are currently 2. This may change in
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
