package db

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xsqrty/op/driver"
)

func OpenPostgres(ctx context.Context, dsn string, options ...OpenPgxOption) (ConnPool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	for _, option := range options {
		option(config)
	}

	opened, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err = opened.Ping(ctx); err != nil {
		opened.Close()
		return nil, err
	}

	return &pgxAdapter{pool: opened, options: driver.NewPostgresSqlOptions()}, nil
}

func OpenSqlite(_ context.Context, dsn string, options ...OpenOption) (ConnPool, error) {
	opened, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	config := openOptions{}
	for _, option := range options {
		option(&config)
	}

	if config.maxIdleCount > 0 {
		opened.SetMaxIdleConns(config.maxIdleCount)
	}

	if config.maxOpen > 0 {
		opened.SetMaxOpenConns(config.maxOpen)
	}

	if config.maxLifetime > 0 {
		opened.SetConnMaxLifetime(config.maxLifetime)
	}

	if config.maxIdleTime > 0 {
		opened.SetConnMaxIdleTime(config.maxIdleTime)
	}

	if err = opened.Ping(); err != nil {
		opened.Close()
		return nil, err
	}

	return &connPool{stdDb: opened, options: driver.NewSqliteSqlOptions()}, nil
}
