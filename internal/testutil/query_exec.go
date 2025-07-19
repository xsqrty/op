package testutil

import (
	"context"

	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

// mockQueryExec represents a mock implementation combining queryable and executable database operations for testing.
type mockQueryExec struct {
	Q *mockQueryable
	E *mockExecutor
}

// NewMockQueryExec creates a new instance of mockQueryExec with initialized mock queryable and executor components.
func NewMockQueryExec() *mockQueryExec {
	return &mockQueryExec{
		Q: NewMockQueryable(),
		E: NewMockExecutor(),
	}
}

// Query executes a mock SQL query with the given context, SQL statement, and arguments.
func (m *mockQueryExec) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	return m.Q.Query(ctx, sql, args...)
}

// QueryRow executes a mock SQL query that is expected to return at most one row.
func (m *mockQueryExec) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	return m.Q.QueryRow(ctx, sql, args...)
}

// SqlOptions returns the SQL configuration options from the mock queryable component.
func (m *mockQueryExec) SqlOptions() *driver.SqlOptions {
	return m.Q.SqlOptions()
}

// Exec executes a mock SQL statement that doesn't return rows using the mock executor.
func (m *mockQueryExec) Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error) {
	return m.E.Exec(ctx, sql, args...)
}
