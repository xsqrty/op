package testutil

import (
	"context"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

type mockQueryExec struct {
	Q *mockQueryable
	E *mockExecutor
}

func NewMockQueryExec() *mockQueryExec {
	return &mockQueryExec{
		Q: NewMockQueryable(),
		E: NewMockExecutor(),
	}
}

func (m *mockQueryExec) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	return m.Q.Query(ctx, sql, args...)
}

func (m *mockQueryExec) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	return m.Q.QueryRow(ctx, sql, args...)
}

func (m *mockQueryExec) SqlOptions() *driver.SqlOptions {
	return m.Q.SqlOptions()
}

func (m *mockQueryExec) Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error) {
	return m.E.Exec(ctx, sql, args...)
}
