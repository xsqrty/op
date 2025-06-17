package testutil

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/driver"
)

type mockExecutor struct {
	mock.Mock
}

type mockExecResult struct {
	count int64
}

func NewMockExecutor() *mockExecutor {
	return &mockExecutor{}
}

func NewMockExecResult(count int64) *mockExecResult {
	return &mockExecResult{count: count}
}

func (m *mockExecutor) Exec(ctx context.Context, sql string, args ...any) (driver.ExecResult, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(driver.ExecResult), mockArgs.Error(1)
}

func (m *mockExecutor) Sql(sqler driver.Sqler) (string, []any, error) {
	return driver.Sql(sqler, NewDefaultOptions())
}

func (er *mockExecResult) RowsAffected() int64 {
	return er.count
}
