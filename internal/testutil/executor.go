package testutil

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

type mockExecutor struct {
	mock.Mock
}

type mockExecResult struct {
	count     int64
	lastId    int64
	rowsError error
}

func NewMockExecutor() *mockExecutor {
	return &mockExecutor{}
}

func NewMockExecResult(count, lastId int64) *mockExecResult {
	return &mockExecResult{count: count, lastId: lastId}
}

func NewMockExecResultAffectedError(err error) *mockExecResult {
	return &mockExecResult{rowsError: err}
}

func (m *mockExecutor) Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(db.ExecResult), mockArgs.Error(1)
}

func (m *mockExecutor) SqlOptions() *driver.SqlOptions {
	return NewDefaultOptions()
}

func (er *mockExecResult) RowsAffected() (int64, error) {
	return er.count, er.rowsError
}

func (er *mockExecResult) LastInsertId() (int64, error) {
	return er.lastId, nil
}
