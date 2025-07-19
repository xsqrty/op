package testutil

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

// mockExecutor provides a mock implementation of a database executor for testing purposes.
type mockExecutor struct {
	mock.Mock
}

// mockExecResult is a mock implementation of a result returned by an Exec query in a database.
// It allows testing of RowsAffected and LastInsertId values.
// The type contains internal fields to simulate execution results and possible errors.
type mockExecResult struct {
	count     int64
	lastId    int64
	rowsError error
}

// NewMockExecutor creates and returns a new instance of mockExecutor for use in testing scenarios.
func NewMockExecutor() *mockExecutor {
	return &mockExecutor{}
}

// NewMockExecResult creates a new instance of mockExecResult with the specified count and lastId values.
func NewMockExecResult(count, lastId int64) *mockExecResult {
	return &mockExecResult{count: count, lastId: lastId}
}

// NewMockExecResultAffectedError creates a new mockExecResult with the provided error for the RowsAffected method.
func NewMockExecResultAffectedError(err error) *mockExecResult {
	return &mockExecResult{rowsError: err}
}

// Exec executes a mock SQL statement that does not return rows, using the provided context, SQL string, and arguments.
func (m *mockExecutor) Exec(ctx context.Context, sql string, args ...any) (db.ExecResult, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(db.ExecResult), mockArgs.Error(1)
}

// SqlOptions returns the default SQL generation options configured for the mockExecutor.
func (m *mockExecutor) SqlOptions() *driver.SqlOptions {
	return NewDefaultOptions()
}

// RowsAffected returns the number of rows affected by the query and any error encountered during execution.
func (er *mockExecResult) RowsAffected() (int64, error) {
	return er.count, er.rowsError
}

// LastInsertId returns the last inserted ID as an int64 or an error if unavailable.
func (er *mockExecResult) LastInsertId() (int64, error) {
	return er.lastId, nil
}
