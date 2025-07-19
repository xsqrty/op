package testutil

import (
	"context"
	"iter"
	"reflect"

	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
)

// mockQueryable represents a mock implementation of a queryable interface for testing database query behavior.
type mockQueryable struct {
	mock.Mock
}

// mockRows is a struct representing a mock implementation of database rows, used for testing purposes.
// It embeds mock.Mock to facilitate method call tracking and mocking behavior in tests.
// rows is a slice containing db.Scanner instances representing the mocked rows.
// err is an optional error returned in mocked scenarios to simulate error conditions.
type mockRows struct {
	mock.Mock
	rows []db.Scanner
	err  error
}

// mockRow represents a mock implementation of a database row, often used in testing environments.
// It contains row data and an error that can be returned during scanning operations.
type mockRow struct {
	row []any
	err error
}

// NewMockRows creates a mockRows instance with the provided error and slice of db.Scanner for mocking database row handling.
func NewMockRows(err error, rows []db.Scanner) *mockRows {
	return &mockRows{
		err:  err,
		rows: rows,
	}
}

// NewMockRow creates a mockRow instance with the specified error and row data for testing purposes.
func NewMockRow(err error, row []any) *mockRow {
	return &mockRow{
		row: row,
		err: err,
	}
}

// NewMockQueryable creates a new instance of mockQueryable for mocking database query operations in tests.
func NewMockQueryable() *mockQueryable {
	return &mockQueryable{}
}

// Query executes a mock SQL query using the provided context, SQL statement, and arguments, returning mock Rows and an error.
func (m *mockQueryable) Query(ctx context.Context, sql string, args ...any) (db.Rows, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(db.Rows), mockArgs.Error(1)
}

// QueryRow executes a mock SQL query expecting to return a single row result, using the given context, SQL, and arguments.
func (m *mockQueryable) QueryRow(ctx context.Context, sql string, args ...any) db.Row {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(db.Row)
}

// SqlOptions returns the default SQL generation configuration options for the mockQueryable instance.
func (m *mockQueryable) SqlOptions() *driver.SqlOptions {
	return NewDefaultOptions()
}

// Rows returns an iterator sequence of index and db.Scanner for each row in the mockRows.
func (mr *mockRows) Rows() iter.Seq2[int, db.Scanner] {
	return func(yield func(int, db.Scanner) bool) {
		for i := range mr.rows {
			if !yield(i, mr.rows[i]) {
				break
			}
		}
	}
}

// Scan copies the values of the mockRow into the provided destinations, returning an error if one exists in the mockRow.
func (ms *mockRow) Scan(dest ...any) error {
	if ms.err != nil {
		return ms.err
	}

	for i := range dest {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(ms.row[i]))
	}

	return nil
}

// Close releases any resources associated with the mockRows and should be called when the rows are no longer needed.
func (mr *mockRows) Close() {}

// Columns returns the column names of the current result set. It returns an error if the operation fails.
func (mr *mockRows) Columns() ([]string, error) {
	return nil, nil
}

// Err returns the error, if any, that was encountered during iteration over mock rows.
func (mr *mockRows) Err() error {
	return mr.err
}
