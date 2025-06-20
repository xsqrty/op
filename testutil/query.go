package testutil

import (
	"context"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/driver"
	"iter"
	"reflect"
)

type mockQueryable struct {
	mock.Mock
}

type mockRows struct {
	mock.Mock
	rows []driver.Scanner
	err  error
}

type mockRow struct {
	row []any
	err error
}

func NewMockRows(err error, rows []driver.Scanner) *mockRows {
	return &mockRows{
		err:  err,
		rows: rows,
	}
}

func NewMockRow(err error, row []any) *mockRow {
	return &mockRow{
		row: row,
		err: err,
	}
}

func NewMockQueryable() *mockQueryable {
	return &mockQueryable{}
}

func (m *mockQueryable) Query(ctx context.Context, sql string, args ...any) (driver.Rows, error) {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(driver.Rows), mockArgs.Error(1)
}

func (m *mockQueryable) QueryRow(ctx context.Context, sql string, args ...any) driver.Row {
	mockArgs := m.Called(ctx, sql, args)
	return mockArgs.Get(0).(driver.Row)
}

func (m *mockQueryable) Sql(sqler driver.Sqler) (string, []any, error) {
	return driver.Sql(sqler, NewDefaultOptions())
}

func (mr *mockRows) Rows() iter.Seq2[int, driver.Scanner] {
	return func(yield func(int, driver.Scanner) bool) {
		for i, _ := range mr.rows {
			if !yield(i, mr.rows[i]) {
				break
			}
		}
	}
}

func (ms *mockRow) Scan(dest ...any) error {
	if ms.err != nil {
		return ms.err
	}

	for i := range dest {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(ms.row[i]))
	}

	return nil
}

func (mr *mockRows) Close() {}

func (mr *mockRows) Err() error {
	return mr.err
}
