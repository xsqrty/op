package op

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xsqrty/op/internal/testutil"
	"testing"
)

func TestCountOf(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err := CountOf("users").By("id").Where(Eq("Name", "Alex")).With(context.Background(), query)
	assert.NoError(t, err)
	assert.Equal(t, uint64(55), count)

	query = testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT(DISTINCT "id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err = CountOf("users").ByDistinct("id").Where(Eq("Name", "Alex")).With(context.Background(), query)
	assert.NoError(t, err)
	assert.Equal(t, uint64(55), count)

	query = testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT(*)) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(nil, []any{uint64(55)}))

	count, err = CountOf("users").Where(Eq("Name", "Alex")).With(context.Background(), query)
	assert.NoError(t, err)
	assert.Equal(t, uint64(55), count)
}

func TestCountOfError(t *testing.T) {
	query := testutil.NewMockQueryable()
	query.
		On("QueryRow", mock.Anything, `SELECT (COUNT("id")) AS "total_count" FROM "users" WHERE "Name" = ? LIMIT ?`, []any{"Alex", uint64(1)}).
		Return(testutil.NewMockRow(errors.New("syntax error"), nil))

	count, err := CountOf("users").By("id").Where(Eq("Name", "Alex")).With(context.Background(), query)
	assert.Equal(t, uint64(0), count)
	assert.EqualError(t, err, "syntax error")
}
