package integration

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"testing"
)

func TestExec(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	count, err := op.CountOf(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Greater(t, count, uint64(0))

	event, err := op.Exec(op.Delete(usersTable).Where(op.Ne("deleted_at", nil))).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, count, event.RowsAffected())

	count, err = op.CountOf(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}
