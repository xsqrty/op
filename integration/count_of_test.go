package integration

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"testing"
)

func TestCountOf(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	count, err := op.CountOf(usersTable).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, len(mockUsers), int(count))

	deletedCount := 0
	for _, user := range mockUsers {
		if user.DeletedAt.Valid {
			deletedCount++
		}
	}

	count, err = op.CountOf(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, deletedCount, int(count))
}
