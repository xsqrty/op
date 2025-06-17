package integration

import (
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	t.Parallel()
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	updatedAt := gofakeit.Date()
	u := mockUsers[len(mockUsers)-1]
	u.Name = "Rename user"
	u.UpdatedAt = driver.ZeroTime(updatedAt)

	err = op.Put(usersTable, u).With(ctx, qe)
	assert.NoError(t, err)

	fromDb, err := op.Query[User](op.Select().From(usersTable).Where(op.Eq("name", "Rename user"))).GetOne(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, u, fromDb)
	assert.Equal(t, updatedAt.UnixMilli(), time.Time(fromDb.UpdatedAt).UnixMilli())
}
