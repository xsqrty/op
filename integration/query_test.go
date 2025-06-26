package integration

import (
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	users, err := op.Query[MockUser](op.Select().From(usersTable).OrderBy(op.Asc("id"))).GetMany(ctx, qe)
	assert.NoError(t, err)
	assert.Len(t, users, len(mockUsers))

	for index, user := range users {
		assert.Equal(t, mockUsers[index].ID, user.ID)
		assert.Equal(t, mockUsers[index].Name, user.Name)
		assert.Equal(t, mockUsers[index].Email, user.Email)
	}

	deletedUsersCount := 0
	for _, user := range mockUsers {
		if user.DeletedAt.Valid {
			deletedUsersCount++
		}
	}

	users, err = op.Query[MockUser](op.Select().From(usersTable).Where(op.Ne("deleted_at", nil))).GetMany(ctx, qe)
	assert.NoError(t, err)
	assert.Len(t, users, deletedUsersCount)
}

func TestQueryOne(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	user, err := op.Query[MockUser](op.Select().From(usersTable).Where(op.Eq("id", 1))).GetOne(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, mockUsers[0].ID, user.ID)
}

func TestQueryDelete(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	usersCount, err := op.CountOf(usersTable).With(ctx, qe)
	assert.NoError(t, err)

	deletedUsers, err := op.Query[MockUser](op.Delete(usersTable).Where(op.And{
		op.In("id", mockUsers[0].ID, mockUsers[1].ID, mockUsers[2].ID),
		op.Eq("deleted_at", nil),
	})).GetMany(ctx, qe)

	assert.NoError(t, err)
	assert.Len(t, deletedUsers, 3)

	for i, user := range deletedUsers {
		assert.Equal(t, mockUsers[i].ID, user.ID)
		assert.Equal(t, mockUsers[i].Name, user.Name)
		assert.Equal(t, mockUsers[i].Email, user.Email)
	}

	newUsersCount, err := op.CountOf(usersTable).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, usersCount-3, newUsersCount)
}

func TestQueryInsert(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	usersCount, err := op.CountOf(usersTable).With(ctx, qe)
	assert.NoError(t, err)

	data := op.Inserting{
		"name":       gofakeit.Name(),
		"email":      gofakeit.Email(),
		"created_at": gofakeit.Date(),
	}

	inserted, err := op.Query[MockUser](op.Insert(usersTable, data)).GetOne(ctx, qe)
	assert.NoError(t, err)

	assert.Equal(t, data["name"], inserted.Name)
	assert.Equal(t, data["email"], inserted.Email)
	assert.Equal(t, data["created_at"].(time.Time).UnixMilli(), inserted.CreatedAt.UnixMilli())

	newUsersCount, err := op.CountOf(usersTable).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, usersCount+1, newUsersCount)
}

func TestQueryUpdate(t *testing.T) {
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
	require.NoError(t, err)

	deletedCount, err := op.CountOf(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Greater(t, deletedCount, uint64(0))

	_, err = op.Query[MockUser](op.Update(usersTable, op.Updates{
		"deleted_at": nil,
	}).Where(op.Ne("deleted_at", nil))).GetMany(ctx, qe)
	assert.NoError(t, err)

	newDeletedCount, err := op.CountOf(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), newDeletedCount)
}
