package integration

import (
	"context"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			users, err := orm.Query[MockUser](op.Select().From(usersTable).OrderBy(op.Asc("id"))).GetMany(ctx, conn)
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

			users, err = orm.Query[MockUser](op.Select().From(usersTable).Where(op.Ne("deleted_at", nil))).GetMany(ctx, conn)
			assert.NoError(t, err)
			assert.Len(t, users, deletedUsersCount)

			users, err = orm.Query[MockUser](op.Select().From(usersTable).Where(op.Eq("id", -1))).GetMany(ctx, conn)
			assert.NoError(t, err)
			assert.Len(t, users, 0)

			return errRollback
		}))
	})
}

func TestQueryOne(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			user, err := orm.Query[MockUser](op.Select().From(usersTable).Where(op.Eq("id", 1))).GetOne(ctx, conn)
			assert.NoError(t, err)
			assert.Equal(t, mockUsers[0].ID, user.ID)

			user, err = orm.Query[MockUser](op.Select().From(usersTable).Where(op.Eq("id", -1))).GetOne(ctx, conn)
			assert.Nil(t, user)
			assert.Contains(t, err.Error(), "no rows in result set")

			return errRollback
		}))
	})
}

func TestQueryDelete(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			usersCount, err := orm.Count(usersTable).With(ctx, conn)
			assert.NoError(t, err)

			deletedUsers, err := orm.Query[MockUser](op.Delete(usersTable).Where(op.And{
				op.In("id", mockUsers[0].ID, mockUsers[1].ID, mockUsers[2].ID),
				op.Eq("deleted_at", nil),
			})).GetMany(ctx, conn)

			assert.NoError(t, err)
			assert.Len(t, deletedUsers, 3)

			for i, user := range deletedUsers {
				assert.Equal(t, mockUsers[i].ID, user.ID)
				assert.Equal(t, mockUsers[i].Name, user.Name)
				assert.Equal(t, mockUsers[i].Email, user.Email)
			}

			newUsersCount, err := orm.Count(usersTable).With(ctx, conn)
			assert.NoError(t, err)
			assert.Equal(t, usersCount-3, newUsersCount)

			return errRollback
		}))
	})
}

func TestQueryInsert(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			usersCount, err := orm.Count(usersTable).With(ctx, conn)
			assert.NoError(t, err)

			data := op.Inserting{
				"name":       gofakeit.Name(),
				"email":      gofakeit.Email(),
				"created_at": gofakeit.Date(),
			}

			inserted, err := orm.Query[MockUser](op.Insert(usersTable, data)).GetOne(ctx, conn)
			assert.NoError(t, err)

			assert.Equal(t, data["name"], inserted.Name)
			assert.Equal(t, data["email"], inserted.Email)
			assert.Equal(t, data["created_at"].(time.Time).UnixMilli(), inserted.CreatedAt.UnixMilli())

			newUsersCount, err := orm.Count(usersTable).With(ctx, conn)
			assert.NoError(t, err)
			assert.Equal(t, usersCount+1, newUsersCount)

			return errRollback
		}))
	})
}

func TestQueryUpdate(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			deletedCount, err := orm.Count(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, conn)
			assert.NoError(t, err)
			assert.Greater(t, deletedCount, uint64(0))

			_, err = orm.Query[MockUser](op.Update(usersTable, op.Updates{
				"deleted_at": nil,
			}).Where(op.Ne("deleted_at", nil))).GetMany(ctx, conn)
			assert.NoError(t, err)

			newDeletedCount, err := orm.Count(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, conn)
			assert.NoError(t, err)
			assert.Equal(t, uint64(0), newDeletedCount)

			return errRollback
		}))
	})
}
