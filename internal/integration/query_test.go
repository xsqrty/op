package integration

import (
	"context"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			users, err := orm.Query[MockUser](
				op.Select().From(usersTable).OrderBy(op.Asc("id")),
			).GetMany(ctx, conn)
			require.NoError(t, err)
			require.Len(t, users, len(seed.Users))

			for index, user := range users {
				require.Equal(t, seed.Users[index].ID, user.ID)
				require.Equal(t, seed.Users[index].Name, user.Name)
				require.Equal(t, seed.Users[index].Email, user.Email)
			}

			deletedUsersCount := 0
			for _, user := range seed.Users {
				if user.DeletedAt.Valid {
					deletedUsersCount++
				}
			}

			users, err = orm.Query[MockUser](
				op.Select().From(usersTable).Where(op.Ne("deleted_at", nil)),
			).GetMany(ctx, conn)
			require.NoError(t, err)
			require.Len(t, users, deletedUsersCount)

			users, err = orm.Query[MockUser](
				op.Select().From(usersTable).Where(op.Eq("id", -1)),
			).GetMany(ctx, conn)
			require.NoError(t, err)
			require.Len(t, users, 0)

			return errRollback
		}))
	})
}

func TestQueryOne(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			user, err := orm.Query[MockUser](
				op.Select().From(usersTable).Where(op.Eq("id", seed.Users[0].ID)),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, seed.Users[0].ID, user.ID)

			user, err = orm.Query[MockUser](
				op.Select().From(usersTable).Where(op.Eq("id", -1)),
			).GetOne(ctx, conn)
			require.Nil(t, user)
			require.Contains(t, err.Error(), "no rows in result set")

			return errRollback
		}))
	})
}

func TestQueryDelete(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			usersCount, err := orm.Count(op.Select().From(usersTable)).With(ctx, conn)
			require.NoError(t, err)

			deletedUsers, err := orm.Query[MockUser](op.Delete(usersTable).Where(op.And{
				op.In("id", seed.Users[0].ID, seed.Users[1].ID, seed.Users[2].ID),
				op.Eq("deleted_at", nil),
			})).GetMany(ctx, conn)

			require.NoError(t, err)
			require.Len(t, deletedUsers, 3)

			for i, user := range deletedUsers {
				require.Equal(t, seed.Users[i].ID, user.ID)
				require.Equal(t, seed.Users[i].Name, user.Name)
				require.Equal(t, seed.Users[i].Email, user.Email)
			}

			newUsersCount, err := orm.Count(op.Select().From(usersTable)).With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, usersCount-3, newUsersCount)

			return errRollback
		}))
	})
}

func TestQueryInsert(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			usersCount, err := orm.Count(op.Select().From(usersTable)).With(ctx, conn)
			require.NoError(t, err)

			data := op.Inserting{
				"id":         seed.Users[len(seed.Users)-1].ID + 1,
				"name":       gofakeit.Name(),
				"email":      gofakeit.Email(),
				"created_at": gofakeit.Date(),
			}

			inserted, err := orm.Query[MockUser](op.Insert(usersTable, data)).GetOne(ctx, conn)
			require.NoError(t, err)

			require.Equal(t, data["name"], inserted.Name)
			require.Equal(t, data["email"], inserted.Email)
			require.Equal(
				t,
				data["created_at"].(time.Time).UnixMilli(),
				inserted.CreatedAt.UnixMilli(),
			)

			newUsersCount, err := orm.Count(op.Select().From(usersTable)).With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, usersCount+1, newUsersCount)

			return errRollback
		}))
	})
}

func TestQueryUpdate(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			_, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			deletedCount, err := orm.Count(op.Select().From(usersTable).Where(op.Ne("deleted_at", nil))).
				With(ctx, conn)
			require.NoError(t, err)
			require.Greater(t, deletedCount, int64(0))

			_, err = orm.Query[MockUser](op.Update(usersTable, op.Updates{
				"deleted_at": nil,
			}).Where(op.Ne("deleted_at", nil))).GetMany(ctx, conn)
			require.NoError(t, err)

			newDeletedCount, err := orm.Count(op.Select().From(usersTable).Where(op.Ne("deleted_at", nil))).
				With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, int64(0), newDeletedCount)

			return errRollback
		}))
	})
}
