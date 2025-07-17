package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/db/postgres"
	"github.com/xsqrty/op/orm"
)

func TestExec(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			count, err := orm.Count(op.Select().From(usersTable).Where(op.Ne("deleted_at", nil))).
				With(ctx, conn)
			require.NoError(t, err)
			require.Greater(t, count, int64(0))

			event, err := orm.Exec(op.Delete(usersTable).Where(op.Ne("deleted_at", nil))).
				With(ctx, conn)
			require.NoError(t, err)

			rowsCount, err := event.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, count, rowsCount)

			count, err = orm.Count(op.Select().From(usersTable).Where(op.Ne("deleted_at", nil))).
				With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, int64(0), count)

			name := gofakeit.Name()
			email := gofakeit.Email()
			createdAt := time.Now()

			event, err = orm.Exec(op.Insert(usersTable, op.Inserting{
				"id":         seed.Users[len(seed.Users)-1].ID + 1,
				"name":       name,
				"email":      email,
				"created_at": createdAt,
			})).With(ctx, conn)
			require.NoError(t, err)

			lastId, err := event.LastInsertId()
			require.Conditionf(t, func() bool {
				if errors.Is(err, postgres.ErrPgxUnsupported) {
					return true
				}

				return lastId > 0
			}, "last id condition error: %v, id: %d", err, lastId)

			if err == nil {
				user, err := orm.Query[MockUser](
					op.Select("created_at", "name", "email").
						From(usersTable).
						Where(op.Eq("id", lastId)),
				).GetOne(ctx, conn)
				require.NoError(t, err)
				require.Equal(t, email, user.Email)
				require.Equal(t, name, user.Name)
				require.Equal(t, createdAt.Unix(), user.CreatedAt.Unix())
			}

			lastId = int64(seed.Users[len(seed.Users)-1].ID + 1)
			event, err = orm.Exec(op.InsertMany(usersTable).Columns("id", "name", "email").Values(lastId+1, gofakeit.Name(), gofakeit.Email()).Values(lastId+2, gofakeit.Name(), gofakeit.Email())).
				With(ctx, conn)
			require.NoError(t, err)

			rowsCount, err = event.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(2), rowsCount)

			return errRollback
		}))
	})
}
