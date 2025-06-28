package integration

import (
	"context"
	"errors"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
	"testing"
	"time"
)

func TestExec(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			count, err := orm.Count(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, conn)
			assert.NoError(t, err)
			assert.Greater(t, count, uint64(0))

			event, err := orm.Exec(op.Delete(usersTable).Where(op.Ne("deleted_at", nil))).With(ctx, conn)
			assert.NoError(t, err)

			rowsCount, err := event.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, count, uint64(rowsCount))

			count, err = orm.Count(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, conn)
			assert.NoError(t, err)
			assert.Equal(t, uint64(0), count)

			name := gofakeit.Name()
			email := gofakeit.Email()
			createdAt := time.Now()

			event, err = orm.Exec(op.Insert(usersTable, op.Inserting{
				"name":       name,
				"email":      email,
				"created_at": createdAt,
			})).With(ctx, conn)

			lastId, err := event.LastInsertId()
			assert.Conditionf(t, func() bool {
				if errors.Is(err, db.ErrPgxUnsupported) {
					return true
				}

				return lastId > 0
			}, "last id condition error: %v, id: %d", err, lastId)

			if err == nil {
				user, err := orm.Query[MockUser](op.Select("created_at", "name", "email").From(usersTable).Where(op.Eq("id", lastId))).GetOne(ctx, conn)
				assert.NoError(t, err)
				assert.Equal(t, email, user.Email)
				assert.Equal(t, name, user.Name)
				assert.Equal(t, createdAt.Unix(), user.CreatedAt.Unix())
			}

			event, err = orm.Exec(op.InsertMany(usersTable).Columns("name", "email").Values(gofakeit.Name(), gofakeit.Email()).Values(gofakeit.Name(), gofakeit.Email())).With(ctx, conn)
			assert.NoError(t, err)

			rowsCount, err = event.RowsAffected()
			assert.NoError(t, err)
			assert.Equal(t, int64(2), rowsCount)

			return errRollback
		}))
	})
}
