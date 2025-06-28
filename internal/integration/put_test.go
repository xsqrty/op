package integration

import (
	"context"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
	"testing"
	"time"
)

func TestPut(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		assert.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			updatedAt := gofakeit.Date()
			createdAt := gofakeit.Date()

			u := mockUsers[len(mockUsers)-1]
			u.Name = "Rename user"
			u.CreatedAt = createdAt
			u.UpdatedAt = driver.ZeroTime(updatedAt)

			err = orm.Put(usersTable, u).With(ctx, conn)
			assert.NoError(t, err)

			fromDb, err := orm.Query[MockUser](op.Select().From(usersTable).Where(op.Eq("name", "Rename user"))).GetOne(ctx, conn)
			assert.NoError(t, err)
			assert.Equal(t, u, fromDb)
			assert.Equal(t, createdAt.UnixMilli(), time.Time(fromDb.CreatedAt).UnixMilli())
			assert.Equal(t, updatedAt.UnixMilli(), time.Time(fromDb.UpdatedAt).UnixMilli())

			return errRollback
		}))
	})
}
