package integration

import (
	"context"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
)

func TestPut(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			updatedAt := gofakeit.Date()
			createdAt := gofakeit.Date()

			u := seed.Users[len(seed.Users)-1]
			u.Name = "Rename user"
			u.CreatedAt = createdAt
			u.UpdatedAt = driver.ZeroTime(updatedAt)

			err = orm.Put(usersTable, u).With(ctx, conn)
			require.NoError(t, err)

			fromDb, err := orm.Query[MockUser](
				op.Select().From(usersTable).Where(op.Eq("name", "Rename user")),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, u, fromDb)
			require.Equal(t, createdAt.UnixMilli(), fromDb.CreatedAt.UnixMilli())
			require.Equal(t, updatedAt.UnixMilli(), time.Time(fromDb.UpdatedAt).UnixMilli())

			return errRollback
		}))
	})
}

func TestPutIds(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			id := 1000

			companyNoId := &MockCompany{Name: gofakeit.Name()}
			err := orm.Put(companiesTable, companyNoId).With(ctx, conn)

			require.NoError(t, err)
			require.NotEmpty(t, companyNoId)

			companyWithId := &MockCompany{Name: gofakeit.Name(), ID: id}
			err = orm.Put(companiesTable, companyWithId).With(ctx, conn)

			require.NoError(t, err)
			require.Equal(t, id, companyWithId.ID)

			comp1, err := orm.Query[MockCompany](
				op.Select().From(companiesTable).Where(op.Eq("id", id)),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, companyWithId.ID, comp1.ID)
			require.Equal(t, companyWithId.Name, comp1.Name)

			comp2, err := orm.Query[MockCompany](
				op.Select().From(companiesTable).Where(op.Eq("id", companyNoId.ID)),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, companyNoId.ID, comp2.ID)
			require.Equal(t, companyNoId.Name, comp2.Name)

			return errRollback
		}))
	})
}
