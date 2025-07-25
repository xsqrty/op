package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
)

func TestTransact_Commit(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		name := gofakeit.UUID()
		require.NoError(t, conn.Transact(ctx, func(ctx context.Context) error {
			err := orm.Put(countriesTable, &MockCountry{
				Name: name,
			}).With(ctx, conn)

			return err
		}))

		count, err := orm.Count(op.Select().From(countriesTable).Where(op.Eq("name", name))).
			With(ctx, conn)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})
}

func TestTransact_Rollback(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		name := gofakeit.UUID()
		err := conn.Transact(ctx, func(ctx context.Context) error {
			err := orm.Put(countriesTable, &MockCountry{
				Name: name,
			}).With(ctx, conn)
			if err != nil {
				return err
			}

			err = orm.Put(countriesTable, &MockCountry{
				Name: name,
			}).With(ctx, conn)

			return err
		})

		require.Condition(t, func() bool {
			if strings.Contains(err.Error(), "UNIQUE constraint failed") {
				return true
			}

			if strings.Contains(err.Error(), "duplicate key value violates unique constraint") {
				return true
			}

			return false
		})
		count, err := orm.Count(op.Select().From(countriesTable).Where(op.Eq("name", name))).
			With(ctx, conn)
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})
}
