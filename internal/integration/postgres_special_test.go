package integration

import (
	"context"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/orm"
)

func TestPostgresSpecial(t *testing.T) {
	t.Parallel()
	require.Equal(t, errRollback, pgConn.Transact(ctx, func(ctx context.Context) error {
		id := uuid.Must(uuid.NewV7())
		roles := []string{gofakeit.Name(), gofakeit.Name(), gofakeit.Name(), gofakeit.Name()}
		data := MockPostgresData{
			Name: gofakeit.Name(),
			Age:  gofakeit.Int(),
		}

		err := orm.Put(pgSpecialTable, &MockPostgres{
			ID:    id,
			Roles: roles,
			Data:  data,
		}).With(ctx, pgConn)

		require.NoError(t, err)
		pu, err := orm.Query[MockPostgres](
			op.Select().From(pgSpecialTable).Where(op.Eq("ID", id)),
		).GetOne(ctx, pgConn)
		require.NoError(t, err)
		require.Equal(t, id, pu.ID)
		require.Equal(t, roles, pu.Roles)
		require.Equal(t, data, pu.Data)

		return errRollback
	}))
}
