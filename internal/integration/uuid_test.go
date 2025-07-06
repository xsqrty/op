package integration

import (
	"context"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
	"testing"
)

func TestUUID(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, conn.Transact(ctx, func(ctx context.Context) error {
			id := uuid.Must(uuid.NewV7())
			label := gofakeit.Name()

			err := orm.Put(labelsTable, &MockLabel{
				ID:    id,
				Label: label,
			}).With(ctx, conn)

			require.NoError(t, err)
			l, err := orm.Query[MockLabel](op.Select().From(labelsTable).Where(op.Eq("ID", id))).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, id, l.ID)
			require.Equal(t, label, l.Label)

			return errRollback
		}))
	})
}
