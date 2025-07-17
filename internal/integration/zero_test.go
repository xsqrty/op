package integration

import (
	"context"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
)

func TestZeroTime(t *testing.T) {
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
			l, err := orm.Query[MockLabel](
				op.Select().From(labelsTable).Where(op.Eq("ID", id)),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, id, l.ID)
			require.Equal(t, label, l.Label)
			require.True(t, l.DeletedAt.IsZero())

			l.Label = label + "_updated"
			require.NoError(t, orm.Put(labelsTable, l).With(ctx, conn))

			l, err = orm.Query[MockLabel](
				op.Select().
					From(labelsTable).
					Where(op.Eq("ID", id)).
					Where(op.Or{op.Eq("DeletedAt", nil), op.Eq("DeletedAt", time.Time{})}),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, id, l.ID)
			require.Equal(t, label+"_updated", l.Label)
			require.True(t, l.DeletedAt.IsZero())

			deletedAt := time.Now()
			l.Label = label + "_updated_2"
			l.DeletedAt = driver.ZeroTime(deletedAt)
			require.NoError(t, orm.Put(labelsTable, l).With(ctx, conn))

			l, err = orm.Query[MockLabel](
				op.Select().From(labelsTable).Where(op.Eq("ID", id)),
			).GetOne(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, id, l.ID)
			require.Equal(t, label+"_updated_2", l.Label)
			require.Equal(t, deletedAt.UnixMilli(), l.DeletedAt.Time().UnixMilli())

			return errRollback
		}))
	})
}
