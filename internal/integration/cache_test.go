package integration

import (
	"context"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/cache"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
	"testing"
)

func TestCache(t *testing.T) {
	t.Parallel()

	getLabelById := orm.NewReturnableCache(op.Select().From(labelsTable).Where(op.Eq("ID", cache.Arg("id"))))
	countLabelById := orm.NewReturnableCache(op.Select().From(labelsTable).Where(op.Eq("ID", cache.Arg("id"))))
	delLabelById := orm.NewReturnableCache(op.Delete(labelsTable).Where(op.Eq("ID", cache.Arg("id"))))

	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, conn.Transact(ctx, func(ctx context.Context) error {
			for i := 0; i < 2; i++ {
				id := uuid.Must(uuid.NewV7())
				label := gofakeit.Name()

				err := orm.Put(labelsTable, &MockLabel{
					ID:    id,
					Label: label,
				}).With(ctx, conn)

				require.NoError(t, err)

				l, err := orm.Query[MockLabel](getLabelById.Use(cache.Args{
					"id": id,
				})).GetOne(ctx, conn)

				require.NoError(t, err)
				require.Equal(t, id, l.ID)
				require.Equal(t, label, l.Label)

				count, err := orm.Count(countLabelById.Use(cache.Args{
					"id": id,
				})).With(ctx, conn)

				require.NoError(t, err)
				require.Equal(t, uint64(1), count)

				count, err = orm.Count(delLabelById.Use(cache.Args{
					"id": id,
				})).With(ctx, conn)

				require.NoError(t, err)
				require.Equal(t, uint64(1), count)

				count, err = orm.Count(countLabelById.Use(cache.Args{
					"id": id,
				})).With(ctx, conn)

				require.NoError(t, err)
				require.Equal(t, uint64(0), count)
			}

			return errRollback
		}))
	})
}
