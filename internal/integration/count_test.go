package integration

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/orm"
	"testing"
)

func TestCount(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			err := DataSeed(ctx, conn)
			require.NoError(t, err)

			count, err := orm.Count(usersTable).With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, len(mockUsers), int(count))

			company := mockCompanies[0]
			companyCount := 0
			deletedCount := 0
			for _, user := range mockUsers {
				if user.CompanyId.Int64 == int64(company.ID) {
					companyCount++
				}

				if user.DeletedAt.Valid {
					deletedCount++
				}
			}

			count, err = orm.Count(usersTable).Where(op.Ne("deleted_at", nil)).With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, deletedCount, int(count))

			count, err = orm.Count(usersTable).LeftJoin(companiesTable, op.Eq("companies.id", op.Column("users.company_id"))).Where(op.Eq("companies.name", company.Name)).With(ctx, conn)
			require.NoError(t, err)
			require.Equal(t, companyCount, int(count))

			return errRollback
		}))
	})
}
