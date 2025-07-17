package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
)

func TestNative(t *testing.T) {
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			sql, args, err := op.Select().
				From(usersTable).
				OrderBy(op.Asc("id")).
				Sql(conn.SqlOptions())
			require.NoError(t, err)

			rows, err := conn.Query(ctx, sql, args...)
			require.NoError(t, err)
			defer rows.Close()

			cols, err := rows.Columns()
			require.NoError(t, err)

			expected := []string{
				"id",
				"name",
				"email",
				"company_id",
				"created_at",
				"updated_at",
				"deleted_at",
			}
			require.Equal(t, expected, cols)

			for i, row := range rows.Rows() {
				var u MockUser

				require.NoError(
					t,
					row.Scan(
						&u.ID,
						&u.Name,
						&u.Email,
						&u.CompanyId,
						&u.CreatedAt,
						&u.UpdatedAt,
						&u.DeletedAt,
					),
				)
				require.Equal(t, seed.Users[i].ID, u.ID)
				require.Equal(t, seed.Users[i].Name, u.Name)
				require.Equal(t, seed.Users[i].Email, u.Email)
			}

			return errRollback
		}))
	})
}
