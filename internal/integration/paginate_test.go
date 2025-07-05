package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
	"slices"
	"testing"
)

type PaginateMockUser struct {
	Name        string            `op:"user_name"`
	ID          int               `op:"user_id"`
	CompanyId   driver.ZeroInt64  `op:"user_company_id"`
	CompanyName driver.ZeroString `op:"user_company_name"`
	DeletedAt   sql.NullTime      `op:"user_deleted_at"`
}

func TestPaginate(t *testing.T) {
	t.Parallel()
	EachConn(t, func(conn db.ConnPool) {
		require.Equal(t, errRollback, Transact(t, ctx, conn, func(ctx context.Context) error {
			seed, err := DataSeed(ctx, conn)
			require.NoError(t, err)

			reqString := `{
			"offset": 2,
			"limit": 2,
			"filters": {
					"user_deleted_at": {"$ne": null}
			},
			"orders": [
					{
							"key": "user_id",
							"desc": true
					}
			]
	}`

			var req orm.PaginateRequest
			err = json.Unmarshal([]byte(reqString), &req)
			require.NoError(t, err)

			res, err := orm.Paginate[PaginateMockUser](usersTable, &req).
				WhiteList("user_id", "user_deleted_at").
				Fields(
					op.As("user_id", op.Column("users.id")),
					op.As("user_name", op.Column("users.name")),
					op.As("user_company_id", op.Column("users.company_id")),
					op.As("user_company_name", op.Column("companies.name")),
					op.As("user_deleted_at", op.Column("users.deleted_at")),
				).
				LeftJoin(companiesTable, op.Eq("company_id", op.Column("companies.id"))).
				With(ctx, conn)

			deletedCount := 0
			for _, user := range seed.Users {
				if user.DeletedAt.Valid {
					deletedCount++
				}
			}

			require.NoError(t, err)
			require.NotNil(t, res)
			require.Equal(t, uint64(deletedCount), res.TotalRows)

			if res.TotalRows > 0 {
				for _, row := range res.Rows {
					if row.CompanyName != "" {
						company := seed.Companies[int(row.CompanyId)-1]
						require.Equal(t, company.ID, int(row.CompanyId))
						require.Equal(t, company.Name, string(row.CompanyName))
					}

					require.NotEqual(t, 0, row.ID)
					require.NotEqual(t, "", row.Name)
					require.Condition(t, func() bool {
						return row.DeletedAt.Valid
					})
				}
			}

			startId := seed.Users[0].ID
			users := make([]*MockUser, len(seed.Users))
			copy(users, seed.Users)

			slices.SortFunc(users, func(u1 *MockUser, u2 *MockUser) int {
				return u2.ID - u1.ID
			})

			require.Equal(t, startId, users[len(users)-1].ID)
			var filtered []*MockUser
			for _, u := range users {
				if u.DeletedAt.Valid {
					filtered = append(filtered, u)
				}
			}

			prepared := filtered[2:4]
			for i, u := range res.Rows {
				require.Equal(t, prepared[i].ID, u.ID)
			}

			return errRollback
		}))
	})
}
