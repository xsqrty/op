package integration

import (
	"database/sql"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
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
	qe, rollback, err := GetQueryExec(ctx)
	defer rollback()
	require.NoError(t, err)

	err = DataSeed(ctx, qe)
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

	var req op.PaginateRequest
	err = json.Unmarshal([]byte(reqString), &req)
	assert.NoError(t, err)

	res, err := op.Paginate[PaginateMockUser](usersTable, &req).
		WhiteList("user_id", "user_deleted_at").
		Fields(
			op.As("user_id", op.Column("users.id")),
			op.As("user_name", op.Column("users.name")),
			op.As("user_company_id", op.Column("users.company_id")),
			op.As("user_company_name", op.Column("companies.name")),
			op.As("user_deleted_at", op.Column("users.deleted_at")),
		).
		LeftJoin(companiesTable, op.Eq("company_id", op.Column("companies.id"))).
		With(ctx, qe)

	deletedCount := 0
	for _, user := range mockUsers {
		if user.DeletedAt.Valid {
			deletedCount++
		}
	}

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, uint64(deletedCount), res.TotalRows)

	if res.TotalRows > 0 {
		for _, row := range res.Rows {
			if row.CompanyName != "" {
				company := mockCompanies[int(row.CompanyId)-1]
				assert.Equal(t, company.ID, int(row.CompanyId))
				assert.Equal(t, company.Name, string(row.CompanyName))
			}

			assert.NotEqual(t, 0, row.ID)
			assert.NotEqual(t, "", row.Name)
			assert.Condition(t, func() bool {
				return row.DeletedAt.Valid
			})
		}
	}

	startId := mockUsers[0].ID
	users := make([]*MockUser, len(mockUsers))
	copy(users, mockUsers)

	slices.SortFunc(users, func(u1 *MockUser, u2 *MockUser) int {
		return u2.ID - u1.ID
	})

	assert.Equal(t, startId, users[len(users)-1].ID)
	var filtered []*MockUser
	for _, u := range users {
		if u.DeletedAt.Valid {
			filtered = append(filtered, u)
		}
	}

	prepared := filtered[2:4]
	for i, u := range res.Rows {
		assert.Equal(t, prepared[i].ID, u.ID)
	}
}
