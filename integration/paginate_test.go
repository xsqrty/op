package integration

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xsqrty/op"
	"slices"
	"testing"
)

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
					"filters": [
							{
									"op": "ne",
									"key": "deleted_at",
									"value": null
							}
					]
			},
			"orders": [
					{
							"key": "id",
							"desc": true
					}
			]
	}`

	var req op.PaginateRequest
	err = json.Unmarshal([]byte(reqString), &req)
	assert.NoError(t, err)

	res, err := op.Paginate[User](usersTable, &req, []string{"id", "deleted_at"}).
		Fields(
			op.As("id", op.Column("users.id")),
			op.As("name", op.Column("users.name")),
			op.As("deleted_at", op.Column("users.deleted_at")),
		).
		With(ctx, qe)

	deletedCount := 0
	for _, user := range mockUsers {
		if user.DeletedAt.Valid {
			deletedCount++
		}
	}

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, int64(deletedCount), res.TotalRows)

	startId := mockUsers[0].ID
	users := make([]*User, len(mockUsers))
	copy(users, mockUsers)

	slices.SortFunc(users, func(u1 *User, u2 *User) int {
		return u2.ID - u1.ID
	})

	assert.Equal(t, startId, users[len(users)-1].ID)
	var filtered []*User
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
