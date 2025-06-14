package op

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDelete(t *testing.T) {
	runCases(t, []testCase{
		{
			builder:      Delete("users").Where(Ne("DeletedAt", nil)),
			expectedSql:  `DELETE FROM "users" WHERE "DeletedAt" IS NOT NULL`,
			expectedArgs: []any(nil),
		},
		{
			builder:      Delete("users").Where(Ne("Id", 1)).Returning("Id", "Name"),
			expectedSql:  `DELETE FROM "users" WHERE "Id" != ? RETURNING "Id","Name"`,
			expectedArgs: []any{1},
		},
	})
}

func TestDeleteUsingTables(t *testing.T) {
	item := Delete("users").Where(Ne("DeletedAt", nil))
	tables := item.UsingTables()

	assert.Equal(t, []string{"users"}, tables)
}
