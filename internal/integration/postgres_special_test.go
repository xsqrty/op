package integration

import (
	"context"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/orm"
	"testing"
)

func TestPostgresSpecial(t *testing.T) {
	assert.Equal(t, errRollback, pgConn.Transact(ctx, func(ctx context.Context) error {
		id := uuid.Must(uuid.NewV7())
		roles := []string{gofakeit.Name(), gofakeit.Name(), gofakeit.Name(), gofakeit.Name()}
		data := MockPostgresData{
			Name: gofakeit.Name(),
			Age:  gofakeit.Int(),
		}

		err := orm.Put[MockPostgres](pgSpecialTable, &MockPostgres{
			ID:    id,
			Roles: roles,
			Data:  data,
		}).With(ctx, pgConn)

		assert.NoError(t, err)
		pu, err := orm.Query[MockPostgres](op.Select().From(pgSpecialTable).Where(op.Eq("ID", id))).GetOne(ctx, pgConn)
		assert.NoError(t, err)
		assert.Equal(t, id, pu.ID)
		assert.Equal(t, roles, pu.Roles)
		assert.Equal(t, data, pu.Data)

		return errRollback
	}))
}
