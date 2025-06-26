package integration

import (
	"context"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/assert"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/tx"
	"testing"
)

func TestPostgresTxProvider(t *testing.T) {
	provider := tx.NewPostgresProvider(pool)
	qe := driver.NewPostgresDriver(provider)
	name := gofakeit.UUID()

	assert.NoError(t, provider.Do(context.Background(), func(ctx context.Context) error {
		err := op.Put[MockCountry](countriesTable, &MockCountry{
			Name: name,
		}).With(ctx, qe)

		return err
	}))

	count, err := op.CountOf(countriesTable).Where(op.Eq("name", name)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count)
}

func TestPostgresTxProvider_Rollback(t *testing.T) {
	provider := tx.NewPostgresProvider(pool)
	qe := driver.NewPostgresDriver(provider)
	name := gofakeit.UUID()

	err := provider.Do(context.Background(), func(ctx context.Context) error {
		err := op.Put[MockCountry](countriesTable, &MockCountry{
			Name: name,
		}).With(ctx, qe)

		if err != nil {
			return err
		}

		err = op.Put[MockCountry](countriesTable, &MockCountry{
			Name: name,
		}).With(ctx, qe)

		return err
	})

	assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
	count, err := op.CountOf(countriesTable).Where(op.Eq("name", name)).With(ctx, qe)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
}
