package integration

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xsqrty/op"
	"github.com/xsqrty/op/driver"
	"log"
	"testing"
	"time"
)

const (
	postgresUser = "postgres"
	postgresPass = "postgres"
	postgresDB   = "integration_test"
	usersTable   = "users"
)

type User struct {
	ID        int             `op:"id,primary"`
	Name      string          `op:"name"`
	Email     string          `op:"email"`
	CreatedAt time.Time       `op:"created_at"`
	UpdatedAt driver.ZeroTime `op:"updated_at"`
	DeletedAt sql.NullTime    `op:"deleted_at"`
}

var mockUsers []*User

var pool *pgxpool.Pool
var ctx = context.Background()

func TestMain(m *testing.M) {
	dsn, cleanup, err := StartContainer(ctx)
	defer cleanup()
	if err != nil {
		log.Fatalf("failed to start container: %v", err)
	}

	pool, err = ConnectDB(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	err = CreateTables(ctx, pool)
	if err != nil {
		log.Fatalf("failed to create postgres tables: %v", err)
	}

	for i := 0; i < 100; i++ {
		mockUsers = append(mockUsers, &User{
			Name:      gofakeit.Name(),
			Email:     gofakeit.Email(),
			CreatedAt: gofakeit.Date(),
			DeletedAt: sql.NullTime{Valid: i > 30 && i < 50, Time: gofakeit.Date()},
		})
	}

	m.Run()
}

func CreateTables(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		create table if not exists "%s" (
		    id serial PRIMARY KEY,
		    name text not null,
		    email text not null,
		    created_at timestamptz not null default now(),
		    updated_at timestamptz,
		    deleted_at timestamptz
		)
	`, usersTable))

	return err
}

func DataSeed(ctx context.Context, qe driver.QueryExec) error {
	for _, user := range mockUsers {
		user.CreatedAt = time.Now()
		err := op.Put[User](usersTable, user).With(ctx, qe)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetQueryExec(ctx context.Context) (driver.QueryExec, func(), error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, nil, err
	}

	return driver.NewPostgresDriver(tx), func() {
		tx.Rollback(ctx)
	}, nil
}

func ConnectDB(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	err = pool.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func StartContainer(ctx context.Context) (string, func(), error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_PASSWORD": postgresPass,
			"POSTGRES_DB":       postgresDB,
		},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return "", nil, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", nil, err
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		return "", nil, err
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresUser, postgresPass, host, port.Port(), postgresDB)
	return dsn, func() {
		container.Terminate(ctx)
	}, nil
}
