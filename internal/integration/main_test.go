package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/db/postgres"
	"github.com/xsqrty/op/db/sqlite"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
)

const (
	sqliteDSN      = "storage.db"
	postgresUser   = "postgres"
	postgresPass   = "postgres"
	postgresDB     = "integration_test"
	companiesTable = "companies"
	usersTable     = "users"
	countriesTable = "countries"
	labelsTable    = "LabelsCamel"
	pgSpecialTable = "PgSpecial"
)

type MockUser struct {
	ID        int             `op:"id,primary"`
	Name      string          `op:"name"`
	Email     string          `op:"email"`
	CreatedAt time.Time       `op:"created_at"`
	CompanyId sql.NullInt64   `op:"company_id"`
	UpdatedAt driver.ZeroTime `op:"updated_at"`
	DeletedAt sql.NullTime    `op:"deleted_at"`
}

type MockCompany struct {
	ID        int       `op:"id,primary"`
	Name      string    `op:"name"`
	CreatedAt time.Time `op:"created_at"`
}

type MockCountry struct {
	ID   int    `op:"id,primary"`
	Name string `op:"name"`
}

type MockLabel struct {
	ID        uuid.UUID       `op:"ID,primary"`
	Label     string          `op:"Label"`
	DeletedAt driver.ZeroTime `op:"DeletedAt"`
}

type MockPostgresData struct {
	Name string
	Age  int
}

type MockPostgres struct {
	ID    uuid.UUID        `op:"ID,primary"`
	Roles []string         `op:"Roles"`
	Data  MockPostgresData `op:"Data"`
}

type MockSeeding struct {
	Users     []*MockUser
	Companies []*MockCompany
}

var (
	pgConn     db.ConnPool
	sqliteConn db.ConnPool
)

var (
	ctx         = context.Background()
	errRollback = errors.New("rollback")
)

func TestMain(m *testing.M) {
	dsn, cleanup, err := startPostgresContainer(ctx)
	if err != nil {
		log.Panicf("failed to start container: %v", err)
	}
	defer cleanup()

	pgConn, err = connectPostgres(ctx, dsn)
	if err != nil {
		log.Panicf("failed to connect to postgres: %v", err)
	}
	defer pgConn.Close() // nolint: errcheck

	err = createPostgresTables(ctx, pgConn)
	if err != nil {
		log.Panicf("failed to create postgres tables: %v", err)
	}

	sqliteConn, err = connectSqlite(sqliteDSN)
	if err != nil {
		log.Panicf("failed to connect to sqlite: %v", err)
	}
	defer func() {
		os.Remove(sqliteDSN) // nolint: errcheck, gosec
	}()
	defer sqliteConn.Close() // nolint: errcheck, gosec

	err = createSqliteTables(ctx, sqliteConn)
	if err != nil {
		log.Panicf("failed to create sqlite tables: %v", err)
	}

	m.Run()
}

func DataSeed(ctx context.Context, qe db.QueryExec) (*MockSeeding, error) {
	var mockUsers []*MockUser
	var mockCompanies []*MockCompany

	for i := 0; i < 10; i++ {
		company := &MockCompany{
			ID:        i + 1,
			Name:      gofakeit.Company(),
			CreatedAt: time.Now(),
		}

		err := orm.Put(companiesTable, company).With(ctx, qe)
		if err != nil {
			return nil, err
		}

		mockCompanies = append(mockCompanies, company)
	}

	for i := 0; i < 100; i++ {
		comp := mockCompanies[rand.IntN(len(mockCompanies))] // nolint: gosec
		user := &MockUser{
			ID:        i + 1,
			Name:      gofakeit.Name(),
			Email:     gofakeit.Email(),
			CreatedAt: gofakeit.Date(),
			DeletedAt: sql.NullTime{Valid: i > 30 && i < 50, Time: gofakeit.Date()},
		}

		if i%2 == 0 {
			user.CompanyId = sql.NullInt64{Valid: true, Int64: int64(comp.ID)}
		}

		err := orm.Put(usersTable, user).With(ctx, qe)
		if err != nil {
			return nil, err
		}
		mockUsers = append(mockUsers, user)
	}

	return &MockSeeding{
		Users:     mockUsers,
		Companies: mockCompanies,
	}, nil
}

func Transact(
	t *testing.T,
	ctx context.Context,
	conn db.ConnPool,
	handler func(ctx context.Context) error,
) error {
	t.Helper()
	return conn.Transact(ctx, handler)
}

func EachConn(t *testing.T, handler func(conn db.ConnPool)) {
	t.Helper()
	handler(pgConn)
	handler(sqliteConn)
}

func createPostgresTables(ctx context.Context, pool db.ConnPool) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		create table "%s" (
			id serial PRIMARY KEY,
			name text not null,
			created_at timestamptz not null default now()
		)
	`, companiesTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table "%s" (
			id serial PRIMARY KEY,
			name text not null,
			email text not null,
			company_id integer references %s(id) on delete cascade,
			created_at timestamptz not null default now(),
			updated_at timestamptz,
			deleted_at timestamptz
		)
	`, usersTable, companiesTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table "%s" (
			id serial PRIMARY KEY,
			name text unique not null
		)
	`, countriesTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table "%s" (
			"ID" uuid PRIMARY KEY,
			"Label" text not null,
			"DeletedAt" timestamptz
		)
	`, labelsTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table "%s" (
			"ID" uuid PRIMARY KEY,
			"Roles" text[] not null default '{}'::text[],
			"Data" jsonb not null
		)
	`, pgSpecialTable))
	if err != nil {
		return err
	}

	return err
}

func createSqliteTables(ctx context.Context, pool db.ConnPool) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		create table %s (
			id integer PRIMARY KEY,
			name text not null,
			created_at datetime not null default CURRENT_TIMESTAMP
		)
	`, companiesTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table %s (
			id integer PRIMARY KEY,
			name text not null,
			email text not null,
			company_id integer references %s(id) on delete cascade,
			created_at datetime not null default CURRENT_TIMESTAMP,
			updated_at datetime,
			deleted_at datetime
		)
	`, usersTable, companiesTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table "%s" (
			id integer PRIMARY KEY,
			name text unique not null
		)
	`, countriesTable))
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, fmt.Sprintf(`
		create table %s (
			ID text PRIMARY KEY,
			Label text not null,
			DeletedAt datetime
		)
	`, labelsTable))
	if err != nil {
		return err
	}

	return err
}

func connectPostgres(ctx context.Context, dsn string) (db.ConnPool, error) {
	options := []postgres.OpenOption{
		postgres.WithHealthCheckPeriod(time.Minute),
		postgres.WithMinIdleConns(0),
		postgres.WithMinConns(0),
		postgres.WithMaxConns(4),
		postgres.WithMaxConnIdleTime(time.Minute * 30),
		postgres.WithMaxConnLifetimeJitter(0),
		postgres.WithMaxConnLifetime(time.Hour),
		postgres.WithBeforeClose(func(conn *pgx.Conn) {}),
		postgres.WithAfterRelease(func(conn *pgx.Conn) bool {
			return true
		}),
		postgres.WithBeforeAcquire(func(context.Context, *pgx.Conn) bool {
			return true
		}),
		postgres.WithAfterConnect(func(context.Context, *pgx.Conn) error {
			return nil
		}),
		postgres.WithBeforeConnect(func(context.Context, *pgx.ConnConfig) error {
			return nil
		}),
	}

	pool, err := postgres.Open(ctx, dsn, options...)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func connectSqlite(dsn string) (db.ConnPool, error) {
	os.Remove(dsn) // nolint: errcheck, gosec
	pool, err := sqlite.Open(
		dsn,
		sqlite.WithConnMaxIdleTime(5*time.Minute),
		sqlite.WithConnMaxLifetime(5*time.Minute),
		sqlite.WithMaxIdleConns(2),
		sqlite.WithMaxOpenConns(10),
	)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func startPostgresContainer(ctx context.Context) (string, func(), error) {
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

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s",
		postgresUser,
		postgresPass,
		host,
		port.Port(),
		postgresDB,
	)
	return dsn, func() {
		container.Terminate(ctx) // nolint: errcheck, gosec
	}, nil
}
