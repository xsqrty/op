package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/xsqrty/op/db"
	"github.com/xsqrty/op/driver"
	"github.com/xsqrty/op/orm"
	"log"
	"math/rand/v2"
	"os"
	"testing"
	"time"
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
	ID    uuid.UUID `op:"ID,primary"`
	Label string    `op:"Label"`
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

var mockUsers []*MockUser
var mockCompanies []*MockCompany

var pgConn db.ConnPool
var sqliteConn db.ConnPool

var ctx = context.Background()
var errRollback = errors.New("rollback")

func TestMain(m *testing.M) {
	dsn, cleanup, err := startPostgresContainer(ctx)
	defer cleanup()
	if err != nil {
		log.Fatalf("failed to start container: %v", err)
	}

	pgConn, err = connectPostgres(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pgConn.Close()

	err = createPostgresTables(ctx, pgConn)
	if err != nil {
		log.Fatalf("failed to create postgres tables: %v", err)
	}

	sqliteConn, err = connectSqlite(ctx, sqliteDSN)
	if err != nil {
		log.Fatalf("failed to connect to sqlite: %v", err)
	}
	defer func() {
		os.Remove(sqliteDSN)
	}()
	defer sqliteConn.Close()

	err = createSqliteTables(ctx, sqliteConn)
	if err != nil {
		log.Fatalf("failed to create sqlite tables: %v", err)
	}

	for i := 0; i < 100; i++ {
		mockUsers = append(mockUsers, &MockUser{
			Name:      gofakeit.Name(),
			Email:     gofakeit.Email(),
			CreatedAt: gofakeit.Date(),
			DeletedAt: sql.NullTime{Valid: i > 30 && i < 50, Time: gofakeit.Date()},
		})
	}

	for i := 0; i < 10; i++ {
		mockCompanies = append(mockCompanies, &MockCompany{
			ID:   i + 1,
			Name: gofakeit.Company(),
		})
	}

	m.Run()
}

func DataSeed(ctx context.Context, qe db.QueryExec) error {
	for _, company := range mockCompanies {
		company.CreatedAt = time.Now()
		err := orm.Put[MockCompany](companiesTable, company).With(ctx, qe)
		if err != nil {
			return err
		}
	}

	for i, user := range mockUsers {
		comp := mockCompanies[rand.IntN(len(mockCompanies))]
		user.CreatedAt = time.Now()
		if i%2 == 0 {
			user.CompanyId = sql.NullInt64{Valid: true, Int64: int64(comp.ID)}
		}

		err := orm.Put[MockUser](usersTable, user).With(ctx, qe)
		if err != nil {
			return err
		}
	}

	return nil
}

func Transact(t *testing.T, ctx context.Context, conn db.ConnPool, handler func(ctx context.Context) error) error {
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
			"Label" text not null
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
			Label text not null
		)
	`, labelsTable))
	if err != nil {
		return err
	}

	return err
}

func connectPostgres(ctx context.Context, dsn string) (db.ConnPool, error) {
	options := []db.OpenPgxOption{
		db.WithPgxHealthCheckPeriod(time.Minute),
		db.WithPgxMinIdleConns(0),
		db.WithPgxMinConns(0),
		db.WithPgxMaxConns(4),
		db.WithPgxMaxConnIdleTime(time.Minute * 30),
		db.WithPgxMaxConnLifetimeJitter(0),
		db.WithPgxMaxConnLifetime(time.Hour),
		db.WithPgxBeforeClose(func(conn *pgx.Conn) {}),
		db.WithPgxAfterRelease(func(conn *pgx.Conn) bool {
			return true
		}),
		db.WithPgxBeforeAcquire(func(context.Context, *pgx.Conn) bool {
			return true
		}),
		db.WithPgxAfterConnect(func(context.Context, *pgx.Conn) error {
			return nil
		}),
		db.WithPgxBeforeConnect(func(context.Context, *pgx.ConnConfig) error {
			return nil
		}),
	}

	pool, err := db.OpenPostgres(ctx, dsn, options...)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func connectSqlite(ctx context.Context, dsn string) (db.ConnPool, error) {
	pool, err := db.OpenSqlite(
		ctx,
		dsn,
		db.WithConnMaxIdleTime(5*time.Minute),
		db.WithConnMaxLifetime(5*time.Minute),
		db.WithMaxIdleConns(2),
		db.WithMaxOpenConns(10),
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

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", postgresUser, postgresPass, host, port.Port(), postgresDB)
	return dsn, func() {
		container.Terminate(ctx)
	}, nil
}
