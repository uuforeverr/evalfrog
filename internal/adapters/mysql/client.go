package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/uu999/evalfrog/internal/platform/config"
)

var positionalParameter = regexp.MustCompile(`\$([1-9][0-9]*)`)

// Client owns the authoritative MySQL connection pool. Application and domain
// packages depend on ports; only the MySQL adapter sees database/sql.
type Client struct {
	database *sql.DB
	pool     *Pool
}

type AcquireObserver interface {
	ObserveMySQLPoolAcquire(time.Duration, string)
}

func Open(ctx context.Context, value config.MySQLConfig) (*Client, error) {
	return OpenWithAcquireObserver(ctx, value, nil)
}

func OpenWithAcquireObserver(ctx context.Context, value config.MySQLConfig, observer AcquireObserver) (*Client, error) {
	driverConfig, err := mysqldriver.ParseDSN(value.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse MySQL DSN: %w", err)
	}
	if driverConfig.DBName != value.Database {
		return nil, fmt.Errorf("MySQL DSN database %q does not match configured database %q", driverConfig.DBName, value.Database)
	}
	driverConfig.ParseTime = true
	driverConfig.Loc = time.UTC
	driverConfig.MultiStatements = true
	if driverConfig.Params == nil {
		driverConfig.Params = make(map[string]string)
	}
	driverConfig.Params["time_zone"] = "'+00:00'"
	driverConfig.Params["transaction_isolation"] = "'REPEATABLE-READ'"
	if timeout := value.LockTimeout.Duration(); timeout > 0 {
		seconds := int64((timeout + time.Second - 1) / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		driverConfig.Params["innodb_lock_wait_timeout"] = strconv.FormatInt(seconds, 10)
	}
	database, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("create MySQL pool: %w", err)
	}
	database.SetMaxOpenConns(int(value.PoolMax))
	database.SetMaxIdleConns(int(value.PoolMin))
	database.SetConnMaxIdleTime(value.ConnMaxIdleTime.Duration())
	database.SetConnMaxLifetime(value.ConnMaxLifetime.Duration())
	client := &Client{database: database}
	client.pool = &Pool{database: database, observer: observer}
	if err := client.Check(ctx); err != nil {
		database.Close()
		return nil, err
	}
	return client, nil
}

func (client *Client) Check(ctx context.Context) error {
	started := time.Now()
	connection, err := client.database.Conn(ctx)
	if client.pool.observer != nil {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		client.pool.observer.ObserveMySQLPoolAcquire(time.Since(started), outcome)
	}
	if err != nil {
		return fmt.Errorf("acquire MySQL connection: %w", err)
	}
	defer connection.Close()
	if err := connection.PingContext(ctx); err != nil {
		return fmt.Errorf("MySQL ping: %w", err)
	}
	return nil
}

func (client *Client) Pool() *Pool { return client.pool }

func (client *Client) Close() { _ = client.database.Close() }

// Pool preserves the small context-first API the repository adapter needs
// while database/sql remains private to this package.
type Pool struct {
	database *sql.DB
	observer AcquireObserver
}

func (pool *Pool) Raw() *sql.DB { return pool.database }

func (pool *Pool) Stats() sql.DBStats { return pool.database.Stats() }

func (pool *Pool) Exec(ctx context.Context, query string, arguments ...any) (CommandTag, error) {
	query, arguments, err := bind(query, arguments)
	if err != nil {
		return CommandTag{}, err
	}
	result, err := pool.database.ExecContext(ctx, query, arguments...)
	return CommandTag{result: result}, err
}

func (pool *Pool) Query(ctx context.Context, query string, arguments ...any) (*sql.Rows, error) {
	query, arguments, err := bind(query, arguments)
	if err != nil {
		return nil, err
	}
	return pool.database.QueryContext(ctx, query, arguments...)
}

func (pool *Pool) QueryRow(ctx context.Context, query string, arguments ...any) Row {
	query, arguments, err := bind(query, arguments)
	if err != nil {
		return Row{err: err}
	}
	return Row{row: pool.database.QueryRowContext(ctx, query, arguments...)}
}

func (pool *Pool) Begin(ctx context.Context) (*Tx, error) {
	return pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
}

func (pool *Pool) BeginTx(ctx context.Context, options *sql.TxOptions) (*Tx, error) {
	if options == nil {
		options = &sql.TxOptions{Isolation: sql.LevelRepeatableRead}
	}
	started := time.Now()
	transaction, err := pool.database.BeginTx(ctx, options)
	if pool.observer != nil {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		pool.observer.ObserveMySQLPoolAcquire(time.Since(started), outcome)
	}
	if err != nil {
		return nil, err
	}
	return &Tx{transaction: transaction}, nil
}

type Tx struct {
	transaction *sql.Tx
}

func (transaction *Tx) Exec(ctx context.Context, query string, arguments ...any) (CommandTag, error) {
	query, arguments, err := bind(query, arguments)
	if err != nil {
		return CommandTag{}, err
	}
	result, err := transaction.transaction.ExecContext(ctx, query, arguments...)
	return CommandTag{result: result}, err
}

func (transaction *Tx) Query(ctx context.Context, query string, arguments ...any) (*sql.Rows, error) {
	query, arguments, err := bind(query, arguments)
	if err != nil {
		return nil, err
	}
	return transaction.transaction.QueryContext(ctx, query, arguments...)
}

func (transaction *Tx) QueryRow(ctx context.Context, query string, arguments ...any) Row {
	query, arguments, err := bind(query, arguments)
	if err != nil {
		return Row{err: err}
	}
	return Row{row: transaction.transaction.QueryRowContext(ctx, query, arguments...)}
}

func (transaction *Tx) Commit(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = transaction.transaction.Rollback()
		return err
	}
	return transaction.transaction.Commit()
}

func (transaction *Tx) Rollback(context.Context) error { return transaction.transaction.Rollback() }

type Row struct {
	row *sql.Row
	err error
}

func (row Row) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	return row.row.Scan(destinations...)
}

type CommandTag struct {
	result sql.Result
}

func (tag CommandTag) RowsAffected() int64 {
	if tag.result == nil {
		return 0
	}
	value, _ := tag.result.RowsAffected()
	return value
}

// bind lets the repository migrate incrementally from MySQL's numbered
// placeholders. Repeated and out-of-order placeholders rebuild argument order.
func bind(query string, arguments []any) (string, []any, error) {
	if !positionalParameter.MatchString(query) {
		return query, arguments, nil
	}
	bound := make([]any, 0)
	var bindErr error
	rebound := positionalParameter.ReplaceAllStringFunc(query, func(parameter string) string {
		index, err := strconv.Atoi(parameter[1:])
		if err != nil || index < 1 || index > len(arguments) {
			bindErr = fmt.Errorf("SQL placeholder %s has no argument", parameter)
			return "?"
		}
		bound = append(bound, arguments[index-1])
		return "?"
	})
	return rebound, bound, bindErr
}

func isDuplicateKey(err error) bool {
	var mysqlError *mysqldriver.MySQLError
	return errors.As(err, &mysqlError) && mysqlError.Number == 1062
}

func isRetryableTransactionError(err error) bool {
	var mysqlError *mysqldriver.MySQLError
	if !errors.As(err, &mysqlError) {
		return false
	}
	return mysqlError.Number == 1213 || mysqlError.Number == 1205
}
