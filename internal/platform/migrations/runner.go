package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	migrationName = regexp.MustCompile(`^(\d{6})_([a-z0-9_]+)\.up\.sql$`)
	identifier    = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,63}$`)
)

type Migration struct {
	Version  int64
	Name     string
	SQL      string
	Checksum string
}

type Runner struct {
	Database     *sql.DB
	DatabaseName string
	Directory    string
	LockTimeout  time.Duration
}

// Up applies append-only MySQL migrations under a server-side named lock.
// MySQL DDL is atomic per statement but not transactional across a file, so
// released migrations must remain immutable. This cutover supports fresh
// databases only: after partial DDL failure, rebuild an empty database before
// retrying. The checksum ledger detects history edits before later migrations run.
func (runner Runner) Up(ctx context.Context) error {
	if runner.Database == nil {
		return errors.New("MySQL migration database is required")
	}
	if !identifier.MatchString(runner.DatabaseName) {
		return fmt.Errorf("unsafe MySQL database name %q", runner.DatabaseName)
	}
	plan, err := LoadPlan(os.DirFS(runner.Directory))
	if err != nil {
		return err
	}
	connection, err := runner.Database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer connection.Close()
	var selectedDatabase sql.NullString
	if err := connection.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&selectedDatabase); err != nil {
		return fmt.Errorf("read selected MySQL database: %w", err)
	}
	if !selectedDatabase.Valid || selectedDatabase.String != runner.DatabaseName {
		return fmt.Errorf("MySQL DSN selected database %q, expected %q", selectedDatabase.String, runner.DatabaseName)
	}
	lockSeconds := int64((runner.LockTimeout + time.Second - 1) / time.Second)
	if lockSeconds < 1 {
		lockSeconds = 1
	}
	lockDigest := sha256.Sum256([]byte(runner.DatabaseName))
	lockName := "evalfrog:migrations:" + hex.EncodeToString(lockDigest[:16])
	var locked sql.NullInt64
	if err := connection.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, lockSeconds).Scan(&locked); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if !locked.Valid || locked.Int64 != 1 {
		return fmt.Errorf("acquire migration lock %q timed out", lockName)
	}
	defer func() { _, _ = connection.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", lockName) }()
	if _, err := connection.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version BIGINT PRIMARY KEY,
		name VARCHAR(200) NOT NULL,
		checksum CHAR(64) NOT NULL,
		applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	) ENGINE=InnoDB`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	for _, migration := range plan {
		var checksum string
		err := connection.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE version=?", migration.Version).Scan(&checksum)
		switch {
		case err == nil:
			if checksum != migration.Checksum {
				return fmt.Errorf("migration %06d checksum mismatch", migration.Version)
			}
			continue
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("read migration %06d: %w", migration.Version, err)
		}
		if strings.TrimSpace(migration.SQL) == "" {
			return fmt.Errorf("migration %06d_%s is empty", migration.Version, migration.Name)
		}
		if _, err := connection.ExecContext(ctx, migration.SQL); err != nil {
			return fmt.Errorf("execute migration %06d_%s: %w", migration.Version, migration.Name, err)
		}
		if _, err := connection.ExecContext(ctx,
			"INSERT INTO schema_migrations (version, name, checksum) VALUES (?, ?, ?)",
			migration.Version, migration.Name, migration.Checksum); err != nil {
			return fmt.Errorf("record migration %06d: %w", migration.Version, err)
		}
	}
	return nil
}

func LoadPlan(filesystem fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(filesystem, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	plan := make([]Migration, 0, len(entries))
	versions := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := migrationName.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version %s: %w", entry.Name(), err)
		}
		if previous, exists := versions[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %06d in %s and %s", version, previous, entry.Name())
		}
		content, err := fs.ReadFile(filesystem, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		digest := sha256.Sum256(content)
		versions[version] = entry.Name()
		plan = append(plan, Migration{Version: version, Name: matches[2], SQL: string(content), Checksum: hex.EncodeToString(digest[:])})
	}
	sort.Slice(plan, func(left, right int) bool { return plan[left].Version < plan[right].Version })
	return plan, nil
}
