package sqlite

import (
	"os"
	"io/ioutil"
	"sync/atomic"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"

	//"github.com/mattes/migrate"
	"github.com/mattes/migrate/database"
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	database.Register("sqlite3", &Sqlite{})
}

var DefaultMigrationsTable = "schema_migrations"

type Sqlite struct {
	db              *sql.DB
	dbPath    string
	migrationsTable string
	locked          bool
	txid            int64
}

func getOptionWithDefault(u *url.URL, opt, dflt string) string {
	values := u.Query()[opt]
	if values == nil {
		return dflt
	}
	return values[0]
}

func (s *Sqlite) Open(uri string) (database.Driver, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	migTbl := getOptionWithDefault(u, "x-migrations-table", DefaultMigrationsTable)
	db, err := sql.Open("sqlite3", u.Path)
	s = &Sqlite{
		db:              db,
		dbPath:    u.Path,
		migrationsTable: migTbl,
	}

	if err != nil {
		return nil, err
	}

	// make sure we can actually talk to the database
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	// make sure the version table exists
	err = s.ensureVersionTable()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func dberror(err error, query string) error {
	return &database.Error{OrigErr: err, Query: []byte(query)}
}

func (s *Sqlite) ensureVersionTable() error {
	// check if migration table exists
	query := `SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = $1`

	var count int
	err := s.db.QueryRow(query, s.migrationsTable).Scan(&count)
	if err != nil {
		return dberror(err, query)
	}
	if count == 1 {
		return nil
	}

	// if not, create the empty migration table
	query = `CREATE TABLE ${MIGRATIONS_TABLE} (version bigint not null primary key, dirty boolean not null)`
	if err := s.exec(query); err != nil {
		return err
	}
	return nil
}

func (s *Sqlite) Close() error {
	return s.db.Close()
}

// Lock should acquire a database lock so that only one migration process
// can run at a time. Migrate will call this function before Run is called.
// If the implementation can't provide this functionality, return nil.
// Return database.ErrLocked if database is already locked.
func (s *Sqlite) Lock() error {
	if s.locked {
		return database.ErrLocked
	}
	s.locked = true
	if err := s.exec(`PRAGMA locking_mode = EXCLUSIVE`); err != nil {
		return err
	}
	if err := s.exec(`BEGIN EXCLUSIVE`); err != nil {
		return err
	}
	return nil
}

func (s *Sqlite) exec(op string, arg ...interface{}) error {
	op = strings.Replace(op, "${MIGRATIONS_TABLE}", s.migrationsTable, -1)

	_, err := s.db.Exec(op, arg...)
	if err != nil {
		return &database.Error{
			OrigErr: err,
			Query:   []byte(op),
		}
	}
	return nil
}

// Unlock should release the lock. Migrate will call this function after
// all migrations have been run.
func (s *Sqlite) Unlock() error {
	if !s.locked {
		return nil // strange... but same as postgres driver
	}
	if err := s.exec(`COMMIT`); err != nil {
		return err
	}
	return nil
}

// Run applies a migration to the database. migration is guaranteed to be not nil.
func (s *Sqlite) Run(migration io.Reader) error {
	buf, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	return s.exec(string(buf))
}

// SetVersion saves version and dirty state.
// Migrate will call this function before and after each call to Run.
// version must be >= -1. -1 means NilVersion.
func (s *Sqlite) SetVersion(version int, dirty bool) error {
	return s.transactionally(func () error {
		err := s.exec(`DELETE FROM ${MIGRATIONS_TABLE}`)
		if err != nil {
			return err
		}

		if version >= 0 {
			return s.exec(`INSERT INTO ${MIGRATIONS_TABLE} (version, dirty) 
			               VALUES ($1, $2)`,
				version, dirty)
		}
		return nil
	})
}

func (s *Sqlite) transactionally(thunk func() error) error {
	txname := fmt.Sprintf("txn_%d", atomic.AddInt64(&s.txid, 1))
	var success bool

	defer func() {
		if !success {
			s.exec("ROLLBACK TO " + txname)
			// silently ignore rollback failures
		}
	}()

	err := s.exec("SAVEPOINT " + txname)
	if err != nil {
		return err
	}

	err = thunk()
	if err != nil {
		return err
	}
	s.exec("RELEASE " + txname)
	success = true
	return nil
}

// Version returns the currently active version and if the database is dirty.
// When no migration has been applied, it must return version -1.
// Dirty means, a previous migration failed and user interaction is required.
func (s *Sqlite) Version() (int, bool, error) {
	query := `SELECT version, dirty FROM "` + s.migrationsTable + `" LIMIT 1`

	var version int
	var dirty bool
	err := s.db.QueryRow(query).Scan(&version, &dirty)
	if err != nil {
		// nothing has been set yet
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		// some other error
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty, nil
}

// Drop deletes everyting in the database.
func (s *Sqlite) Drop() error {
	return os.Remove(s.dbPath)
}
