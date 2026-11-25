package syngodrv

import (
	"context"
	"errors"

	"github.com/syngodb/syngo"
)

// ErrNotBuilt is what every call that would speak to a server returns until the driver is built
// (Phase 3 of the plan).
var ErrNotBuilt = errors.New("syngodrv: the driver is a skeleton, and does not speak to a server yet")

// Option configures a database handle at Open. A handle opens well with none.
type Option func(*config)

type config struct{}

// DB is a database of a SyngoDB Server. It keeps a pool of connections, and is safe for use by
// any number of goroutines.
type DB struct {
	address string
}

// Open makes a handle on a database of a server. The address names the server and the database.
func Open(ctx context.Context, address string, options ...Option) (*DB, error) {
	var cfg config
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return nil, ErrNotBuilt
}

// Collection returns a handle on the collection with that name. It never fails and sends nothing.
func (db *DB) Collection(name string) *Collection { return &Collection{db: db, name: name} }

// Collections lists the names of the collections, in order.
func (db *DB) Collections(ctx context.Context) ([]string, error) { return nil, ErrNotBuilt }

// DropCollection removes a collection with its documents and indexes.
func (db *DB) DropCollection(ctx context.Context, name string) error { return ErrNotBuilt }

// Stats says what the database holds and how its memory is doing.
func (db *DB) Stats(ctx context.Context) (syngo.Stats, error) { return syngo.Stats{}, ErrNotBuilt }

// Update runs fn in a transaction and commits what it wrote, all of it or none. fn may run more
// than once: see syngo.DB.
func (db *DB) Update(ctx context.Context, fn func(tx *Tx) error) error { return ErrNotBuilt }

// View runs fn in a transaction that only reads. fn runs once.
func (db *DB) View(ctx context.Context, fn func(tx *Tx) error) error { return ErrNotBuilt }

// Begin starts a transaction that the caller ends itself, with Commit or Rollback. It keeps one
// connection of the pool until then.
func (db *DB) Begin(ctx context.Context) (*Tx, error) { return nil, ErrNotBuilt }

// BeginView starts a transaction that only reads and that the caller ends itself. The server is
// told it only reads when it begins, and remembers nothing to check it against.
func (db *DB) BeginView(ctx context.Context) (*Tx, error) { return nil, ErrNotBuilt }

// Close closes the connections of the handle. Every later operation returns syngo.ErrClosed.
func (db *DB) Close() error { return nil }

// Update runs fn in a transaction, like DB.Update, and returns the value fn returned in the
// attempt that was committed.
func Update[T any](ctx context.Context, db *DB, fn func(tx *Tx) (T, error)) (T, error) {
	var out T
	err := db.Update(ctx, func(tx *Tx) error {
		v, err := fn(tx)
		if err != nil {
			return err
		}
		out = v
		return nil
	})
	if err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// Tx is a transaction: it lives on one connection, from Begin to Commit or Rollback. A Tx is for
// one goroutine at a time.
type Tx struct {
	db *DB
}

// Collection returns the collection with that name, as the transaction sees it.
func (tx *Tx) Collection(name string) *Collection {
	return &Collection{db: tx.db, name: name, tx: tx}
}

// Commit writes what the transaction did, all of it or none, and ends the transaction. When the
// connection is lost before the answer comes, it returns an error that is
// syngo.ErrOutcomeUnknown.
func (tx *Tx) Commit(ctx context.Context) error { return ErrNotBuilt }

// Rollback ends the transaction without writing anything.
func (tx *Tx) Rollback() {}
