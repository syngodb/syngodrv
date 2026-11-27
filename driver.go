package syngodrv

import (
	"context"
	"errors"
	"fmt"

	"github.com/syngodb/syngo"
	"github.com/syngodb/syngodrv/wire"
)

// DB is a database of a SyngoDB Server. It is safe for use by any number of goroutines, and
// shares the connections of its client with the other databases of the server.
type DB struct {
	client *Client
	name   string
	// own says the handle was made by Open, with a client that is its alone: Close closes it.
	own bool
}

// Open makes a handle on one database of a server, for a program that uses no other:
//
//	syngodbs://host:port/name
//	syngodb://host:port/name
//	syngodb+unix:///path/to/sock?database=name
//	syngodb+unix:///C:/path/to/sock?database=name   on Windows
//
// It is Connect followed by Database, in one line, and Close closes the connections. When the
// server has no database of that name it returns an error that is ErrNoDatabase: a database is
// made on purpose, with Client.EnsureDatabase.
func Open(ctx context.Context, addr string, options ...Option) (*DB, error) {
	where, err := parse(addr)
	if err != nil {
		return nil, err
	}
	if where.database == "" {
		return nil, fmt.Errorf("syngodrv: the address %q names no database; Connect makes a client of the whole server", addr)
	}
	client, err := connect(ctx, where, options)
	if err != nil {
		return nil, err
	}
	db := &DB{client: client, name: where.database, own: true}
	if _, err := db.Collections(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return db, nil
}

// Name is the name of the database.
func (db *DB) Name() string { return db.name }

// Collection returns a handle on the collection with that name. It never fails and sends nothing.
func (db *DB) Collection(name string) *Collection { return &Collection{db: db, name: name} }

// Collections lists the names of the collections, in order.
func (db *DB) Collections(ctx context.Context) (names []string, err error) {
	err = db.client.call(ctx, db.name, true, func(req *wire.Request) { req.Op = wire.OpCollections }, func(resp *wire.Response) {
		names = append([]string(nil), resp.Names...)
	})
	return names, err
}

// DropCollection removes a collection with its documents and indexes.
func (db *DB) DropCollection(ctx context.Context, name string) error {
	return db.client.call(ctx, db.name, false, func(req *wire.Request) { req.Op, req.Name = wire.OpDropCollection, name }, nil)
}

// Stats says what the database holds and how its memory is doing.
func (db *DB) Stats(ctx context.Context) (stats syngo.Stats, err error) {
	err = db.client.call(ctx, db.name, true, func(req *wire.Request) { req.Op = wire.OpStats }, func(resp *wire.Response) {
		stats = statsOf(&resp.Stats)
	})
	return stats, err
}

func statsOf(s *wire.Stats) syngo.Stats {
	out := syngo.Stats{
		DiskBytes:    s.DiskBytes,
		Cache:        syngo.CacheStats{Budget: s.Cache.Budget, Used: s.Cache.Used, Hits: s.Cache.Hits, Misses: s.Cache.Misses, Evictions: s.Cache.Evictions},
		Engine:       syngo.EngineStats{CacheBudget: s.Engine.CacheBudget, CacheUsed: s.Engine.CacheUsed, CacheHits: s.Engine.CacheHits, CacheMisses: s.Engine.CacheMisses, Buffered: s.Engine.Buffered},
		Commits:      syngo.CommitStats{Remembered: int(s.Commits.Remembered), Bytes: s.Commits.Bytes, Kept: s.Commits.Kept, KeptUsed: s.Commits.KeptUsed, KeptBudget: s.Commits.KeptBudget, Expired: s.Commits.Expired},
		Transactions: syngo.TxStats{Open: int(s.Transactions.Open), Writing: int(s.Transactions.Writing), Limit: s.Transactions.Limit, Refused: s.Transactions.Refused},
		Sorts:        syngo.SortStats{InMemory: s.Sorts.InMemory, OnDisk: s.Sorts.OnDisk, Each: s.Sorts.Each, Budget: s.Sorts.Budget, Used: s.Sorts.Used},
	}
	for _, c := range s.Collections {
		cs := syngo.CollectionStats{Name: c.Name, Documents: c.Documents}
		for _, ix := range c.Indexes {
			cs.Indexes = append(cs.Indexes, syngo.IndexStats{Name: ix.Index.Name, Fields: append([]string(nil), ix.Index.Fields...), Unique: ix.Index.Unique,
				Building: ix.Building, Rebuilding: ix.Rebuilding, Stale: ix.Stale})
		}
		out.Collections = append(out.Collections, cs)
	}
	return out
}

// Close closes the connections of a handle made by Open: every later operation then returns
// syngo.ErrClosed. A handle made by Client.Database has no connections of its own, and is
// closed with its client.
func (db *DB) Close() error {
	if db.own {
		return db.client.Close()
	}
	return nil
}

// maxAttempts is how many times in a row Update runs its function before it gives up, as the
// embedded library does.
const maxAttempts = 32

// Update runs fn in a transaction and commits what it wrote, all of it or none. When what fn
// read was changed by someone else before it could commit, fn is run again from the start: it
// may run more than once, and must have no effect outside the transaction that running it twice
// would repeat. After 32 attempts in a row Update gives up with an error that is
// syngo.ErrConflict.
//
// A function that keeps losing finishes all the same, as it does embedded: the attempt that
// runs again is told to the server as such, on the connection of the one that lost, and the
// server gives it its turn.
func (db *DB) Update(ctx context.Context, fn func(tx *Tx) error) error {
	// kept is the connection of the attempt that lost, on which the next one begins.
	var kept *conn
	defer func() {
		if kept != nil {
			db.client.give(kept)
		}
	}()
	for attempt := 1; ; attempt++ {
		tx, err := db.beginOn(ctx, kept, false, attempt > 1)
		kept = nil
		if err != nil {
			return err
		}
		tx.managed, tx.keep = true, true
		err = run(tx, fn)
		if err != nil {
			tx.end(ctx, wire.OpRollback)
			return err
		}
		if err = tx.end(ctx, wire.OpCommit); !errors.Is(err, syngo.ErrConflict) {
			return err
		}
		kept, tx.lost = tx.lost, nil
		if attempt >= maxAttempts {
			return &syngo.ConflictError{Attempts: attempt}
		}
	}
}

// run runs the function of a transaction, and ends the transaction when the function panics.
func run(tx *Tx, fn func(tx *Tx) error) (err error) {
	done := false
	defer func() {
		if !done {
			tx.end(context.Background(), wire.OpRollback)
		}
	}()
	err = fn(tx)
	done = true
	return err
}

// View runs fn in a transaction that only reads: everything fn reads is the database at one
// moment. fn runs once.
func (db *DB) View(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := db.begin(ctx, true)
	if err != nil {
		return err
	}
	tx.managed = true
	err = run(tx, fn)
	tx.end(ctx, wire.OpRollback)
	return err
}

// Begin starts a transaction that the caller ends itself, with Commit or Rollback. It keeps one
// connection of the pool until then.
func (db *DB) Begin(ctx context.Context) (*Tx, error) { return db.begin(ctx, false) }

// BeginView starts a transaction that only reads and that the caller ends itself. The server is
// told it only reads when it begins, and remembers nothing to check it against.
func (db *DB) BeginView(ctx context.Context) (*Tx, error) { return db.begin(ctx, true) }

func (db *DB) begin(ctx context.Context, readOnly bool) (*Tx, error) {
	return db.beginOn(ctx, nil, readOnly, false)
}

// beginOn begins a transaction on a connection: the one given, which the attempt of an Update
// that lost had, or one of the pool. again says the transaction runs again what that attempt
// ran.
func (db *DB) beginOn(ctx context.Context, cn *conn, readOnly, again bool) (*Tx, error) {
	c := db.client
	pooled := cn == nil
	if pooled {
		// A transaction does not begin on a connection of a password that is being changed:
		// see Client.changing.
		c.changing.RLock()
		defer c.changing.RUnlock()
		var err error
		if cn, err = c.take(ctx); err != nil {
			return nil, err
		}
	}
	fill := func() {
		req := cn.begin(db.name)
		req.Op, req.ReadOnly, req.Again = wire.OpBegin, readOnly, again && !pooled
	}
	fill()
	err := cn.exchange(ctx)
	if err != nil && pooled && !cn.fresh && (errors.Is(err, errUnsent) || (errors.Is(err, errUnanswered) && ctx.Err() == nil)) {
		// A connection that died in the pool. A transaction that began and was never heard of
		// again ends with its connection: beginning another is safe.
		if cn, err = c.again(ctx, cn); err != nil {
			return nil, err
		}
		fill()
		err = cn.exchange(ctx)
	}
	if err != nil {
		c.give(cn)
		return nil, lost(err, true)
	}
	return &Tx{db: db, cn: cn, readOnly: readOnly}, nil
}

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

// errEndsItself is returned by Commit inside the function of Update or View.
var errEndsItself = errors.New("syngodrv: a transaction that Update or View runs is ended by what its function returns")

// Tx is a transaction: it lives on one connection, from Begin to Commit or Rollback. A Tx is for
// one goroutine at a time.
type Tx struct {
	db *DB
	// cn is the connection of the transaction, until it ends.
	cn       *conn
	readOnly bool
	// managed says Update or View runs the transaction, and ends it.
	managed bool
	// keep says the connection is not to go back to the pool when the commit loses to another:
	// it is left in lost, for the attempt that runs the function again.
	keep bool
	lost *conn
	// cursors counts the answers that are being pulled inside the transaction.
	cursors int
}

// Collection returns the collection with that name, as the transaction sees it.
func (tx *Tx) Collection(name string) *Collection {
	return &Collection{db: tx.db, name: name, tx: tx}
}

// call sends a request of the transaction on its connection.
func (tx *Tx) call(ctx context.Context, fill func(req *wire.Request), read func(resp *wire.Response)) error {
	if tx.cn == nil {
		return syngo.ErrTxDone
	}
	fill(tx.cn.begin(tx.db.name))
	err := tx.cn.exchange(ctx)
	if err != nil {
		if tx.cn.broken {
			// The connection is gone, and the transaction with it: the server ends what a
			// session had open when the session ends, and nothing of it was written.
			tx.release()
			return fmt.Errorf("%w: the transaction is lost with it: %w", ErrConnection, unwrapped(err))
		}
		return err
	}
	if read != nil {
		read(&tx.cn.resp)
	}
	return nil
}

// release gives the connection of the transaction back: the transaction is over.
func (tx *Tx) release() {
	if cn := tx.cn; cn != nil {
		tx.cn = nil
		tx.db.client.give(cn)
	}
}

// end ends the transaction with a commit or a rollback.
func (tx *Tx) end(ctx context.Context, op wire.Op) error {
	cn := tx.cn
	if cn == nil {
		if op == wire.OpCommit {
			return syngo.ErrTxDone
		}
		return nil
	}
	cn.begin(tx.db.name).Op = op
	if op == wire.OpRollback {
		// A rollback is not to wait on a context that may be over already: it is what a program
		// does when it gives up.
		ctx = context.WithoutCancel(ctx)
	}
	err := cn.exchange(ctx)
	if err != nil && !cn.broken && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		// The program's context ended, perhaps before the request left: the session may still
		// be in the transaction, and its connection must not go back to the pool that way. A
		// rollback settles it; a session that is in no transaction takes it as nothing.
		cn.begin(tx.db.name).Op = wire.OpRollback
		if cn.exchange(context.WithoutCancel(ctx)) != nil {
			cn.close()
		}
	}
	if tx.keep && op == wire.OpCommit && !cn.broken && errors.Is(err, syngo.ErrConflict) {
		tx.cn, tx.lost = nil, cn
	}
	tx.release()
	switch {
	case err == nil || op == wire.OpRollback:
		return nil
	case errors.Is(err, errUnanswered):
		// The commit left and no answer came: it may have been made.
		return fmt.Errorf("%w (%w)", syngo.ErrOutcomeUnknown, unwrapped(err))
	case errors.Is(err, errUnsent):
		return fmt.Errorf("%w: the transaction is lost with it: %w", ErrConnection, unwrapped(err))
	}
	return err
}

// Commit writes what the transaction did, all of it or none, and ends the transaction. When what
// it read was changed by someone else in the meantime it returns an error that is
// syngo.ErrConflict, and nothing is written. When the connection is lost before the answer
// comes, it returns an error that is syngo.ErrOutcomeUnknown: the commit may have been made, and
// the driver does not send it again.
func (tx *Tx) Commit(ctx context.Context) error {
	if tx.managed {
		return errEndsItself
	}
	return tx.end(ctx, wire.OpCommit)
}

// Rollback ends the transaction without writing anything. On a transaction that has ended, or
// that Update or View runs, it does nothing.
func (tx *Tx) Rollback() {
	if !tx.managed {
		tx.end(context.Background(), wire.OpRollback)
	}
}
