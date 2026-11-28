package syngodrv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/syngodb/syngodrv/wire"
)

// A database as a stream, both ways, and what looks after the databases of a server: none of it
// has a counterpart in the embedded library's contracts, where a database is a directory.

var (
	// ErrBackup is returned for a stream that is not a backup the server can restore, or not an
	// export it can make a database of: it is no backup, it was cut short or altered, it is
	// encrypted and the server is not, or it is an export that does not say where it ends.
	ErrBackup = errors.New("syngodrv: not a usable backup")
	// ErrPassphrase is returned for a backup whose passphrase is missing or is not the right
	// one.
	ErrPassphrase = errors.New("syngodrv: the passphrase of the backup is missing or is not the right one")
	// ErrNoSpace is returned when the disk of the server has too little room left for what was
	// to be built: nothing was changed.
	ErrNoSpace = errors.New("syngodrv: the disk of the server has too little room left")
	// ErrNoKept is returned for the number of nothing that is kept.
	ErrNoKept = errors.New("syngodrv: nothing is kept under that number")
)

// partSize is how many bytes of an upload travel in one request.
const partSize = 256 << 10

// Backup writes the database of that name to w as a backup, which Restore reads back: the
// database as it was when the server took the request, whatever is written to it meanwhile.
// The backup of a server that is encrypted is encrypted with that server's passphrase. Every
// user may ask it: a backup holds nothing a reader cannot read.
func (c *Client) Backup(ctx context.Context, database string, w io.Writer) error {
	return c.download(ctx, database, wire.OpBackup, w)
}

// Export writes the database of that name to w as lines of JSON, which Import reads back: for
// each collection the declarations of its indexes, then its documents. Every user may ask it.
func (c *Client) Export(ctx context.Context, database string, w io.Writer) error {
	return c.download(ctx, database, wire.OpExport, w)
}

// download pulls a stream of the server, a part at a time, on one connection, and writes each
// part to w before it asks for the next: the server goes no faster than w takes.
func (c *Client) download(ctx context.Context, database string, op wire.Op, w io.Writer) error {
	c.changing.RLock()
	defer c.changing.RUnlock()
	cn, err := c.take(ctx)
	if err != nil {
		return err
	}
	send := func() error {
		cn.begin(database).Op = op
		return cn.exchange(ctx)
	}
	err = send()
	if err != nil && !cn.fresh && (errors.Is(err, errUnsent) || (errors.Is(err, errUnanswered) && ctx.Err() == nil)) {
		// A connection that died in the pool: nothing was changed, and it is asked again.
		if cn, err = c.again(ctx, cn); err != nil {
			return err
		}
		err = send()
	}
	// giveUp tells the server that nobody takes the rest of the stream, so that it does not
	// keep it: the connection goes back to the pool, and the next to use it must find no
	// answer of somebody else's open on it.
	var cursor uint64
	giveUp := func() {
		if cursor != 0 && !cn.broken {
			req := cn.begin(database)
			req.Op, req.Cursor = wire.OpCloseCursor, cursor
			stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if cn.exchange(stop) != nil {
				cn.broken = true
			}
			cancel()
		}
		c.give(cn)
	}
	for {
		if err != nil {
			// The request for the next part failed: when it never left, or the server refused
			// it for a moment, the stream is still open there.
			giveUp()
			return lost(err, true)
		}
		if cn.resp.Result != wire.ResultStream {
			cn.broken = true
			c.give(cn)
			return fmt.Errorf("%w: the server answered a stream with something else", ErrProtocol)
		}
		cursor = cn.resp.Stream.Cursor
		if _, werr := w.Write(cn.resp.Stream.Data); werr != nil {
			giveUp()
			return werr
		}
		if cursor == 0 {
			c.give(cn)
			return nil
		}
		req := cn.begin(database)
		req.Op, req.Cursor = wire.OpNext, cursor
		err = cn.exchange(ctx)
	}
}

// RestoreOption says how a backup is restored.
type RestoreOption func(*restoring)

type restoring struct{ passphrase string }

// BackupPassphrase is the passphrase a backup was made under, for a backup of another server
// than the one that restores it: what is restored is then encrypted with the passphrase of the
// server it is restored on. A server restores its own backups without it.
func BackupPassphrase(passphrase string) RestoreOption {
	return func(m *restoring) { m.passphrase = passphrase }
}

// Restore makes the backup read from r the database of that name, all or nothing: the backup
// is built beside the databases of the server while it is sent, and is given the name in one
// step once it is whole. A database that was there under the name is replaced at that moment:
// what programs had open on it ends, and the server keeps it for a time, for an undo (see
// Kept and BringBack). When Restore fails nothing is changed. It is an administrator's.
func (c *Client) Restore(ctx context.Context, database string, r io.Reader, options ...RestoreOption) error {
	var how restoring
	for _, option := range options {
		if option != nil {
			option(&how)
		}
	}
	_, err := c.upload(ctx, r, func(req *wire.Request) {
		req.Op, req.Restore = wire.OpRestore, wire.Restore{Name: database, Passphrase: how.passphrase}
	})
	return err
}

// ImportOption says how an export is imported.
type ImportOption func(*importing)

type importing struct{ replace, skip bool }

// AsNewDatabase has the lines that are imported become the database of that name, all or
// nothing, in the place of what was there, as a backup that is restored does: the database
// that was there is kept by the server for a time, for an undo. Only a whole export is taken
// so: one that Export wrote, to its last line, which says where it ends. Without it the lines
// are added to the database as it is. It is an administrator's.
func AsNewDatabase() ImportOption { return func(m *importing) { m.replace = true } }

// SkipRefusedLines has an import pass over the lines it refuses and carry on to the end,
// instead of stopping at the first: they are listed in its report.
func SkipRefusedLines() ImportOption { return func(m *importing) { m.skip = true } }

// ImportReport is what an import did.
type ImportReport struct {
	// Lines is the number of lines read that held something.
	Lines int64
	// Documents is the number of documents stored, and Indexes the number of index
	// declarations honoured.
	Documents int64
	Indexes   int
	// Refused is the number of lines that were refused and passed over (SkipRefusedLines),
	// and Refusals lists them in the order they came, the first thousand of them.
	Refused  int64
	Refusals []*ImportError
}

// ImportError says which line of an import was refused, and why.
type ImportError struct {
	// Line is the number of the line, counted from one.
	Line int
	// Err is why the line was refused: the error the write of that line would have returned.
	Err error
}

func (e *ImportError) Error() string {
	return fmt.Sprintf("syngodrv: line %d of the import: %v", e.Line, e.Err)
}

// Unwrap returns why the line was refused.
func (e *ImportError) Unwrap() error { return e.Err }

// Import reads what Export wrote from r, and adds it to the database of that name: the indexes
// are declared and the documents stored, each as Put would store it. It stops at the first
// line the server refuses, with an *ImportError that names the line: what came before stays.
// With SkipRefusedLines it passes over such lines and lists them in its report. With
// AsNewDatabase the lines become the database, all or nothing. Either way the report says what
// was done. Adding to a database is a writer's.
func (c *Client) Import(ctx context.Context, database string, r io.Reader, options ...ImportOption) (ImportReport, error) {
	var how importing
	for _, option := range options {
		if option != nil {
			option(&how)
		}
	}
	did, err := c.upload(ctx, r, func(req *wire.Request) {
		req.Op, req.Import = wire.OpImport, wire.Import{Name: database, Replace: how.replace, SkipRefused: how.skip}
	})
	var report ImportReport
	if did == nil {
		return report, err
	}
	report = ImportReport{Lines: did.Lines, Documents: did.Documents, Indexes: int(did.Indexes), Refused: did.Refused}
	for i := range did.Refusals {
		report.Refusals = append(report.Refusals, refusalOf(&did.Refusals[i]))
	}
	if err == nil && did.Stopped.Line != 0 {
		err = refusalOf(&did.Stopped)
	}
	return report, err
}

func refusalOf(r *wire.Refusal) *ImportError {
	return &ImportError{Line: int(r.Line), Err: errorOf(&r.Error)}
}

// upload sends what r holds to the server, a part at a time, on one connection: begin fills
// the request that begins the upload. It returns the report of an import.
func (c *Client) upload(ctx context.Context, r io.Reader, begin func(req *wire.Request)) (*wire.Report, error) {
	c.changing.RLock()
	defer c.changing.RUnlock()
	cn, err := c.take(ctx)
	if err != nil {
		return nil, err
	}
	if !cn.fresh && cn.waited > trusted && ended(cn.nc) {
		// A connection that ended while it waited in the pool: the upload begins on a new one.
		if cn, err = c.again(ctx, cn); err != nil {
			return nil, err
		}
	}
	begin(cn.begin(""))
	err = cn.exchange(ctx)
	if err != nil && !cn.fresh && (errors.Is(err, errUnsent) || (errors.Is(err, errUnanswered) && ctx.Err() == nil)) {
		// A connection that waited in the pool and is dead. The beginning of an upload is
		// begun again on a new one: what the dead one began, if the server heard of it at all,
		// ended with its session.
		if cn, err = c.again(ctx, cn); err != nil {
			return nil, err
		}
		begin(cn.begin(""))
		err = cn.exchange(ctx)
	}
	if err != nil {
		c.give(cn)
		return nil, lost(err, false)
	}
	// failed ends an upload that cannot go on. The connection goes back to the pool, and the
	// next to use it must not find an upload of somebody else's open on it: the server is told
	// that the rest will not come, unless it said itself that the upload is over (a part or an
	// end that is answered with an error ends it there), or the connection is no more. An
	// upload the server no longer has is answered so, which is as good.
	failed := func(err error) (*wire.Report, error) {
		var said *ServerError
		if !cn.broken && !errors.As(err, &said) {
			req := cn.begin("")
			req.Op, req.Abandon = wire.OpEnd, true
			stop, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if gone := cn.exchange(stop); gone != nil && !errors.Is(gone, ErrStale) {
				cn.broken = true
			}
			cancel()
		}
		c.give(cn)
		return nil, err
	}
	// The report is the server's memory of the connection until the next request: it is
	// copied before the connection is given back.
	report := func() *wire.Report {
		if cn.resp.Result != wire.ResultReport {
			return nil
		}
		did := cn.resp.Report
		did.Refusals = append([]wire.Refusal(nil), did.Refusals...)
		return &did
	}
	buf := make([]byte, partSize)
	for {
		n, rerr := io.ReadFull(r, buf)
		if n > 0 {
			req := cn.begin("")
			req.Op, req.Part = wire.OpPart, buf[:n]
			err := cn.exchange(ctx)
			req.Part = nil
			if err != nil {
				return failed(lost(err, false))
			}
			if did := report(); did != nil {
				// An import that stopped at a line: it is over.
				c.give(cn)
				return did, nil
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return failed(rerr)
		}
	}
	req := cn.begin("")
	req.Op, req.Abandon = wire.OpEnd, false
	if err := cn.exchange(ctx); err != nil {
		return failed(lost(err, false))
	}
	did := report()
	c.give(cn)
	return did, nil
}

// Compact has the database of that name give back the room of what was removed or replaced.
// Nothing needs it to be right: the server does the same by itself, little by little. It is an
// administrator's.
func (c *Client) Compact(ctx context.Context, database string) error {
	return c.call(ctx, database, true, func(req *wire.Request) { req.Op = wire.OpCompact }, nil)
}

// Measure sets right what the server takes for the largest document of a collection, and
// returns it, in bytes. The server makes room for a document of that size before every read of
// the collection, and what it takes only ever rises: a collection whose large documents are
// gone is read as slowly as when it had them, until it is measured. It is an administrator's.
func (c *Client) Measure(ctx context.Context, database, collection string) (bound int64, err error) {
	err = c.call(ctx, database, true, func(req *wire.Request) { req.Op, req.Measure = wire.OpMeasure, collection }, func(resp *wire.Response) {
		bound = resp.Number
	})
	return bound, err
}

// DatabaseInfo is a database of the server, with since when it is and what it takes on disk.
type DatabaseInfo struct {
	Name    string
	Created time.Time
	Bytes   int64
}

// DatabaseList lists the databases of the server, in the order of their names, each with since
// when it is a database and what it takes on disk.
func (c *Client) DatabaseList(ctx context.Context) (list []DatabaseInfo, err error) {
	err = c.call(ctx, "", true, func(req *wire.Request) { req.Op, req.Details = wire.OpDatabases, true }, func(resp *wire.Response) {
		list = make([]DatabaseInfo, len(resp.Databases))
		for i, d := range resp.Databases {
			list[i] = DatabaseInfo{Name: d.Name, Created: time.Unix(0, d.Created), Bytes: d.Bytes}
		}
	})
	return list, err
}

// Kept is a database as it was until it was replaced, by a restore or by an import as a new
// database: the server keeps it for a time, for an undo.
type Kept struct {
	// ID names it, for BringBack and Discard.
	ID uint64
	// Name is the database it was, and Created since when it was that database.
	Name    string
	Created time.Time
	// At is when it was replaced, and Until when the server removes it.
	At    time.Time
	Until time.Time
	// Bytes is what it takes on disk.
	Bytes int64
}

// Kept lists what the server keeps of the databases that were replaced, the oldest first. It is
// an administrator's.
func (c *Client) Kept(ctx context.Context) (list []Kept, err error) {
	err = c.call(ctx, "", true, func(req *wire.Request) { req.Op = wire.OpKept }, func(resp *wire.Response) {
		list = make([]Kept, len(resp.Kept))
		for i, k := range resp.Kept {
			list[i] = Kept{ID: k.ID, Name: k.Name, Created: time.Unix(0, k.Created), At: time.Unix(0, k.At), Until: time.Unix(0, k.Until), Bytes: k.Bytes}
		}
	})
	return list, err
}

// BringBack undoes a replacement: the database is again what the kept one of that number was.
// What the database is at that moment is kept in its turn, so that the undo can be undone. It
// is an administrator's.
func (c *Client) BringBack(ctx context.Context, id uint64) error {
	return c.call(ctx, "", false, func(req *wire.Request) { req.Op, req.KeptID = wire.OpBringBack, id }, nil)
}

// Discard removes what is kept under that number, now: there is no undo of it. It is an
// administrator's.
func (c *Client) Discard(ctx context.Context, id uint64) error {
	return c.call(ctx, "", false, func(req *wire.Request) { req.Op, req.KeptID = wire.OpDiscard, id }, nil)
}
