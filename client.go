package syngodrv

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syngodb/syngo"
	"github.com/syngodb/syngodrv/wire"
)

// Version is the version of the driver, which it tells the server for its log.
const Version = "0.1.0"

// DefaultPort is the port a server listens on when its configuration names none.
const DefaultPort = "31415"

// What a server says that the embedded library never does. The errors of the public package
// (syngo.ErrNotFound and the others) come back as themselves.
var (
	// ErrConnection is returned when the server could not be reached, or the connection to it
	// was lost before anything was asked of it.
	ErrConnection = errors.New("syngodrv: no connection to the server")
	// ErrUnauthenticated is returned when the server refuses the login, or the user of the
	// connection is no longer one.
	ErrUnauthenticated = errors.New("syngodrv: the server refused the login")
	// ErrForbidden is returned for what the role of the user does not allow.
	ErrForbidden = errors.New("syngodrv: the role of the user does not allow this")
	// ErrNoDatabase is returned for the name of no database of the server.
	ErrNoDatabase = errors.New("syngodrv: no database of that name")
	// ErrStale is returned for what a request stood on and is no longer there: a page token of
	// data that was replaced, an answer the server gave up, a transaction of a database that
	// was dropped.
	ErrStale = errors.New("syngodrv: what the request stood on is no longer there")
	// ErrUnsupported is returned for what the server does not know how to do.
	ErrUnsupported = errors.New("syngodrv: the server does not know this request")
	// ErrProtocol is returned when the server and the driver do not understand each other.
	ErrProtocol = errors.New("syngodrv: not the protocol")
	// ErrServer is returned when the server failed to serve a request; its log says more.
	ErrServer = errors.New("syngodrv: the server failed")
)

// ServerError is an error only a server reports: what it said, and which of the errors above it
// is.
type ServerError struct {
	Message string
	of      error
}

// Error is what the server said.
func (e *ServerError) Error() string { return "syngodrv: " + e.Message }

// Unwrap is the error of this package the server's error is.
func (e *ServerError) Unwrap() error { return e.of }

// errorOf is the Go error an error of the server stands for.
func errorOf(e *wire.Error) error {
	if err := e.Err(); err != nil {
		return err
	}
	of := ErrServer
	switch e.Code {
	case wire.CodeUnauthenticated:
		of = ErrUnauthenticated
	case wire.CodeForbidden:
		of = ErrForbidden
	case wire.CodeNoDatabase:
		of = ErrNoDatabase
	case wire.CodeStale:
		of = ErrStale
	case wire.CodeUnsupported:
		of = ErrUnsupported
	case wire.CodeProtocol:
		of = ErrProtocol
	case wire.CodeBusy:
		// Only a login is answered so for good: the server had no turn left for it.
		of = ErrConnection
	}
	return &ServerError{Message: e.Message, of: of}
}

// Option configures a client at Connect, or a database handle at Open. Both open well with a
// login and nothing else.
type Option func(*config)

type config struct {
	user, password string
	hasLogin       bool
	tls            *tls.Config
	unverified     bool
	pool           int
	program        string
}

// Login is the user and the password the client logs in with. They are given here and never in
// an address, so that an address can be shown and logged.
func Login(user, password string) Option {
	return func(c *config) { c.user, c.password, c.hasLogin = user, password, true }
}

// TLS gives the rules the connections of a syngodbs:// address are made by: the authorities the
// server's certificate is checked against, a certificate of the client's own. Without it the
// certificate of the server is checked against the authorities of the machine.
func TLS(rules *tls.Config) Option { return func(c *config) { c.tls = rules } }

// CertificateNotVerified makes the client take whatever certificate the server shows: the
// connection is encrypted, and nothing says who is at the other end. It is for a server on the
// same machine that signed its own certificate, and is what ?insecure=true says in an address.
func CertificateNotVerified() Option { return func(c *config) { c.unverified = true } }

// Connections is the most connections the client keeps: as many requests can be under way at
// once, and one more waits for a connection to be free. The default is four for each processor,
// and at least 16.
func Connections(n int) Option { return func(c *config) { c.pool = n } }

// Program names the program to the server, for its log.
func Program(name string) Option { return func(c *config) { c.program = name } }

// address is where a server is, as the address a program gave says it.
type address struct {
	network, address string
	host             string
	database         string
	// secure says the connection is made with TLS, and insecure that whatever certificate the
	// server shows is taken.
	secure, insecure bool
}

// parse reads an address:
//
//	syngodbs://host[:port][/database][?insecure=true]   TCP with TLS
//	syngodb://host[:port][/database]                    TCP without
//	syngodb+unix:///path/to/socket[?database=name]      the Unix socket of the same machine
//
// On Windows the path of a socket is written as in the address of any file, with its drive after
// the third stroke: syngodb+unix:///C:/path/to/socket.
func parse(text string) (address, error) {
	u, err := url.Parse(text)
	if err != nil {
		return address{}, fmt.Errorf("syngodrv: the address %q: %w", text, err)
	}
	if u.User != nil {
		return address{}, fmt.Errorf("syngodrv: the address %q names a user: the user and the password are given with Login, never in an address", u.Redacted())
	}
	query := u.Query()
	switch u.Scheme {
	case "syngodb", "syngodbs":
		where := address{network: "tcp", host: u.Hostname(), database: strings.TrimPrefix(u.Path, "/"), secure: u.Scheme == "syngodbs"}
		for name, values := range query {
			if name != "insecure" || len(values) != 1 || (values[0] != "true" && values[0] != "false") {
				return address{}, fmt.Errorf("syngodrv: the address %q: all that may follow the question mark is insecure=true", text)
			}
			where.insecure = values[0] == "true"
		}
		switch {
		case where.host == "" || u.Fragment != "":
			return address{}, fmt.Errorf("syngodrv: the address %q is not %s://host[:port][/database]", text, u.Scheme)
		case where.insecure && !where.secure:
			return address{}, fmt.Errorf("syngodrv: the address %q asks for no TLS (syngodb://) and says a certificate need not be checked: there is none to check", text)
		}
		port := u.Port()
		if port == "" {
			port = DefaultPort
		}
		where.address = net.JoinHostPort(where.host, port)
		return where, nil
	case "syngodb+unix":
		if u.Host != "" || u.Path == "" || u.Fragment != "" || len(query) > 1 || (len(query) == 1 && !query.Has("database")) {
			return address{}, fmt.Errorf("syngodrv: the address %q is not syngodb+unix:///path/to/socket[?database=name]", text)
		}
		return address{network: "unix", address: socketPath(u.Path, runtime.GOOS == "windows"), database: query.Get("database")}, nil
	}
	return address{}, fmt.Errorf("syngodrv: the address %q begins with none of syngodbs://, syngodb:// and syngodb+unix://", text)
}

// Client is a server, as a program sees it: the databases of the server are reached through it,
// and made and dropped with it. It keeps a pool of connections that all of them share, and is
// safe for use by any number of goroutines.
type Client struct {
	addr address
	cfg  config
	tls  *tls.Config

	mu     sync.Mutex
	idle   []*conn
	closed bool
	// slots has room for as many connections as the client may keep: whoever wants one takes a
	// place first, and gives it back with the connection.
	slots chan struct{}
	// message is the most one message may hold, as the server said when a connection was made.
	message atomic.Uint64
}

// room is how much of one message the documents of a PutMany may take.
func (c *Client) room() int {
	return int(min(max(c.message.Load(), 128<<10)-64<<10, 1<<30))
}

// socketPath is the path of a socket as the system takes it, from the path of an address. On
// Windows the stroke that stands before the drive goes, and the others are turned.
func socketPath(path string, windows bool) string {
	if !windows {
		return path
	}
	if len(path) >= 3 && path[0] == '/' && path[2] == ':' && (path[1]|0x20 >= 'a' && path[1]|0x20 <= 'z') {
		path = path[1:]
	}
	return strings.ReplaceAll(path, "/", `\`)
}

// Connect makes a client of the server at an address:
//
//	syngodbs://host:port            TCP with TLS, the certificate of the server checked
//	syngodbs://host                 the same, on port 31415
//	syngodbs://host?insecure=true   the same, whatever certificate the server shows
//	syngodb://host:port             TCP without TLS, which a server takes from its own machine only
//	syngodb+unix:///path/to/sock    the Unix socket of a server on the same machine
//	syngodb+unix:///C:/path/to/sock the same on Windows
//
// These are the addresses SyngoDB Server 6 took: an address written for it still says where the
// server is.
//
// One connection is made at once, so that an address or a login that is wrong is known here and
// not at the first request.
func Connect(ctx context.Context, addr string, options ...Option) (*Client, error) {
	where, err := parse(addr)
	if err != nil {
		return nil, err
	}
	if where.database != "" {
		return nil, fmt.Errorf("syngodrv: the address %q names a database, which Open takes; a client is of the whole server", addr)
	}
	return connect(ctx, where, options)
}

func connect(ctx context.Context, where address, options []Option) (*Client, error) {
	c := &Client{addr: where}
	for _, option := range options {
		if option != nil {
			option(&c.cfg)
		}
	}
	if c.cfg.pool <= 0 {
		c.cfg.pool = max(16, 4*runtime.GOMAXPROCS(0))
	}
	c.slots = make(chan struct{}, c.cfg.pool)
	if where.secure {
		if c.tls = c.cfg.tls.Clone(); c.tls == nil {
			c.tls = &tls.Config{}
		}
		if c.tls.ServerName == "" {
			c.tls.ServerName = where.host
		}
		if c.cfg.unverified || where.insecure {
			c.tls.InsecureSkipVerify = true
		}
		c.tls.NextProtos = []string{alpn}
		if c.tls.MinVersion < tls.VersionTLS12 {
			c.tls.MinVersion = tls.VersionTLS12
		}
	}
	cn, err := c.take(ctx)
	if err != nil {
		return nil, err
	}
	c.give(cn)
	return c, nil
}

// trusted is how long a connection may have waited in the pool and still be handed a write
// without being asked first whether it is still there: asking costs a call to the system.
const trusted = time.Second

// take takes a connection out of the pool, making one when none is idle.
func (c *Client) take(ctx context.Context) (*conn, error) {
	select {
	case c.slots <- struct{}{}:
	default:
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	now := time.Now()
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			<-c.slots
			return nil, syngo.ErrClosed
		}
		n := len(c.idle)
		if n == 0 {
			c.mu.Unlock()
			break
		}
		// The connection used last is taken first: it is the one most surely alive.
		cn := c.idle[n-1]
		c.idle[n-1] = nil
		c.idle = c.idle[:n-1]
		c.mu.Unlock()
		// One the server may be about to close for its silence is not trusted with a request.
		if limit := time.Duration(cn.welcome.IdleSeconds) * time.Second; limit > 0 && now.Sub(cn.used) > limit-limit/4 {
			cn.close()
			continue
		}
		cn.fresh, cn.waited = false, now.Sub(cn.used)
		return cn, nil
	}
	cn, err := c.dial(ctx)
	if err != nil {
		<-c.slots
		return nil, err
	}
	return cn, nil
}

// again makes a new connection in the place of one that failed, which is closed.
func (c *Client) again(ctx context.Context, failed *conn) (*conn, error) {
	failed.close()
	cn, err := c.dial(ctx)
	if err != nil {
		<-c.slots
		return nil, err
	}
	return cn, nil
}

// give puts a connection back in the pool, or closes it when it is of no further use.
func (c *Client) give(cn *conn) {
	if cn.broken {
		_ = cn.nc.Close()
		<-c.slots
		return
	}
	cn.used = time.Now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = cn.nc.Close()
		<-c.slots
		return
	}
	c.idle = append(c.idle, cn)
	c.mu.Unlock()
	<-c.slots
}

// Close closes the connections of the client. Every later operation, on the client and on the
// databases reached through it, returns syngo.ErrClosed. A transaction that is open, and an
// answer that is still read, keep their connection until they end: it is closed then. One that
// is never ended keeps it until the server ends the transaction and closes the silent session.
func (c *Client) Close() error {
	c.mu.Lock()
	idle := c.idle
	c.idle, c.closed = nil, true
	c.mu.Unlock()
	for _, cn := range idle {
		_ = cn.nc.Close()
	}
	return nil
}

// Database returns a handle on the database with that name. It never fails, sends nothing and
// costs nothing: a program may ask for one whenever it needs it. A name that is no database's
// shows at the first request, as ErrNoDatabase.
func (c *Client) Database(name string) *DB { return &DB{client: c, name: name} }

// call sends, on a connection of the pool, the request fill writes, and hands the answer to
// read. A request that asks for nothing to be changed (safe) is sent again when the connection
// it was sent on turns out to be dead.
func (c *Client) call(ctx context.Context, database string, safe bool, fill func(req *wire.Request), read func(resp *wire.Response)) error {
	cn, err := c.take(ctx)
	if err != nil {
		return err
	}
	if !safe && !cn.fresh && cn.waited > trusted && ended(cn.nc) {
		// A connection that ended while it waited: a write is not sent after it.
		if cn, err = c.again(ctx, cn); err != nil {
			return err
		}
	}
	fill(cn.begin(database))
	err = cn.exchange(ctx)
	if err != nil && !cn.fresh && (errors.Is(err, errUnsent) || (safe && errors.Is(err, errUnanswered) && ctx.Err() == nil)) {
		// A connection that waited in the pool and is dead: the request never reached the
		// server, or asked for nothing that could have been done twice. Once more, on a new one.
		if cn, err = c.again(ctx, cn); err != nil {
			return err
		}
		fill(cn.begin(database))
		err = cn.exchange(ctx)
	}
	if err == nil && read != nil {
		read(&cn.resp)
	}
	c.give(cn)
	return lost(err, safe)
}

// lost is the error a program is given for a request that failed with its connection.
func lost(err error, safe bool) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errUnsent):
		return fmt.Errorf("%w: %w", ErrConnection, unwrapped(err))
	case errors.Is(err, errUnanswered):
		cause := unwrapped(err)
		if safe {
			if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
				return cause
			}
			return fmt.Errorf("%w: %w", ErrConnection, cause)
		}
		// Something was to be written, and nobody says whether it was.
		return fmt.Errorf("%w (%w)", syngo.ErrOutcomeUnknown, cause)
	}
	return err
}

// unwrapped is what an error of a connection wraps: the cause, without the word of this package
// that says at which moment it struck.
func unwrapped(err error) error {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		if causes := joined.Unwrap(); len(causes) == 2 {
			return causes[1]
		}
	}
	return err
}

// Databases lists the names of the databases of the server, in order.
func (c *Client) Databases(ctx context.Context) (names []string, err error) {
	err = c.call(ctx, "", true, func(req *wire.Request) { req.Op = wire.OpDatabases }, func(resp *wire.Response) {
		names = append([]string(nil), resp.Names...)
	})
	return names, err
}

// EnsureDatabase makes sure the server has a database of that name: it makes one when there is
// none, and changes nothing when there is. A database is made on purpose, never by being named
// in a request for its data: a program asks for the ones it uses at every start, as it declares
// its indexes with EnsureIndex.
func (c *Client) EnsureDatabase(ctx context.Context, name string) error {
	return c.call(ctx, "", false, func(req *wire.Request) { req.Op, req.Name = wire.OpEnsureDatabase, name }, nil)
}

// DropDatabase removes a database with everything in it.
func (c *Client) DropDatabase(ctx context.Context, name string) error {
	return c.call(ctx, "", false, func(req *wire.Request) { req.Op, req.Name = wire.OpDropDatabase, name }, nil)
}

// Ping asks the server whether it is there, on one connection.
func (c *Client) Ping(ctx context.Context) error {
	return c.call(ctx, "", true, func(req *wire.Request) { req.Op = wire.OpPing }, nil)
}
