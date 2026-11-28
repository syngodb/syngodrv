package syngodrv

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/syngodb/syngo"
	"github.com/syngodb/syngodrv/wire"
)

// protocol is the version of the protocol this driver speaks, and alpn the name a TLS connection
// asks for it by.
const (
	protocol = 1
	alpn     = "syngo"
)

// cancelGrace is how long a request that was cancelled waits for the server to say so, before
// the connection is given up.
const cancelGrace = 5 * time.Second

// conn is one connection to the server: a session, which carries one request at a time. Whoever
// has it has it alone, from the pool until it goes back.
type conn struct {
	client *Client
	nc     net.Conn
	in     *wire.Reader
	out    []byte
	// req is the request being sent and resp the answer to it: both are used again for the next.
	req  wire.Request
	resp wire.Response
	// number is the number of the last request sent.
	number  uint32
	welcome wire.Welcome
	// wmu is held by whoever writes to the connection: the request, or the frame that asks the
	// server to stop it, which is written from elsewhere when the program's context ends.
	wmu sync.Mutex
	// answered is the number of the last request whose answer was read, under wmu.
	answered uint32
	// broken says the connection is of no further use.
	broken bool
	// fresh says the connection was made for the request it is serving, not taken from the pool:
	// when it fails, the server is not there, and asking again would not help.
	fresh bool
	// used is when the connection went back to the pool, and waited how long it stayed there
	// before it was taken this time.
	used   time.Time
	waited time.Duration
	// small counts the requests in a row that took little memory.
	small int
	// epoch says which of the client's passwords the connection logged in with.
	epoch uint64
}

// keepOut is the most memory a connection holds on to for its requests once they are small
// again, and patience how many small ones in a row it takes to believe that they are.
const (
	keepOut  = 4 << 10
	patience = 32
)

// dial makes a connection and logs in.
func (c *Client) dial(ctx context.Context) (*conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, c.addr.network, c.addr.address)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConnection, err)
	}
	// Everything up to the welcome has the time the program's context leaves it, and no more
	// than a moment when the context sets no limit.
	limit, ok := ctx.Deadline()
	if !ok {
		limit = time.Now().Add(30 * time.Second)
	}
	_ = nc.SetDeadline(limit)
	if c.tls != nil {
		t := tls.Client(nc, c.tls)
		if err := t.HandshakeContext(ctx); err != nil {
			_ = nc.Close()
			return nil, fmt.Errorf("%w: %w", ErrConnection, err)
		}
		nc = t
	}
	cn := &conn{client: c, nc: nc, in: wire.NewReader(nc), fresh: true}
	// The documents of an answer are copied before the connection reads anything else.
	cn.resp.AliasIDs = true
	user, password, has, epoch := c.login()
	cn.epoch = epoch
	hello := wire.Hello{ProtocolMin: protocol, ProtocolMax: protocol, Driver: "syngodrv " + Version, Program: c.cfg.program,
		HasPassword: has, User: user, Password: password}
	cn.out = hello.Append(wire.Begin(cn.out))
	if err := wire.Send(nc, wire.LoginFrame, wire.KindHello, 0, cn.out); err != nil {
		_ = nc.Close()
		return nil, fmt.Errorf("%w: %w", ErrConnection, err)
	}
	kind, _, payload, err := cn.in.Next()
	if err != nil {
		_ = nc.Close()
		return nil, fmt.Errorf("%w: %w", ErrConnection, err)
	}
	switch kind {
	case wire.KindWelcome:
		if !cn.welcome.Decode(payload) || cn.welcome.Protocol != protocol || cn.welcome.MaxFrame == 0 {
			_ = nc.Close()
			return nil, fmt.Errorf("%w: the welcome of the server is not one", ErrProtocol)
		}
	case wire.KindResponse:
		_ = nc.Close()
		if !cn.resp.Decode(payload) || cn.resp.Result != wire.ResultError {
			return nil, fmt.Errorf("%w: the server answered the hello with something else than a welcome", ErrProtocol)
		}
		return nil, errorOf(&cn.resp.Error)
	default:
		_ = nc.Close()
		return nil, fmt.Errorf("%w: the server answered the hello with something else than a welcome", ErrProtocol)
	}
	_ = nc.SetDeadline(time.Time{})
	cn.in.Limits(int(cn.welcome.MaxFrame), int(min(cn.welcome.MaxMessage, 1<<40)))
	c.message.Store(cn.welcome.MaxMessage)
	return cn, nil
}

// begin empties the request of the connection for the caller to fill.
func (cn *conn) begin(database string) *wire.Request {
	cn.req.Timeout, cn.req.Database, cn.req.Op = 0, database, 0
	return &cn.req
}

// A request fails in one of three ways, which the callers tell apart:
//
//   - the server answered with an error: the connection is as good as before;
//   - errUnsent: the connection failed before the request had left whole. The server did nothing;
//   - errUnanswered: the request left and no answer came. What the server did is not known.
var (
	errUnsent     = errors.New("the request did not leave")
	errUnanswered = errors.New("no answer came")
)

// exchange sends the request that is in req and reads its answer into resp. An error of the
// server is returned as the error it stands for; see errUnsent and errUnanswered for the others.
//
// A request that carries a secret (a password, the passphrase of a backup) is forgotten once
// it was exchanged, and so is the message it was written into: a connection waits in the pool
// for minutes, and holds nothing there that it has no use for.
func (cn *conn) exchange(ctx context.Context) error {
	err := cn.exchanged(ctx)
	if cn.req.Secret() {
		cn.req.Forget()
		clear(cn.out[:cap(cn.out)])
	}
	return err
}

// exchanged is exchange, with the request still in req afterwards.
func (cn *conn) exchanged(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		err := cn.once(ctx)
		if err != nil {
			return err
		}
		if cn.resp.Result != wire.ResultError {
			return nil
		}
		e := &cn.resp.Error
		switch e.Code {
		case wire.CodeBusy:
			// The server asks for a moment: it gets it, and the request is sent again.
			wait := time.NewTimer(time.Duration(max(e.WaitMS, 1)) * time.Millisecond)
			select {
			case <-wait.C:
			case <-ctx.Done():
				wait.Stop()
				return ctx.Err()
			}
			continue
		case wire.CodeCancelled:
			if err := ctx.Err(); err != nil {
				return err
			}
			// The server stopped waiting a moment before this program's own clock did.
			return context.DeadlineExceeded
		case wire.CodeProtocol:
			cn.broken = true
		}
		return errorOf(e)
	}
}

// once is one request and its answer.
func (cn *conn) once(ctx context.Context) error {
	if limit, ok := ctx.Deadline(); ok {
		left := time.Until(limit)
		if left <= 0 {
			return context.DeadlineExceeded
		}
		cn.req.Timeout = uint32(min(left/time.Millisecond+1, 1<<31))
	}
	cn.number++
	number := cn.number
	cn.out = cn.req.Append(wire.Begin(cn.out))
	if uint64(len(cn.out)-wire.HeaderSize) > cn.welcome.MaxMessage {
		return fmt.Errorf("%w: the request takes %d bytes, and the server takes %d in one", syngo.ErrTooLarge, len(cn.out)-wire.HeaderSize, cn.welcome.MaxMessage)
	}
	// When the program's context ends before the answer comes, the server is asked to stop.
	var unwatch func() bool
	if ctx.Done() != nil {
		unwatch = context.AfterFunc(ctx, func() { cn.interrupt(number) })
	}
	cn.wmu.Lock()
	err := wire.Send(cn.nc, int(cn.welcome.MaxFrame), wire.KindRequest, number, cn.out)
	cn.wmu.Unlock()
	// The memory of a large request is kept while large ones keep coming.
	if len(cn.out) > keepOut {
		cn.small = 0
	} else if cn.small < patience {
		cn.small++
	} else if cap(cn.out) > keepOut {
		cn.out = nil
	}
	if err != nil {
		cn.broken = true
		if unwatch != nil {
			unwatch()
		}
		return fmt.Errorf("%w: %w", errUnsent, err)
	}
	var kind byte
	var payload []byte
	for {
		var got uint32
		if kind, got, payload, err = cn.in.Next(); err != nil || kind != wire.KindResponse || got == number {
			break
		}
		// The answer to a request this connection gave up on: it is passed over.
	}
	if unwatch != nil && !unwatch() {
		// The context ended: the frame that says so is being written, or was. It is let finish,
		// so that it cannot fall into the middle of the next request.
		cn.wmu.Lock()
		cn.answered = number
		cn.wmu.Unlock()
		_ = cn.nc.SetReadDeadline(time.Time{})
	} else {
		cn.answered = number
	}
	switch {
	case err != nil:
		cn.broken = true
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("%w: %w", errUnanswered, ctxErr)
		}
		return fmt.Errorf("%w: %w", errUnanswered, err)
	case kind != wire.KindResponse || !cn.resp.Decode(payload) || cn.resp.Result == 0:
		cn.broken = true
		return fmt.Errorf("%w: %w: what the server sent is no answer", errUnanswered, ErrProtocol)
	}
	return nil
}

// interrupt asks the server to stop the request of that number, and gives it a moment to answer.
func (cn *conn) interrupt(number uint32) {
	cn.wmu.Lock()
	defer cn.wmu.Unlock()
	if cn.answered >= number {
		return
	}
	_ = wire.Cancel(cn.nc, number)
	_ = cn.nc.SetReadDeadline(time.Now().Add(cancelGrace))
}

func (cn *conn) close() {
	cn.broken = true
	_ = cn.nc.Close()
}
