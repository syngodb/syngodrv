package syngodrv

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/syngodb/syngodrv/wire"
)

// A password is in no memory the driver keeps. A request is written from memory that the
// connection keeps for its next request: when the request that held the password is the last
// one, or the longest, nothing writes over it, and the password would lie there for as long as
// the connection waits in the pool. It is wiped once the request was exchanged.
func TestAConnectionKeepsNoPasswordInTheMemoryItWritesFrom(t *testing.T) {
	const old, secret = "the password this user had until now", "the password this user is given from now on"
	c := &Client{slots: make(chan struct{}, 8)}
	c.cfg.user, c.cfg.password, c.cfg.hasLogin = "ann", old, true
	near, far := net.Pipe()
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	_, _, _, epoch := c.login()
	cn := &conn{client: c, nc: near, in: wire.NewReader(near), epoch: epoch, used: time.Now()}
	cn.welcome.MaxFrame, cn.welcome.MaxMessage = wire.MaxFrame, 32<<20
	cn.in.Limits(int(cn.welcome.MaxFrame), int(cn.welcome.MaxMessage))
	c.idle = append(c.idle, cn)
	// A server that takes every request.
	go func() {
		in := wire.NewReader(far)
		in.Limits(wire.MaxFrame, 32<<20)
		done := wire.Response{Result: wire.ResultDone}
		for {
			kind, number, _, err := in.Next()
			if err != nil || kind != wire.KindRequest {
				return
			}
			if wire.Send(far, wire.MaxFrame, wire.KindResponse, number, done.Append(wire.Begin(nil))) != nil {
				return
			}
		}
	}()
	ctx := context.Background()
	if err := c.PutUser(ctx, "bob", Read, secret); err != nil {
		t.Fatalf("the user: %v", err)
	}
	if len(c.idle) != 1 || c.idle[0] != cn {
		t.Fatalf("the premise of this test does not hold: the connection is not back in the pool (%d there)", len(c.idle))
	}
	if bytes.Contains(cn.out[:cap(cn.out)], []byte(secret)) {
		t.Errorf("the connection that is back in the pool keeps the password of a user in the memory its requests are written from")
	}
	// A request without a secret is not what this is about, and is left as it is: the memory
	// is written over by the next request anyway.
	if err := c.Ping(ctx); err != nil {
		t.Fatalf("a request after it: %v", err)
	}
}
