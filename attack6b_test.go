package syngodrv

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/syngodb/syngodrv/wire"
)

// A program restores the backup of another server, and gives the passphrase it was made under.
// The rule: a passphrase is in no kept request. The server forgets it once the request was
// served (wire.Request.Forget); the connection the driver gives back to its pool must not hold
// it either, for as long as the connection lives.
func TestAttack6BAPooledConnectionKeepsNoPassphrase(t *testing.T) {
	const secret = "the passphrase of another server"
	c := &Client{slots: make(chan struct{}, 8)}
	c.cfg.user, c.cfg.password, c.cfg.hasLogin = "ann", "a password", true
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
	if err := c.Restore(context.Background(), "shop", strings.NewReader(strings.Repeat("what is restored. ", 60)), BackupPassphrase(secret)); err != nil {
		t.Fatalf("the restore: %v", err)
	}
	if len(c.idle) != 1 || c.idle[0] != cn {
		t.Fatalf("the premise of this test does not hold: the connection is not back in the pool (%d there)", len(c.idle))
	}
	if cn.req.Restore.Passphrase != "" {
		t.Errorf("the connection that is back in the pool keeps the request that began the restore, with its passphrase: %q", cn.req.Restore.Passphrase)
	}
	if bytes.Contains(cn.out[:cap(cn.out)], []byte(secret)) {
		t.Errorf("the connection that is back in the pool keeps the passphrase in the memory its requests are written from")
	}
}
