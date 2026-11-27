package syngodrv

import (
	"io"
	"net"
	"testing"
	"time"
)

// A client that is given another password lets go of the connections it made with the old
// one: those that wait in its pool are closed at once, new ones log in with the new password,
// and one that was in use when the password changed is closed when it comes back, and not
// used again. The server ends every session of the old password, so none of them is of use.
func TestAClientLetsGoOfTheConnectionsOfItsOldPassword(t *testing.T) {
	c := &Client{slots: make(chan struct{}, 8)}
	c.cfg.user, c.cfg.password, c.cfg.hasLogin = "ann", "old", true
	pipe := func() (*conn, net.Conn) {
		near, far := net.Pipe()
		t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
		_, _, _, epoch := c.login()
		return &conn{client: c, nc: near, epoch: epoch, used: time.Now()}, far
	}
	ended := func(far net.Conn) bool {
		_ = far.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := far.Read(make([]byte, 1))
		return err == io.EOF
	}
	waiting, farWaiting := pipe()
	inUse, farInUse := pipe()
	c.idle = append(c.idle, waiting)
	c.slots <- struct{}{} // the one that is in use holds a place

	c.newPassword("new")
	if user, password, has, epoch := c.login(); user != "ann" || password != "new" || !has || epoch != inUse.epoch+1 {
		t.Errorf("after another password the client logs in as %q with %q (%v), in its epoch %d", user, password, has, epoch)
	}
	if len(c.idle) != 0 || !ended(farWaiting) {
		t.Errorf("a connection of the old password still waits in the pool: %d", len(c.idle))
	}
	// The one that was in use comes back: it is closed, and its place is free again.
	c.give(inUse)
	if len(c.idle) != 0 || !ended(farInUse) || len(c.slots) != 0 {
		t.Errorf("a connection of the old password was kept when it came back: %d in the pool, %d places taken", len(c.idle), len(c.slots))
	}
	// One of the new password is kept, as ever.
	fresh, farFresh := pipe()
	c.slots <- struct{}{}
	c.give(fresh)
	if len(c.idle) != 1 || len(c.slots) != 0 {
		t.Errorf("a connection of the new password was not kept: %d in the pool", len(c.idle))
	}
	// And one of an older password that is still in the pool, were there ever one, is not
	// handed out: it is closed, and a new one would be made in its place.
	old, farOld := pipe()
	old.epoch--
	c.mu.Lock()
	c.idle = append(c.idle, old)
	c.mu.Unlock()
	c.slots <- struct{}{}
	c.mu.Lock()
	top := c.idle[len(c.idle)-1]
	c.mu.Unlock()
	if top != old {
		t.Fatal("the test did not put the old connection on top")
	}
	// take would dial for a new one once the old is skipped: the test asks only what take
	// does with the two that wait, by taking as take does.
	if cn := c.pooled(time.Now()); cn != fresh {
		t.Errorf("the pool handed out %v, and the connection of the new password was to be", cn)
	}
	if !ended(farOld) {
		t.Error("a connection of an older password that waited in the pool was not closed")
	}
	_ = farFresh
}
