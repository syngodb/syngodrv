package syngodrv

import (
	"context"
	"strings"

	"github.com/syngodb/syngodrv/wire"
)

// The users of a server. They are the server's and not a database's: a user has one role, on
// every database. The embedded library has no users, so nothing here is part of what a program
// writes the same either way: it is what a program that sets a server up, or looks after one,
// asks of it.

// Role is what a user may do, on every database of the server.
type Role uint8

const (
	// Read lets a user read documents and ask queries.
	Read Role = 1
	// Write lets a user change documents and indexes as well.
	Write Role = 2
	// Admin lets a user do everything: databases, users, the server itself.
	Admin Role = 3
)

// String is the role as a configuration writes it: "read", "write" or "admin". Anything else
// reads "none".
func (r Role) String() string {
	switch r {
	case Read:
		return "read"
	case Write:
		return "write"
	case Admin:
		return "admin"
	}
	return "none"
}

// User is a user of the server as Users lists them: who, and what the user may do. Nothing of a
// password is ever listed.
type User struct {
	Name string
	Role Role
}

// PutUser makes a user, or changes one that is there. A role of zero keeps the role the user
// has, and an empty password the password: a new user is given both. Names are told apart
// without regard to case. It is an administrator's to ask, and a change that would leave the
// server without an administrator is refused with ErrLastAdmin.
//
// The password travels to the server as the login of a connection does, and the server keeps
// only what checks it.
//
// Another password ends what the user has open: the server closes every session that came in
// with the one before. When the user is the one the client is logged in as, the client logs
// in with the new password from then on, as after SetPassword.
func (c *Client) PutUser(ctx context.Context, name string, role Role, password string) error {
	fill := func(req *wire.Request) {
		req.Op, req.PutUser = wire.OpPutUser, wire.PutUser{Name: name, Role: uint8(role), Password: password}
	}
	if user, _, has, _ := c.login(); password == "" || !has || !strings.EqualFold(user, name) {
		return c.call(ctx, "", false, fill, nil)
	}
	// The client's own password: as SetPassword.
	c.changing.Lock()
	defer c.changing.Unlock()
	err := c.do(ctx, "", false, fill, nil)
	if err == nil {
		c.newPassword(password)
	}
	return err
}

// RemoveUser removes a user, and closes what the user has open. It is an administrator's to
// ask. The name of no user is ErrNoUser, and the last administrator is not removed
// (ErrLastAdmin).
func (c *Client) RemoveUser(ctx context.Context, name string) error {
	return c.call(ctx, "", false, func(req *wire.Request) { req.Op, req.Name = wire.OpRemoveUser, name }, nil)
}

// Users lists the users of the server, in the order of their names. It is an administrator's
// to ask.
func (c *Client) Users(ctx context.Context) (users []User, err error) {
	err = c.call(ctx, "", true, func(req *wire.Request) { req.Op = wire.OpUsers }, func(resp *wire.Response) {
		users = make([]User, len(resp.Users))
		for i, u := range resp.Users {
			users[i] = User{Name: u.Name, Role: Role(u.Role)}
		}
	})
	return users, err
}

// SetPassword gives the user the client is logged in as another password. Every user may ask it
// for themselves, whatever their role, and gives the password they have: the wrong one is
// answered as a failed login is (ErrUnauthenticated), after the same delay. From then on the
// client logs in with the new password. The server closes whatever else came in with the old
// one, the other connections of this client too: the client lets go of them, and makes new
// ones as it needs them. So that none of its requests meets a session that has just ended,
// the password is changed when no other request of the client is under way: it waits for
// those that are, and the ones that come meanwhile wait for it. A transaction that is open and
// an answer that is still pulled are on connections of the old password: they end with it.
//
// When the connection is lost before the answer, the password may be the old one or the new
// one, and the client goes on with the old: a program that meets that connects again with the
// one that is let in.
func (c *Client) SetPassword(ctx context.Context, old, new string) error {
	c.changing.Lock()
	defer c.changing.Unlock()
	err := c.do(ctx, "", false, func(req *wire.Request) {
		req.Op, req.SetPassword = wire.OpSetPassword, wire.SetPassword{Old: old, New: new}
	}, nil)
	if err == nil {
		c.newPassword(new)
	}
	return err
}

// newPassword is told that the user the client logs in as has another password. New
// connections log in with it. The ones the client has came in with the old one, and the
// server ends them: those that wait in the pool are closed here, and those that are in use
// are closed when they come back, so that no request is sent on a connection that is over.
func (c *Client) newPassword(password string) {
	c.mu.Lock()
	c.cfg.password = password
	c.epoch++
	idle := c.idle
	c.idle = nil
	c.mu.Unlock()
	for _, cn := range idle {
		_ = cn.nc.Close()
	}
}

// login is the user and the password a new connection logs in with, and which of the client's
// passwords that is.
func (c *Client) login() (user, password string, has bool, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cfg.user, c.cfg.password, c.cfg.hasLogin, c.epoch
}

// Identity is an identity of the server as Identities lists them: a client the server knows by
// its certificate. Fingerprint is "sha256:" and 64 hexadecimal digits: the SHA-256 of the key
// the certificate states. It says which certificate, and lets nobody in.
type Identity struct {
	Name        string
	Role        Role
	Fingerprint string
}

// PutIdentity makes an identity, or changes one that is there. A role of zero keeps the role
// the identity has, and an empty fingerprint the fingerprint: a new identity is given both. A
// connection that shows a certificate the deployment's authority signed, under this name and
// with this fingerprint, is the identity from then on, and gives no password. Another
// fingerprint is another certificate: what the one before has open is closed. It is an
// administrator's to ask.
func (c *Client) PutIdentity(ctx context.Context, name string, role Role, fingerprint string) error {
	return c.call(ctx, "", false, func(req *wire.Request) {
		req.Op, req.PutIdentity = wire.OpPutIdentity, wire.PutIdentity{Name: name, Role: uint8(role), Fingerprint: fingerprint}
	}, nil)
}

// RemoveIdentity removes an identity, and closes what it has open. It is an administrator's to
// ask. The name of no identity is ErrNoIdentity, and the last administrator is not removed
// (ErrLastAdmin).
func (c *Client) RemoveIdentity(ctx context.Context, name string) error {
	return c.call(ctx, "", false, func(req *wire.Request) { req.Op, req.Name = wire.OpRemoveIdentity, name }, nil)
}

// Identities lists the identities of the server, in the order of their names. It is an
// administrator's to ask.
func (c *Client) Identities(ctx context.Context) (identities []Identity, err error) {
	err = c.call(ctx, "", true, func(req *wire.Request) { req.Op = wire.OpIdentities }, func(resp *wire.Response) {
		identities = make([]Identity, len(resp.Identities))
		for i, id := range resp.Identities {
			identities[i] = Identity{Name: id.Name, Role: Role(id.Role), Fingerprint: id.Fingerprint}
		}
	})
	return identities, err
}
