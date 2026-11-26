//go:build unix

package syngodrv

import (
	"crypto/tls"
	"net"
	"syscall"
)

// ended says whether a connection that waited in the pool is known to have ended meanwhile:
// the server closed it, or the network did. It asks the system, without waiting, whether there
// is something to read: on a connection that is alive and silent there is nothing; on one the
// other end closed there is its end.
//
// It is asked before a write is sent on a connection that waited for a while. Without it, such
// a write would leave, find nobody, and be reported as a write whose outcome nobody knows,
// though it was never made.
func ended(nc net.Conn) bool {
	if secure, ok := nc.(*tls.Conn); ok {
		nc = secure.NetConn()
	}
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	over := false
	err = raw.Read(func(fd uintptr) bool {
		var one [1]byte
		n, err := syscall.Read(int(fd), one[:])
		switch {
		case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK || err == syscall.EINTR:
			// Nothing to read: the connection is there.
		case n == 0 || err != nil:
			over = true
		default:
			// The server said something nobody asked for, and a byte of it is gone: the
			// connection is of no further use either way.
			over = true
		}
		return true
	})
	return over || err != nil
}
