package syngodrv

import (
	"crypto/tls"
	"net"
	"syscall"
	"unsafe"
)

// wsaPoll is WSAPoll of the system's sockets: it says what there is to learn about a socket
// without waiting.
var wsaPoll = syscall.NewLazyDLL("ws2_32.dll").NewProc("WSAPoll")

// pollFD is the system's WSAPOLLFD.
type pollFD struct {
	fd      uintptr
	events  int16
	revents int16
}

// pollRead asks whether there is something to read (POLLRDNORM).
const pollRead = 0x0100

// ended says whether a connection that waited in the pool is known to have ended meanwhile:
// the server closed it, or the network did. It asks the system, without waiting, whether there
// is something to learn from the connection: on one that is alive and silent there is nothing;
// on one the other end closed, or that broke, there is its end.
//
// It is asked before a write is sent on a connection that waited for a while. Without it, such
// a write would leave, find nobody, and be reported as a write whose outcome nobody knows,
// though it was never made: after a server was started again, that would be the first write of
// every connection of every program.
func ended(nc net.Conn) bool {
	if secure, ok := nc.(*tls.Conn); ok {
		nc = secure.NetConn()
	}
	sc, ok := nc.(syscall.Conn)
	if !ok || wsaPoll.Find() != nil {
		return false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false
	}
	over := false
	err = raw.Control(func(fd uintptr) {
		ask := pollFD{fd: fd, events: pollRead}
		n, _, _ := wsaPoll.Call(uintptr(unsafe.Pointer(&ask)), 1, 0)
		// Nothing to report is a connection that is there. Anything else is its end, an error,
		// or something the server said that nobody asked for: the connection is of no further
		// use either way. A call that failed says nothing, and nothing is concluded from it.
		over = int32(n) > 0 && ask.revents != 0
	})
	return over || err != nil
}
