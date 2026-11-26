//go:build !unix && !windows

package syngodrv

import "net"

// ended says whether a connection that waited in the pool is known to have ended meanwhile.
// Here nothing is known without waiting: a write on such a connection is reported for what it
// is, a write whose outcome nobody knows.
func ended(net.Conn) bool { return false }
