package syngodrv

import (
	"crypto/tls"
	"net"
	"runtime"
	"testing"
	"time"
)

// A connection that waits in the pool and is alive is not taken for one that ended; one the
// other end closed, or reset, is, without anything being sent on it. With TLS over it too.
func TestAConnectionThatEndedWhileItWaitedIsKnown(t *testing.T) {
	switch runtime.GOOS {
	case "windows", "linux", "darwin", "freebsd", "openbsd", "netbsd", "dragonfly", "solaris", "illumos", "aix":
	default:
		t.Skip("on this system nothing is known of a connection without waiting")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	pair := func() (ours, theirs net.Conn) {
		t.Helper()
		accepted := make(chan net.Conn, 1)
		go func() {
			c, _ := l.Accept()
			accepted <- c
		}()
		ours, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		theirs = <-accepted
		if theirs == nil {
			t.Fatal("no connection was accepted")
		}
		return ours, theirs
	}
	// known waits a moment for the system to learn what the other end did.
	known := func(nc net.Conn) bool {
		for i := 0; i < 200; i++ {
			if ended(nc) {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}
	for _, how := range []string{"closed", "reset", "spoken to"} {
		for _, secure := range []bool{false, true} {
			ours, theirs := pair()
			held := ours
			if secure {
				// The check looks beneath the TLS, which needs no handshake to be looked beneath.
				held = tls.Client(ours, &tls.Config{InsecureSkipVerify: true})
			}
			for i := 0; i < 20; i++ {
				if ended(held) {
					t.Fatalf("a connection that is alive and silent (TLS %v) is taken for one that ended", secure)
				}
				time.Sleep(time.Millisecond)
			}
			switch how {
			case "closed":
				_ = theirs.Close()
			case "reset":
				_ = theirs.(*net.TCPConn).SetLinger(0)
				_ = theirs.Close()
			case "spoken to":
				// Something nobody asked for: the connection is of no further use either.
				if _, err := theirs.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
			}
			if !known(held) {
				t.Errorf("a connection that was %s by the other end (TLS %v) is not known to have ended", how, secure)
			}
			_ = ours.Close()
			_ = theirs.Close()
		}
	}
}
