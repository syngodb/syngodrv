// Package wire is how a SyngoDB Server and its clients speak: the frames, and the messages in
// them. The driver and the server both use it, so that every byte has one writer and one reader.
//
// What travels, and when, is in doc/protocol.md. What each message holds is in syngo.proto,
// which is the definition: the encoders of this package are written by hand to it.
package wire

import (
	"encoding/binary"
	"errors"
	"io"
)

// HeaderSize is the size of the header of a frame: the length of its payload (4 bytes, big
// endian), its kind with the flag that says the message goes on (1 byte), and the number of the
// request it belongs to (4 bytes, big endian).
const HeaderSize = 9

// The kinds of frame.
const (
	KindHello    byte = 1 // from the client: a Hello
	KindWelcome  byte = 2 // from the server: a Welcome
	KindRequest  byte = 3 // from the client: a Request
	KindResponse byte = 4 // from the server: a Response
	KindCancel   byte = 5 // from the client, without payload: the request of that number is to stop
)

// more is set in the kind of a frame whose message goes on in the next frame.
const more byte = 0x80

const (
	// LoginFrame is the most a frame holds before the login, and the most a whole message holds
	// then: a peer that has said nothing useful yet is given no more memory than that.
	LoginFrame = 64 << 10
	// MaxFrame is the most a frame ever holds.
	MaxFrame = 1 << 20
	// keep is the most memory a reader holds on to once the messages are small again, and
	// patience how many small messages in a row it takes to believe that they are: a reader of
	// large answers, one after the other, keeps the room it has.
	keep     = 4 << 10
	patience = 32
	// first is the memory a reader begins with: most messages are a few hundred bytes, and a
	// connection that is open and silent holds this much for as long as it is.
	first = 1 << 10
)

var (
	// ErrFrame is returned for what is not a frame of the protocol: a kind nobody knows, a frame
	// larger than a frame may be, a message whose frames do not belong together.
	ErrFrame = errors.New("wire: not a frame of the protocol")
	// ErrTooLarge is returned for a message larger than the session allows.
	ErrTooLarge = errors.New("wire: a message larger than the session allows")
)

// Reader reads the messages of a connection, each put together from its frames.
type Reader struct {
	src io.Reader
	// buf[from:to] was read from the connection and not handed out yet.
	buf      []byte
	from, to int
	// whole is a message that came in several frames.
	whole []byte
	// frame and message are the most a frame and a whole message may hold.
	frame, message int
	// small counts the messages in a row that would have fitted in keep.
	small int
	// midway says the first frames of a message were read and its last one was not, and
	// announced how long the frame that began it said the whole message is.
	midway    bool
	announced uint64
	// side is the header of a frame that Header is reading, or read part of: sided bytes of it.
	side  [HeaderSize]byte
	sided int

	// Announce, when it is set, is called once for every message longer than Over bytes, as
	// soon as its length is known and before the message itself is read: for a message in one
	// frame, when the header of the frame is read; for a message in several, when the frame
	// that announces it is. It is for whoever must have room for a large message before it
	// takes it: Announce may wait. An error it returns is the error of Next.
	Announce func(length int) error
	Over     int

	// Await, when it is set, is called before every read from the connection that Next makes,
	// with how many bytes of a message that is not whole yet were read so far: none when the
	// next message has not begun. It is for whoever gives its peer a time to send the rest of
	// what it began, which is another time than the one to begin.
	Await func(begun int)
}

// NewReader returns a reader of src with the limits of a connection that has not logged in.
func NewReader(src io.Reader) *Reader {
	return &Reader{src: src, frame: LoginFrame, message: LoginFrame}
}

// Limits sets the most a frame and a whole message may hold from now on. A frame holds
// MaxFrame at most whatever is asked, and a limit below one byte is one byte: no number takes a
// limit away.
func (r *Reader) Limits(frame, message int) {
	r.frame, r.message = min(max(frame, 1), MaxFrame), max(message, 1)
}

// Buffered says whether something was read from the connection and not handed out yet, and
// Pending how many bytes.
func (r *Reader) Buffered() bool { return r.to > r.from }

// Pending is how many bytes were read from the connection and not handed out yet. Whoever
// answers one message at a time learns from it what its peer sent before the answer left.
func (r *Reader) Pending() int { return r.to - r.from }

// Held is how much memory the reader holds on to, and Release gives it back when nothing that
// was read waits in it: for whoever keeps a reader on a connection that has fallen silent after
// a large message. A reader that gave its memory back begins again with a little.
func (r *Reader) Held() int { return cap(r.buf) + cap(r.whole) }

// Release gives back the memory the reader holds, unless something that was read waits in it.
func (r *Reader) Release() {
	if r.from == r.to && !r.midway && r.sided == 0 {
		r.buf, r.from, r.to, r.whole = nil, 0, 0, nil
	}
}

// Midway says whether Next failed in the middle of a message that came in several frames. A
// Next that fails because the connection was told to wait no longer can be called again and
// goes on where it stood, unless it stood there: the frames that were read are lost.
func (r *Reader) Midway() bool { return r.midway }

// Next reads the next message: its kind, the number of its request and its payload. The payload
// is the reader's own memory: it holds until the next call, and whoever keeps a part of it
// copies it.
//
// An error is the connection's (io.EOF when the peer closed it between two messages), ErrFrame
// or ErrTooLarge. After any of them the connection is of no further use.
func (r *Reader) Next() (kind byte, number uint32, payload []byte, err error) {
	if r.sided > 0 {
		// Header read the beginning of a frame's header and was sent away: those bytes go back
		// in front of what follows them.
		back := make([]byte, max(first, r.sided+r.to-r.from))
		n := copy(back, r.side[:r.sided])
		n += copy(back[n:], r.buf[r.from:r.to])
		r.buf, r.from, r.to, r.sided = back, 0, n, 0
	}
	if r.small >= patience {
		if left := r.to - r.from; cap(r.buf) > keep && left <= first {
			// What was read ahead and not handed out yet moves to a small room of its own.
			var small []byte
			if left > 0 {
				small = make([]byte, first)
				copy(small, r.buf[r.from:r.to])
			}
			r.buf, r.from, r.to = small, 0, left
		}
		if cap(r.whole) > keep {
			r.whole = nil
		}
	}
	kind, number, payload, last, err := r.one()
	if len(payload)+HeaderSize > keep || !last {
		r.small = 0
	} else if r.small < patience {
		r.small++
	}
	if err != nil {
		return kind, number, payload, err
	}
	if last {
		// A message in one frame: it was held to the limit of a message before it was read
		// (see one).
		return kind, number, payload, nil
	}
	// The message comes in several frames. The first holds its length and nothing else, and is
	// no part of the message: the limit of a message is for the length it tells, not for the
	// frame that tells it.
	if kind == KindCancel || len(payload) != AnnounceSize {
		return 0, 0, nil, ErrFrame
	}
	announced := binary.BigEndian.Uint64(payload)
	r.announced = announced
	if announced > uint64(r.message) {
		// Refused before any of it is read.
		return 0, 0, nil, ErrTooLarge
	}
	if announced == 0 {
		return 0, 0, nil, ErrFrame
	}
	if r.Announce != nil && int(announced) > r.Over {
		if err := r.Announce(int(announced)); err != nil {
			// With the kind and the number of the message, for whoever answers it all the same.
			return kind, number, nil, err
		}
	}
	r.whole = r.whole[:0]
	r.midway = true
	for !last {
		var k byte
		var n uint32
		if k, n, payload, last, err = r.one(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, 0, nil, err
		}
		// A frame that goes on holds something, or a peer could send frames for ever at no cost
		// to itself.
		if k != kind || n != number || (len(payload) == 0 && !last) {
			return 0, 0, nil, ErrFrame
		}
		if uint64(len(r.whole)+len(payload)) > announced {
			// More than was announced is no message: its reader made room for what it was told.
			return 0, 0, nil, ErrFrame
		}
		r.whole = append(r.whole, payload...)
	}
	if uint64(len(r.whole)) != announced {
		return 0, 0, nil, ErrFrame
	}
	r.midway = false
	return kind, number, r.whole, nil
}

// Header reads the header of the next frame and nothing more, and does not touch the memory
// the last message lies in. It is for whoever watches a connection, while the message read last
// is still in use, for a frame that has no payload (the one that asks a request to stop). The
// payload of any other frame is left unread: after such a frame the reader is of no further use.
//
// An error is the connection's. When the connection was told to wait no longer, Header and Next
// can be called again, and go on where this call stood.
func (r *Reader) Header() (kind byte, number uint32, size uint32, err error) {
	for r.sided < HeaderSize {
		// What was read ahead with the last message comes first: it is taken where it lies.
		if r.from < r.to {
			n := copy(r.side[r.sided:], r.buf[r.from:r.to])
			r.from, r.sided = r.from+n, r.sided+n
			continue
		}
		n, err := r.src.Read(r.side[r.sided:])
		r.sided += n
		if err != nil && r.sided < HeaderSize {
			if err == io.EOF && r.sided > 0 {
				err = io.ErrUnexpectedEOF
			}
			return 0, 0, 0, err
		}
	}
	r.sided = 0
	size = binary.BigEndian.Uint32(r.side[:])
	kind, number = r.side[4]&^more, binary.BigEndian.Uint32(r.side[5:])
	if kind < KindHello || kind > KindCancel || (kind == KindCancel && (size != 0 || r.side[4]&more != 0)) {
		return 0, 0, 0, ErrFrame
	}
	return kind, number, size, nil
}

// one reads one frame.
func (r *Reader) one() (kind byte, number uint32, payload []byte, last bool, err error) {
	if r.from == r.to {
		// Nothing was read ahead: the next frame is read into the beginning of the room, with
		// all of it to arrive in. Left where the last one ended, a frame that arrives whole
		// would often be read in two parts, the second after the first was moved to the front.
		r.from, r.to = 0, 0
	}
	if err = r.fill(HeaderSize); err != nil {
		return 0, 0, nil, false, err
	}
	header := r.buf[r.from : r.from+HeaderSize]
	size := binary.BigEndian.Uint32(header)
	kind, last = header[4]&^more, header[4]&more == 0
	number = binary.BigEndian.Uint32(header[5:])
	if kind < KindHello || kind > KindCancel || (kind == KindCancel && (size != 0 || !last)) {
		return 0, 0, nil, false, ErrFrame
	}
	if size > uint32(r.frame) {
		if int64(size) > MaxFrame {
			return 0, 0, nil, false, ErrFrame
		}
		return 0, 0, nil, false, ErrTooLarge
	}
	switch {
	case r.midway && uint64(len(r.whole))+uint64(size) > r.announced:
		// More than was announced is no message, and is not read to find that out: its reader
		// made room for what it was told, and for no more.
		return 0, 0, nil, false, ErrFrame
	case !last && !r.midway && size != AnnounceSize:
		// The first frame of a message in several holds its length and nothing else. A frame
		// of any other size that says it goes on announces nothing, and is not read.
		return 0, 0, nil, false, ErrFrame
	}
	if last && !r.midway {
		// A message in one frame: its length is known now, and nothing of it need be read yet.
		if int(size) > r.message {
			return 0, 0, nil, false, ErrTooLarge
		}
		if r.Announce != nil && int(size) > r.Over {
			if err = r.Announce(int(size)); err != nil {
				return kind, number, nil, false, err
			}
		}
	}
	if err = r.fill(HeaderSize + int(size)); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, 0, nil, false, err
	}
	payload = r.buf[r.from+HeaderSize : r.from+HeaderSize+int(size)]
	r.from += HeaderSize + int(size)
	return kind, number, payload, last, nil
}

// fill reads until n bytes are there to be handed out. It returns io.EOF only when the
// connection ended with nothing read of what was asked for.
//
// The room it reads into follows what has arrived, and not what a header announced: a peer that
// says "a megabyte follows" and sends nothing more is given no megabyte. The room doubles as
// the bytes come, up to what the frame takes and no further.
func (r *Reader) fill(n int) error {
	for r.to-r.from < n {
		if r.to == len(r.buf) {
			// No room left to read into. At least the room a reader begins with, so that what
			// follows a small message is read with it; beyond that, twice what has arrived.
			have := r.to - r.from
			want := max(first, min(n, 2*have))
			if r.from > 0 && cap(r.buf) >= want {
				// What is left moves to the front, where there is room.
				r.buf = r.buf[:cap(r.buf)]
				r.to = copy(r.buf, r.buf[r.from:r.to])
				r.from = 0
			} else {
				grown := make([]byte, want)
				r.to = copy(grown, r.buf[r.from:r.to])
				r.buf, r.from = grown, 0
			}
		}
		if r.Await != nil {
			begun := r.to - r.from
			if r.midway {
				// The frame that announced the message was read, whatever came of it since: a
				// message that is announced has begun.
				begun += AnnounceSize + len(r.whole)
			}
			r.Await(begun)
		}
		got, err := r.src.Read(r.buf[r.to:])
		r.to += got
		if err != nil && r.to-r.from < n {
			if err == io.EOF && r.to > r.from {
				// The connection ended in the middle of something.
				err = io.ErrUnexpectedEOF
			}
			return err
		}
	}
	return nil
}

// Begin returns buf emptied, with room for the header of a frame: the message is appended after
// it and handed to Send.
func Begin(buf []byte) []byte {
	return append(buf[:0], 0, 0, 0, 0, 0, 0, 0, 0, 0)
}

// AnnounceSize is the size of the payload of the frame that announces a message in several
// frames: the length of the whole message, 8 bytes, big endian.
const AnnounceSize = 8

// Send writes to dst a message begun with Begin: in one frame and one write when it fits in
// frame bytes, in several otherwise. It writes over msg while it does, so msg holds nothing of
// use afterwards. Whoever shares dst with other senders keeps them apart.
//
// A message that takes several frames is announced: before them goes a frame that holds
// nothing but the length of the whole message. Its reader then knows what is coming before it
// has taken any of it, and can make room, or wait until it has some.
func Send(dst io.Writer, frame int, kind byte, number uint32, msg []byte) error {
	if len(msg) < HeaderSize || frame <= 0 {
		return ErrFrame
	}
	// A frame holds MaxFrame at most, whatever is asked: a larger one no reader takes.
	frame = min(frame, MaxFrame)
	if whole := len(msg) - HeaderSize; whole > frame {
		var announce [HeaderSize + AnnounceSize]byte
		binary.BigEndian.PutUint32(announce[:], AnnounceSize)
		announce[4] = kind | more
		binary.BigEndian.PutUint32(announce[5:], number)
		binary.BigEndian.PutUint64(announce[HeaderSize:], uint64(whole))
		if _, err := dst.Write(announce[:]); err != nil {
			return err
		}
	}
	for at := 0; ; {
		// The header of each frame after the first is written over the end of the frame before
		// it, which has left already: nothing is copied.
		size := len(msg) - at - HeaderSize
		flag := byte(0)
		if size > frame {
			size, flag = frame, more
		}
		binary.BigEndian.PutUint32(msg[at:], uint32(size))
		msg[at+4] = kind | flag
		binary.BigEndian.PutUint32(msg[at+5:], number)
		if _, err := dst.Write(msg[at : at+HeaderSize+size]); err != nil {
			return err
		}
		if flag == 0 {
			return nil
		}
		at += size
	}
}

// Cancel writes to dst the frame that asks the request of that number to stop.
func Cancel(dst io.Writer, number uint32) error {
	var frame [HeaderSize]byte
	frame[4] = KindCancel
	binary.BigEndian.PutUint32(frame[5:], number)
	_, err := dst.Write(frame[:])
	return err
}
