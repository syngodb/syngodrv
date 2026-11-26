package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"
)

// message makes the message Begin and an encoder would: room for a header, then the payload.
func message(payload []byte) []byte {
	return append(Begin(nil), payload...)
}

// frame makes one frame by hand.
func frame(kind byte, number uint32, payload []byte) []byte {
	out := make([]byte, HeaderSize, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	out[4] = kind
	binary.BigEndian.PutUint32(out[5:], number)
	return append(out, payload...)
}

func random(r *rand.Rand, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(r.Uint32())
	}
	return out
}

// What is sent is what is read, whatever the size of the message and of the frames it is cut
// into, and however few bytes the connection hands over at a time.
func TestWhatIsSentIsRead(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 9))
	for _, size := range []int{0, 1, 8, 9, 10, 100, 4095, 4096, 4097, 70_000, 300_000} {
		for _, cut := range []int{1, 7, 9, 64, 4096, MaxFrame} {
			if size/cut > 5000 {
				continue
			}
			var conn bytes.Buffer
			var sent [][]byte
			for i := 0; i < 3; i++ {
				payload := random(r, size)
				sent = append(sent, payload)
				if err := Send(&conn, cut, KindRequest, uint32(100+i), message(payload)); err != nil {
					t.Fatal(err)
				}
			}
			if want := 3 * (size + HeaderSize*max(1, (size+cut-1)/cut)); conn.Len() != want {
				t.Fatalf("%d bytes in frames of %d: %d bytes were written and %d expected", size, cut, conn.Len(), want)
			}
			for name, src := range map[string]io.Reader{
				"as it comes":                          bytes.NewReader(conn.Bytes()),
				"a byte at a time":                     iotest.OneByteReader(bytes.NewReader(conn.Bytes())),
				"half of what is asked":                iotest.HalfReader(bytes.NewReader(conn.Bytes())),
				"with the error beside the last bytes": iotest.DataErrReader(bytes.NewReader(conn.Bytes())),
			} {
				reader := NewReader(src)
				reader.Limits(MaxFrame, 1<<20)
				for i, want := range sent {
					kind, number, got, err := reader.Next()
					if err != nil || kind != KindRequest || number != uint32(100+i) || !bytes.Equal(got, want) {
						t.Fatalf("%d bytes in frames of %d, %s, message %d: kind %d, number %d, %d bytes, %v", size, cut, name, i, kind, number, len(got), err)
					}
				}
				if _, _, _, err := reader.Next(); err != io.EOF {
					t.Fatalf("%d bytes in frames of %d, %s: after the last message: %v", size, cut, name, err)
				}
			}
		}
	}
}

// A reader gives back the memory a large message took.
func TestAReaderKeepsLittleBetweenMessages(t *testing.T) {
	var conn bytes.Buffer
	large := make([]byte, 900_000)
	payloads := [][]byte{large, []byte("small"), large, large}
	for i := 0; i < patience+2; i++ {
		payloads = append(payloads, []byte("small"))
	}
	for _, payload := range payloads {
		if err := Send(&conn, 300_000, KindResponse, 1, message(payload)); err != nil {
			t.Fatal(err)
		}
	}
	reader := NewReader(&conn)
	reader.Limits(MaxFrame, 1<<20)
	for i, want := range payloads {
		_, _, payload, err := reader.Next()
		if err != nil || len(payload) != len(want) {
			t.Fatalf("message %d: %d bytes, %v", i, len(payload), err)
		}
		// While large messages keep coming the room stays; once enough small ones came in a
		// row it is given back.
		held := cap(reader.buf) + cap(reader.whole)
		switch {
		case i == 3 && held < len(large):
			t.Errorf("after the third large message the reader holds %d bytes: it lets go of its room between two large messages", held)
		case i == len(payloads)-1 && held > 2*keep:
			t.Errorf("after %d small messages the reader holds %d bytes", patience+2, held)
		}
	}
}

// What is not a frame of the protocol is refused, each for what it is.
func TestWhatIsNotAFrameIsRefused(t *testing.T) {
	join := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	going := func(kind byte) byte { return kind | more }
	// The header of a frame that says it holds one byte more than a frame ever may.
	oversized := frame(KindRequest, 1, nil)
	binary.BigEndian.PutUint32(oversized, MaxFrame+1)
	for name, c := range map[string]struct {
		conn           []byte
		frame, message int
		want           error
	}{
		"nothing":                         {nil, MaxFrame, 1 << 20, io.EOF},
		"half a header":                   {frame(KindRequest, 1, nil)[:5], MaxFrame, 1 << 20, io.ErrUnexpectedEOF},
		"half a payload":                  {frame(KindRequest, 1, []byte("payload"))[:12], MaxFrame, 1 << 20, io.ErrUnexpectedEOF},
		"a kind nobody knows":             {frame(9, 1, nil), MaxFrame, 1 << 20, ErrFrame},
		"the kind zero":                   {frame(0, 1, nil), MaxFrame, 1 << 20, ErrFrame},
		"a flag nobody knows":             {frame(KindRequest|0x40, 1, nil), MaxFrame, 1 << 20, ErrFrame},
		"a cancel with a payload":         {frame(KindCancel, 1, []byte("x")), MaxFrame, 1 << 20, ErrFrame},
		"a cancel that goes on":           {frame(going(KindCancel), 1, nil), MaxFrame, 1 << 20, ErrFrame},
		"a frame larger than any may be":  {oversized, MaxFrame, 1 << 30, ErrFrame},
		"a frame larger than the session": {frame(KindRequest, 1, make([]byte, 5000)), 4096, 1 << 20, ErrTooLarge},
		"a message larger than the session": {join(frame(going(KindRequest), 1, make([]byte, 3000)), frame(going(KindRequest), 1, make([]byte, 3000)), frame(KindRequest, 1, make([]byte, 3000))),
			4096, 8000, ErrTooLarge},
		"a first frame larger than the message may be": {frame(going(KindRequest), 1, make([]byte, 3000)), 4096, 2000, ErrTooLarge},
		"a message that ends in the middle":            {frame(going(KindRequest), 1, []byte("begun")), MaxFrame, 1 << 20, io.ErrUnexpectedEOF},
		"frames of two kinds in one message":           {join(frame(going(KindRequest), 1, []byte("a")), frame(KindResponse, 1, []byte("b"))), MaxFrame, 1 << 20, ErrFrame},
		"frames of two requests in one message":        {join(frame(going(KindRequest), 1, []byte("a")), frame(KindRequest, 2, []byte("b"))), MaxFrame, 1 << 20, ErrFrame},
		"a cancel inside a message":                    {join(frame(going(KindRequest), 1, []byte("a")), frame(KindCancel, 1, nil)), MaxFrame, 1 << 20, ErrFrame},
		"a frame that goes on and holds nothing":       {join(frame(going(KindRequest), 1, nil), frame(KindRequest, 1, []byte("b"))), MaxFrame, 1 << 20, ErrFrame},
		"a later frame that goes on and holds nothing": {join(frame(going(KindRequest), 1, []byte("a")), frame(going(KindRequest), 1, nil), frame(KindRequest, 1, []byte("b"))), MaxFrame, 1 << 20, ErrFrame},
	} {
		reader := NewReader(bytes.NewReader(c.conn))
		reader.Limits(c.frame, c.message)
		if _, _, payload, err := reader.Next(); !errors.Is(err, c.want) || payload != nil {
			t.Errorf("%s: %d bytes and %v, where %v is expected", name, len(payload), err, c.want)
		}
	}
	// A message whose last frame holds nothing is one all the same.
	reader := NewReader(bytes.NewReader(join(frame(going(KindRequest), 7, []byte("whole")), frame(KindRequest, 7, nil))))
	if kind, number, payload, err := reader.Next(); err != nil || kind != KindRequest || number != 7 || string(payload) != "whole" {
		t.Errorf("a message whose last frame holds nothing: %q, %v", payload, err)
	}
	// Before the login a frame and a message hold little.
	reader = NewReader(bytes.NewReader(frame(KindHello, 0, make([]byte, LoginFrame+1))))
	if _, _, _, err := reader.Next(); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a first message of more than %d bytes: %v", LoginFrame, err)
	}
}

// A cancel is a frame of its own, between two messages.
func TestCancelIsAFrame(t *testing.T) {
	var conn bytes.Buffer
	if err := Send(&conn, MaxFrame, KindRequest, 4, message([]byte("ask"))); err != nil {
		t.Fatal(err)
	}
	if err := Cancel(&conn, 4); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(&conn)
	if kind, number, payload, err := reader.Next(); err != nil || kind != KindRequest || number != 4 || string(payload) != "ask" {
		t.Fatalf("the request: kind %d, number %d, %q, %v", kind, number, payload, err)
	}
	if !reader.Buffered() {
		t.Error("the cancel was read with the request, and the reader says nothing waits")
	}
	if kind, number, payload, err := reader.Next(); err != nil || kind != KindCancel || number != 4 || len(payload) != 0 {
		t.Fatalf("the cancel: kind %d, number %d, %q, %v", kind, number, payload, err)
	}
	if reader.Buffered() {
		t.Error("nothing waits, and the reader says something does")
	}
}

// Send refuses what Begin did not begin.
func TestSendWantsAMessageThatWasBegun(t *testing.T) {
	if err := Send(io.Discard, MaxFrame, KindRequest, 1, []byte("short")); !errors.Is(err, ErrFrame) {
		t.Errorf("a message without room for its header: %v", err)
	}
	if err := Send(io.Discard, 0, KindRequest, 1, message(nil)); !errors.Is(err, ErrFrame) {
		t.Errorf("frames of no bytes: %v", err)
	}
	if err := Send(brokenWriter{}, 4, KindRequest, 1, message([]byte("several frames"))); err == nil {
		t.Error("a connection that fails, and Send says nothing")
	}
}

type brokenWriter struct{}

func (brokenWriter) Write(p []byte) (int, error) { return 0, errors.New("the connection is gone") }

// Whatever comes from a connection, a reader neither panics nor hands out more than the session
// allows, and what it hands out is what a sender of those frames meant.
func FuzzReader(f *testing.F) {
	f.Add(frame(KindRequest, 1, []byte("payload")), uint16(64), uint16(128))
	f.Add(bytes.Join([][]byte{frame(KindRequest|more, 1, []byte("pay")), frame(KindRequest, 1, []byte("load"))}, nil), uint16(8), uint16(16))
	f.Add(frame(KindCancel, 9, nil), uint16(1), uint16(1))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 3, 0, 0, 0, 1}, uint16(64), uint16(64))
	f.Fuzz(func(t *testing.T, conn []byte, frameLimit, messageLimit uint16) {
		reader := NewReader(iotest.OneByteReader(bytes.NewReader(conn)))
		reader.Limits(int(frameLimit), int(messageLimit))
		// The same bytes read by a second reader that is handed them all at once: the two agree.
		other := NewReader(bytes.NewReader(conn))
		other.Limits(int(frameLimit), int(messageLimit))
		for {
			kind, number, payload, err := reader.Next()
			kind2, number2, payload2, err2 := other.Next()
			if kind != kind2 || number != number2 || !bytes.Equal(payload, payload2) || (err == nil) != (err2 == nil) {
				t.Fatalf("a byte at a time: kind %d, number %d, %d bytes, %v; all at once: kind %d, number %d, %d bytes, %v", kind, number, len(payload), err, kind2, number2, len(payload2), err2)
			}
			if err != nil {
				if payload != nil {
					t.Fatalf("an error (%v) and a payload", err)
				}
				return
			}
			// A message is held to its limit, in one frame or in many; a limit below one byte is
			// one byte.
			if kind < KindHello || kind > KindCancel || len(payload) > max(int(messageLimit), 1) {
				t.Fatalf("kind %d and %d bytes, with frames of %d and messages of %d", kind, len(payload), frameLimit, messageLimit)
			}
		}
	})
}

// Whatever is sent in frames of whatever size is read back as it was.
func FuzzSend(f *testing.F) {
	f.Add([]byte("a message"), uint16(4), uint32(7))
	f.Add([]byte{}, uint16(1), uint32(0))
	f.Fuzz(func(t *testing.T, payload []byte, cut uint16, number uint32) {
		if cut == 0 || len(payload)/int(cut) > 10_000 {
			return
		}
		var conn bytes.Buffer
		if err := Send(&conn, int(cut), KindResponse, number, message(payload)); err != nil {
			t.Fatal(err)
		}
		reader := NewReader(&conn)
		reader.Limits(MaxFrame, len(payload))
		kind, got, read, err := reader.Next()
		if err != nil || kind != KindResponse || got != number || !bytes.Equal(read, payload) {
			t.Fatalf("%d bytes in frames of %d: kind %d, number %d, %d bytes, %v", len(payload), cut, kind, got, len(read), err)
		}
	})
}

// While the message read last is still in use, the header of what follows is read on the side:
// the message is not touched, whatever was read ahead and however the bytes arrive. And a header
// that was half read when the reader was sent away is not lost.
func TestAHeaderReadOnTheSideLeavesTheLastMessageAlone(t *testing.T) {
	r := rand.New(rand.NewPCG(4, 4))
	request := random(r, 3000)
	var conn bytes.Buffer
	if err := Send(&conn, MaxFrame, KindRequest, 7, message(request)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := Cancel(&conn, uint32(5+i)); err != nil {
			t.Fatal(err)
		}
	}
	following := random(r, 2000)
	if err := Send(&conn, MaxFrame, KindRequest, 8, message(following)); err != nil {
		t.Fatal(err)
	}
	all := conn.Bytes()
	for name, src := range map[string]func() io.Reader{
		"as it comes":      func() io.Reader { return bytes.NewReader(all) },
		"a byte at a time": func() io.Reader { return iotest.OneByteReader(bytes.NewReader(all)) },
		"in pieces that end inside a header": func() io.Reader {
			return io.MultiReader(bytes.NewReader(all[:3013]), bytes.NewReader(all[3013:3020]), bytes.NewReader(all[3020:3031]), bytes.NewReader(all[3031:]))
		},
	} {
		reader := NewReader(src())
		reader.Limits(MaxFrame, 1<<20)
		_, _, payload, err := reader.Next()
		if err != nil || !bytes.Equal(payload, request) {
			t.Fatalf("%s: the request: %v", name, err)
		}
		for i := 0; i < 3; i++ {
			kind, number, size, err := reader.Header()
			if err != nil || kind != KindCancel || number != uint32(5+i) || size != 0 {
				t.Fatalf("%s: header %d on the side: kind %d, number %d, size %d, %v", name, i, kind, number, size, err)
			}
			if !bytes.Equal(payload, request) {
				t.Fatalf("%s: the request was changed under whoever is using it, by header %d read on the side", name, i)
			}
		}
		// The reader goes on as if nothing had been read on the side.
		kind, number, payload, err := reader.Next()
		if err != nil || kind != KindRequest || number != 8 || !bytes.Equal(payload, following) {
			t.Fatalf("%s: the message after the headers: kind %d, number %d, %v", name, kind, number, err)
		}
	}
	// Sent away in the middle of a header: the bytes that were read are still the beginning of
	// the next frame, for Header and for Next alike.
	for _, then := range []string{"Header", "Next"} {
		late := &lateReader{parts: [][]byte{all[:3013], all[3013:3017], nil, all[3017:]}}
		reader := NewReader(late)
		reader.Limits(MaxFrame, 1<<20)
		if _, _, payload, err := reader.Next(); err != nil || !bytes.Equal(payload, request) {
			t.Fatal(err)
		}
		if _, _, _, err := reader.Header(); err != errLate {
			t.Fatalf("a header that stops after 4 bytes: %v", err)
		}
		if then == "Header" {
			if kind, number, _, err := reader.Header(); err != nil || kind != KindCancel || number != 5 {
				t.Errorf("the header, asked for again: kind %d, number %d, %v", kind, number, err)
			}
			continue
		}
		if kind, number, payload, err := reader.Next(); err != nil || kind != KindCancel || number != 5 || len(payload) != 0 {
			t.Errorf("the frame whose header was half read on the side: kind %d, number %d, %v", kind, number, err)
		}
		if kind, number, _, err := reader.Next(); err != nil || kind != KindCancel || number != 6 {
			t.Errorf("the frame after it: kind %d, number %d, %v", kind, number, err)
		}
	}
	// What is no frame is refused on the side as it is anywhere.
	reader := NewReader(bytes.NewReader(frame(KindCancel, 1, []byte("x"))))
	if _, _, _, err := reader.Header(); !errors.Is(err, ErrFrame) {
		t.Errorf("a cancel with a payload, read on the side: %v", err)
	}
	reader = NewReader(bytes.NewReader(nil))
	if _, _, _, err := reader.Header(); err != io.EOF {
		t.Errorf("a connection that ended, read on the side: %v", err)
	}
	reader = NewReader(bytes.NewReader(frame(KindCancel, 1, nil)[:4]))
	if _, _, _, err := reader.Header(); err != io.ErrUnexpectedEOF {
		t.Errorf("a connection that ended inside a header, read on the side: %v", err)
	}
}

var errLate = errors.New("told to wait no longer")

// lateReader hands out its parts one by one, and fails once where a part is nil.
type lateReader struct{ parts [][]byte }

func (l *lateReader) Read(p []byte) (int, error) {
	for len(l.parts) > 0 {
		if l.parts[0] == nil {
			l.parts = l.parts[1:]
			return 0, errLate
		}
		if len(l.parts[0]) == 0 {
			l.parts = l.parts[1:]
			continue
		}
		n := copy(p, l.parts[0])
		l.parts[0] = l.parts[0][n:]
		return n, nil
	}
	return 0, io.EOF
}

// wholes hands out, at each read, one message that was written: all of it when there is room.
type wholes struct {
	messages [][]byte
	reads    int
}

func (w *wholes) Read(p []byte) (int, error) {
	if len(w.messages) == 0 {
		return 0, io.EOF
	}
	w.reads++
	n := copy(p, w.messages[0])
	if w.messages[0] = w.messages[0][n:]; len(w.messages[0]) == 0 {
		w.messages = w.messages[1:]
	}
	return n, nil
}

// A reader that holds nothing reads the next message into the beginning of its room. A request
// and its answer take one read each then, where a message that arrived whole was often read in
// two parts: of small messages one after the other, three in eight were.
func TestAMessageThatArrivesWholeIsReadAtOnce(t *testing.T) {
	src := &wholes{}
	var sizes []int
	for i := 0; i < 500; i++ {
		size := (i*379 + 11) % 990
		sizes = append(sizes, size)
		var one bytes.Buffer
		if err := Send(&one, MaxFrame, KindRequest, uint32(i+1), append(Begin(nil), bytes.Repeat([]byte{byte(i)}, size)...)); err != nil {
			t.Fatal(err)
		}
		src.messages = append(src.messages, one.Bytes())
	}
	r := NewReader(src)
	for i, size := range sizes {
		kind, number, payload, err := r.Next()
		if err != nil || kind != KindRequest || number != uint32(i+1) || len(payload) != size || (size > 0 && (payload[0] != byte(i) || payload[size-1] != byte(i))) {
			t.Fatalf("message %d: kind %d, number %d, %d bytes of %d, %v", i, kind, number, len(payload), size, err)
		}
	}
	if src.reads != len(sizes) {
		t.Errorf("%d messages, each smaller than the room of a reader and arrived whole, took %d reads", len(sizes), src.reads)
	}
}
