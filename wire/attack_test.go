package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The tests of this file were written by a reviewer whose task was to break the reader of
// frames and the decoders of the messages. A test named TestAttackFound... shows a defect and
// fails on the code as it stood when the review was made. A test named TestAttackHeld... is an
// attack the code withstood, and passes.

// attackAllocated runs fn and returns the bytes it allocated, and the bytes that are still
// held once everything that can be collected was (fn returns what is to stay alive).
func attackAllocated(fn func() any) (allocated, held int64) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)
	keep := fn()
	runtime.ReadMemStats(&after)
	allocated = int64(after.TotalAlloc - before.TotalAlloc)
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	held = int64(after.HeapAlloc) - int64(before.HeapAlloc)
	runtime.KeepAlive(keep)
	return allocated, held
}

// attackField writes a field that holds bytes: a text, a message.
func attackField(b []byte, field int, body []byte) []byte {
	return append(appendVarint(appendTag(b, field, asBytes), uint64(len(body))), body...)
}

// ---------------------------------------------------------------------------------------------
// Found
// ---------------------------------------------------------------------------------------------

// A message is at most what the session allows, says the page. The reader checks that only for
// a message that comes in several frames. A message in one frame is checked against the limit
// of a frame alone: where the limit of a message is the smaller of the two, a message of up to
// a whole frame is handed out. (The reader of a server whose operator set Limits.Message below
// one MiB, with the frame left at its default, is in that state.)
func TestAttackFixedAMessageInOneFrameIsNotHeldToTheLimitOfAMessage(t *testing.T) {
	const allowed = 1000
	payload := make([]byte, 5*allowed)
	reader := NewReader(bytes.NewReader(frame(KindRequest, 1, payload)))
	reader.Limits(MaxFrame, allowed)
	_, _, got, err := reader.Next()
	if !errors.Is(err, ErrTooLarge) {
		t.Errorf("a message of %d bytes in one frame, on a session that allows messages of %d: the reader handed out %d bytes (%v), and ErrTooLarge was expected", len(payload), allowed, len(got), err)
	}
	// The same bytes cut in two frames, and announced as a message in several is, are refused
	// too: the limit does not depend on how the peer cuts.
	cut := bytes.Join([][]byte{announced(KindRequest, 1, len(payload)), frame(KindRequest|more, 1, payload[:2500]), frame(KindRequest, 1, payload[2500:])}, nil)
	reader = NewReader(bytes.NewReader(cut))
	reader.Limits(MaxFrame, allowed)
	if _, _, _, err := reader.Next(); !errors.Is(err, ErrTooLarge) {
		t.Errorf("the same message in two frames: %v", err)
	}
}

// MaxFrame is "the most a frame ever holds". Limits takes the smaller of what it is given and
// MaxFrame, and so lets a number below zero through. The check of a frame then compares the
// length against uint32(frame), which is a number near four thousand million: no frame is
// refused any more, MaxFrame included.
//
// Where int has 32 bits this is what a driver gets from int(welcome.MaxFrame) when a server
// says 2147483648 or more (syngodrv/conn.go, where Limits is called). On such a platform the
// reader then also computes HeaderSize+int(size) below zero for a length of 2^31 or more, and
// slices its buffer with it.
func TestAttackFixedALimitBelowZeroTakesTheCapOfAFrameAway(t *testing.T) {
	said := uint32(1 << 31) // what a welcome may say
	asInt32 := int(int32(said))
	payload := make([]byte, MaxFrame+1)
	reader := NewReader(bytes.NewReader(frame(KindResponse, 1, payload)))
	reader.Limits(asInt32, 1<<30)
	_, _, got, err := reader.Next()
	if !errors.Is(err, ErrFrame) {
		t.Errorf("a frame of MaxFrame+1 bytes, read after Limits(%d, ...): the reader handed out %d bytes (%v), and ErrFrame was expected", asInt32, len(got), err)
	}
}

// Where int has 32 bits, the same in earnest: a driver hands Limits what the welcome of a server
// says, as an int. A server that says 2147483648 takes the cap of a frame away, and a header
// that announces as much then has the reader slice its memory to a length below zero. The test
// is skipped where int has 64 bits: run it with GOARCH=386.
func TestAttackFixedOn32BitsAHeaderMakesTheReaderPanic(t *testing.T) {
	if strconv.IntSize != 32 {
		t.Skip("int has 64 bits here: run with GOARCH=386")
	}
	welcome := Welcome{MaxFrame: 1 << 31, MaxMessage: 1 << 20}
	header := frame(KindResponse, 1, nil)
	binary.BigEndian.PutUint32(header, 1<<31)
	reader := NewReader(bytes.NewReader(header))
	// As syngodrv/conn.go does.
	reader.Limits(int(welcome.MaxFrame), int(min(welcome.MaxMessage, 1<<40)))
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("a welcome that says frames of %d bytes, then a header that announces as many: the reader panics: %v", welcome.MaxFrame, r)
		}
	}()
	if _, _, _, err := reader.Next(); !errors.Is(err, ErrFrame) {
		t.Errorf("a header that announces %d bytes: %v", uint32(1<<31), err)
	}
}

// Send cuts a message into frames of the size it is told, and is not held to MaxFrame: told
// more, it writes a frame that no reader of this package takes.
func TestAttackFixedSendWritesAFrameNoReaderTakes(t *testing.T) {
	var conn bytes.Buffer
	if err := Send(&conn, 2*MaxFrame, KindRequest, 1, message(make([]byte, MaxFrame+1))); err != nil {
		t.Fatal(err)
	}
	reader := NewReader(&conn)
	reader.Limits(2*MaxFrame, 1<<30)
	if _, _, _, err := reader.Next(); err != nil {
		t.Errorf("what Send wrote in frames of %d bytes, read with Limits(%d, ...): %v", 2*MaxFrame, 2*MaxFrame, err)
	}
}

// The decoders keep a number in less room than the message gives it: the kind of answer, the
// way of writing and the operator of a condition are enums (32 bits) in syngo.proto and uint8
// here, and the conversion drops the high bits. A value this version does not know is to be
// answered UNSUPPORTED. Instead a value of 256 and more is taken for another one, which the
// server then serves: the answer 260 is a removal of every document of the answer.
func TestAttackFixedANumberThatDoesNotFitIsTakenForAnother(t *testing.T) {
	// A query that asks for the kind of answer 260.
	query := attackField(nil, 1, []byte("things"))
	query = appendVarint(appendTag(query, 9, asVarint), 260)
	var req Request
	if !req.Decode(attackField(nil, int(OpQuery), query)) {
		t.Fatal("the query is not read")
	}
	if req.Query.Answer == AnswerDelete {
		t.Errorf("a query that asks for the kind of answer 260 is read as one that asks for %d, which is the removal of its documents", req.Query.Answer)
	}
	// A write in the way 257.
	put := attackField(nil, 1, []byte("things"))
	put = appendVarint(appendTag(put, 2, asVarint), 257)
	put = attackField(put, 3, []byte(`{"_id":"x"}`))
	if !req.Decode(attackField(nil, int(OpPut), put)) {
		t.Fatal("the write is not read")
	}
	if req.Put.Mode == WriteInsert {
		t.Errorf("a write in the way 257 is read as one in the way %d", req.Put.Mode)
	}
	// A condition with the operator 259.
	cond := appendVarint(appendTag(nil, 1, asVarint), 259)
	query = attackField(attackField(nil, 1, []byte("things")), 2, cond)
	if !req.Decode(attackField(nil, int(OpQuery), query)) {
		t.Fatal("the query is not read")
	}
	if req.Query.Where.Op == 3 {
		t.Errorf("a condition with the operator 259 is read as one with the operator %d", req.Query.Where.Op)
	}
}

// A message is bounded in bytes, and what a decoder made of it was not: an element of a list
// that holds nothing takes two bytes of the message and a whole value of the list in memory.
// The worst was a condition made of conditions that say nothing: 88 bytes for 2, so that a
// message of the default limit (33 MiB) decoded to about 1.5 GiB before the server had looked
// at what the request asks, who asks it, or whether the database it names exists.
//
// FIXED: a decoder counts every element it adds to a list, and a message that asks for more
// than its room is refused as too large (which is not "no message": the session goes on). What
// is held for a message that is refused stays within the room, and a list that fits is read as
// it was written.
func TestAttackFixedAMessageOfNothingsIsRefusedPastItsRoom(t *testing.T) {
	nothings := func(field, elements int) []byte {
		out := make([]byte, 0, 2*elements)
		for i := 0; i < elements; i++ {
			out = append(appendTag(out, field, asBytes), 0)
		}
		return out
	}
	const room = 1 << 20
	for _, c := range []struct {
		name    string
		message func(elements int) []byte
		decode  func(b []byte) (kept any, ok, over bool, read int)
	}{
		{"conditions inside a condition", func(n int) []byte { return attackField(nil, int(OpQuery), attackField(nil, 2, nothings(6, n))) }, func(b []byte) (any, bool, bool, int) {
			req := &Request{Room: room}
			ok := req.Decode(b)
			return req, ok, req.Over, len(req.Query.Where.Conds)
		}},
		{"arguments of a query text", func(n int) []byte { return attackField(nil, int(OpQuery), attackField(nil, 3, nothings(2, n))) }, func(b []byte) (any, bool, bool, int) {
			req := &Request{Room: room}
			ok := req.Decode(b)
			return req, ok, req.Over, len(req.Query.Args)
		}},
		{"orders of a query", func(n int) []byte { return attackField(nil, int(OpQuery), attackField(nil, 4, nothings(1, n))) }, func(b []byte) (any, bool, bool, int) {
			req := &Request{Room: room}
			ok := req.Decode(b)
			return req, ok, req.Over, len(req.Query.Order)
		}},
		{"documents of a batch to store", func(n int) []byte { return attackField(nil, int(OpPutBatch), nothings(2, n)) }, func(b []byte) (any, bool, bool, int) {
			req := &Request{Room: room}
			ok := req.Decode(b)
			return req, ok, req.Over, len(req.PutBatch.Documents)
		}},
		{"documents of a batch of an answer (what a server can do to a driver)", func(n int) []byte { return attackField(nil, int(ResultBatch), nothings(1, n)) }, func(b []byte) (any, bool, bool, int) {
			resp := &Response{Room: room}
			ok := resp.Decode(b)
			return resp, ok, resp.Over, len(resp.Batch.Documents)
		}},
	} {
		// A megabyte of nothings: far more than the room.
		message := c.message(500_000)
		var ok, over bool
		allocated, held := attackAllocated(func() any {
			var kept any
			kept, ok, over, _ = c.decode(message)
			return kept
		})
		t.Logf("%-70s a message of %d bytes: refused %v, %d bytes allocated, %d held afterwards", c.name+":", len(message), !ok && over, allocated, held)
		if ok || !over {
			t.Errorf("%s: a message that describes %d elements, with a room of %d bytes: read %v, said to be too large %v", c.name, 500_000, room, ok, over)
		}
		if held > 3*room {
			t.Errorf("%s: a message that was refused left %d bytes held, with a room of %d", c.name, held, room)
		}
		// A few thousand elements fit, and are read as they were written.
		message = c.message(5000)
		if _, ok, over, read := c.decode(message); !ok || over || read != 5000 {
			t.Errorf("%s: a message of 5000 elements, which fits: read %v, too large %v, %d elements", c.name, ok, over, read)
		}
	}
	// Without a word on its room a request has the default, which a megabyte of nothings passes.
	req := &Request{}
	if req.Decode(attackField(nil, int(OpQuery), attackField(nil, 2, nothings(6, 500_000)))) || !req.Over {
		t.Errorf("a megabyte of conditions that say nothing, with the default room: read, or not said to be too large (%v)", req.Over)
	}
}

// ---------------------------------------------------------------------------------------------
// Held
// ---------------------------------------------------------------------------------------------

// A header that lies about the length of its frame is refused before any room is made for the
// payload it announces.
func TestAttackHeldAHeaderThatLiesAboutItsLengthCostsNothing(t *testing.T) {
	for _, c := range []struct {
		size           uint32
		frame, message int
		want           error
	}{
		{0xffffffff, MaxFrame, 1 << 30, ErrFrame},
		{0x80000000, MaxFrame, 1 << 30, ErrFrame},
		{0x7fffffff, MaxFrame, 1 << 30, ErrFrame},
		{MaxFrame + 1, MaxFrame, 1 << 30, ErrFrame},
		{LoginFrame + 1, LoginFrame, LoginFrame, ErrTooLarge},
		{MaxFrame, LoginFrame, LoginFrame, ErrTooLarge},
	} {
		header := frame(KindRequest, 1, nil)
		binary.BigEndian.PutUint32(header, c.size)
		var err error
		allocated, _ := attackAllocated(func() any {
			reader := NewReader(bytes.NewReader(header))
			reader.Limits(c.frame, c.message)
			_, _, _, err = reader.Next()
			return reader
		})
		if !errors.Is(err, c.want) {
			t.Errorf("a header that says %d bytes, with frames of %d: %v, and %v was expected", c.size, c.frame, err, c.want)
		}
		if allocated > 16<<10 {
			t.Errorf("a header that says %d bytes, with frames of %d, made the reader allocate %d bytes before it refused", c.size, c.frame, allocated)
		}
	}
}

// What a reader holds for a peer that has not logged in. A header alone, of nine bytes, has it
// make room for the whole frame it announces: 64 KiB. And a message in two frames has it hold
// the first frame in the room of the message and make room for the second: twice 64 KiB. The
// page says no connection holds more than a frame (64 KiB) for a peer that has said nothing
// useful yet: the reader holds up to about twice that, which this test measures and bounds.
func TestAttackHeldWhatAReaderHoldsBeforeTheLogin(t *testing.T) {
	// Nine bytes.
	header := frame(KindHello, 0, nil)
	binary.BigEndian.PutUint32(header, LoginFrame)
	reader := NewReader(bytes.NewReader(header))
	if _, _, _, err := reader.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("a header and nothing after it: %v", err)
	}
	afterHeader := cap(reader.buf) + cap(reader.whole)
	// A message announced as long as one may be: a first frame one byte short of that, and the
	// header of a second.
	second := frame(KindHello, 0, nil)
	binary.BigEndian.PutUint32(second, 1)
	reader = NewReader(bytes.NewReader(bytes.Join([][]byte{announced(KindHello, 0, LoginFrame), frame(KindHello|more, 0, make([]byte, LoginFrame-1)), second}, nil)))
	if _, _, _, err := reader.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("a message that ends after the header of its second frame: %v", err)
	}
	afterTwo := cap(reader.buf) + cap(reader.whole)
	t.Logf("before the login a reader holds %d bytes after a header of 9 bytes that announces %d, and %d bytes in the middle of a message of two frames", afterHeader, LoginFrame, afterTwo)
	if afterHeader > LoginFrame+HeaderSize+first {
		t.Errorf("after a header of 9 bytes the reader holds %d bytes", afterHeader)
	}
	if afterTwo > 2*LoginFrame+16<<10 {
		t.Errorf("in the middle of a message of two frames the reader holds %d bytes, more than twice what a frame may hold before the login", afterTwo)
	}
}

// A message cut into frames of one byte costs ten bytes of connection for each byte of message,
// and the reader the same work for each: nothing grows faster than the message does.
func TestAttackHeldAMessageInFramesOfOneByte(t *testing.T) {
	cost := func(size int) (time.Duration, int64) {
		var conn bytes.Buffer
		if err := Send(&conn, 1, KindRequest, 1, message(make([]byte, size))); err != nil {
			t.Fatal(err)
		}
		if conn.Len() != size*(HeaderSize+1)+HeaderSize+AnnounceSize {
			t.Fatalf("%d bytes were written for a message of %d in frames of one", conn.Len(), size)
		}
		var took time.Duration
		allocated, _ := attackAllocated(func() any {
			reader := NewReader(&conn)
			reader.Limits(MaxFrame, size)
			began := time.Now()
			_, _, payload, err := reader.Next()
			took = time.Since(began)
			if err != nil || len(payload) != size {
				t.Fatalf("a message of %d bytes in frames of one: %d bytes, %v", size, len(payload), err)
			}
			return reader
		})
		return took, allocated
	}
	cost(64 << 10)
	smallTime, smallBytes := cost(64 << 10)
	largeTime, largeBytes := cost(1 << 20)
	t.Logf("in frames of one byte: 64 KiB take %v and %d bytes allocated, 1 MiB takes %v and %d bytes allocated", smallTime, smallBytes, largeTime, largeBytes)
	// Sixteen times the message: sixteen times the work, and some room for a machine that is busy.
	if largeTime > 64*smallTime+50*time.Millisecond {
		t.Errorf("a message 16 times as large took %v, and the small one %v: the work grows faster than the message", largeTime, smallTime)
	}
	if smallBytes > 8*(64<<10) || largeBytes > 8*(1<<20) {
		t.Errorf("the reader allocated %d bytes for a message of 64 KiB and %d for one of 1 MiB", smallBytes, largeBytes)
	}
	// And one byte more than the session allows is refused at the frame that brings it, however
	// the message is cut.
	var conn bytes.Buffer
	_ = Send(&conn, 1, KindHello, 0, message(make([]byte, LoginFrame+1)))
	if _, _, _, err := NewReader(&conn).Next(); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a first message of one byte more than it may hold, in frames of one byte: %v", err)
	}
}

// Frames that hold nothing cannot be sent for ever inside a message, and outside a message
// each one is a message of its own that the caller sees.
func TestAttackHeldFramesThatHoldNothing(t *testing.T) {
	// Inside a message, at any place.
	for at := 0; at < 5; at++ {
		var stream []byte
		for i := 0; i < 5; i++ {
			payload := []byte("x")
			if i == at {
				payload = nil
			}
			stream = append(stream, frame(KindRequest|more, 3, payload)...)
		}
		stream = append(stream, frame(KindRequest, 3, []byte("end"))...)
		if _, _, payload, err := NewReader(bytes.NewReader(stream)).Next(); !errors.Is(err, ErrFrame) || payload != nil {
			t.Errorf("an empty frame that goes on, at place %d of a message: %d bytes, %v", at, len(payload), err)
		}
	}
	// Outside: a thousand of them are a thousand messages, each handed out, none swallowed.
	var stream []byte
	for i := 0; i < 1000; i++ {
		stream = append(stream, frame(KindRequest, uint32(i), nil)...)
	}
	reader := NewReader(bytes.NewReader(stream))
	for i := 0; i < 1000; i++ {
		kind, number, payload, err := reader.Next()
		if err != nil || kind != KindRequest || number != uint32(i) || len(payload) != 0 {
			t.Fatalf("empty message %d: kind %d, number %d, %d bytes, %v", i, kind, number, len(payload), err)
		}
	}
	if _, _, _, err := reader.Next(); err != io.EOF {
		t.Errorf("after the last: %v", err)
	}
}

// A decoder does the work the size of a message says, whatever the message is made of: lists
// of nothings, conditions as deep as they may be, fields nobody knows written the long way, a
// kind of request stated again and again. Four times the message is about four times the work.
func TestAttackHeldDecodingGrowsWithTheMessageAndNoFaster(t *testing.T) {
	// A chain of conditions as deep as they may be, with a text at every level.
	chain := func() []byte {
		var c []byte
		for i := 0; i < CondDepth-1; i++ {
			c = attackField(attackField(nil, 3, []byte("a field of some length")), 6, c)
		}
		return c
	}()
	long := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00} // zero, in ten bytes
	shapes := map[string]func(n int) []byte{
		"chains of conditions 200 deep": func(n int) []byte {
			var where []byte
			for len(where) < n {
				where = attackField(where, 6, chain)
			}
			return attackField(nil, int(OpQuery), attackField(nil, 2, where))
		},
		"fields nobody knows, the long way": func(n int) []byte {
			var b []byte
			for len(b) < n {
				b = append(appendTag(b, 900, asVarint), long...)
			}
			return b
		},
		"a kind of request stated again and again": func(n int) []byte {
			var b []byte
			one := attackField(attackField(nil, 1, []byte("things")), 2, []byte("an identifier"))
			for len(b) < n {
				b = attackField(b, int(OpGet), one)
				b = attackField(b, int(OpPutBatch), attackField(nil, 2, []byte("{}")))
				b = attackField(b, int(OpQuery), attackField(nil, 3, attackField(nil, 2, nil)))
			}
			return b
		},
		"a query text stated again and again, each with its arguments": func(n int) []byte {
			var q []byte
			for len(q) < n {
				q = attackField(q, 3, attackField(attackField(nil, 1, []byte("n = :n")), 2, attackField(nil, 1, []byte("n"))))
				q = attackField(q, 4, attackField(nil, 1, attackField(nil, 1, []byte("n"))))
			}
			return attackField(nil, int(OpQuery), q)
		},
	}
	for name, shape := range shapes {
		took := func(n int) time.Duration {
			message := shape(n)
			best := time.Hour
			for i := 0; i < 5; i++ {
				var req Request
				began := time.Now()
				if !req.Decode(message) {
					t.Fatalf("%s: the message is no request", name)
				}
				best = min(best, time.Since(began))
			}
			return best
		}
		small, large := took(250_000), took(1_000_000)
		t.Logf("%-62s 250 KB in %v, 1 MB in %v", name+":", small, large)
		if large > 16*small+20*time.Millisecond {
			t.Errorf("%s: 250 KB are decoded in %v and 1 MB in %v", name, small, large)
		}
	}
	// One level deeper than conditions may be is no message.
	var req Request
	if !req.Decode(attackField(nil, int(OpQuery), attackField(nil, 2, attackField(nil, 6, chain)))) {
		t.Error("conditions as deep as they may be are not read")
	}
	if req.Decode(attackField(nil, int(OpQuery), attackField(nil, 2, attackField(nil, 6, attackField(nil, 6, chain))))) {
		t.Error("conditions deeper than they may be are read")
	}
}

// attackSource is a connection that hands over what it holds in pieces of any size, never past
// the next barrier, and that fails once at each barrier as a connection does that was told to
// wait no longer.
type attackSource struct {
	data     []byte
	at       int
	barriers []int // ascending
	rnd      *rand.Rand
	late     int // how many times it failed
}

func (s *attackSource) Read(p []byte) (int, error) {
	limit := len(s.data)
	for len(s.barriers) > 0 && s.barriers[0] < s.at {
		s.barriers = s.barriers[1:]
	}
	if len(s.barriers) > 0 {
		if s.barriers[0] == s.at {
			s.barriers = s.barriers[1:]
			s.late++
			return 0, errLate
		}
		limit = s.barriers[0]
	}
	if s.at >= len(s.data) {
		return 0, io.EOF
	}
	most := min(len(p), limit-s.at)
	n := most
	switch s.rnd.IntN(4) {
	case 0:
		n = 1
	case 1:
		n = 1 + s.rnd.IntN(most)
	}
	copy(p, s.data[s.at:s.at+n])
	s.at += n
	return n, nil
}

// The reader keeps its place, whatever the sizes of the messages and of their frames, however
// the bytes arrive, wherever the reader is sent away (inside a header, inside a payload, between
// two frames of one message), and whether a frame is read by Next or its header on the side by
// Header. What it handed out is not changed by a header read on the side.
func TestAttackHeldTheReaderKeepsItsPlace(t *testing.T) {
	type item struct {
		kind    byte
		number  uint32
		payload []byte
		start   int   // where its first frame begins in the stream
		frames  []int // where each of its frames begins
	}
	for seed := uint64(1); seed <= 400; seed++ {
		rnd := rand.New(rand.NewPCG(seed, 77))
		var stream bytes.Buffer
		var items []item
		var barriers []int
		for i, n := 0, 5+rnd.IntN(120); i < n; i++ {
			it := item{number: rnd.Uint32(), start: stream.Len()}
			if rnd.IntN(3) == 0 {
				it.kind = KindCancel
				it.frames = []int{it.start}
				_ = Cancel(&stream, it.number)
			} else {
				it.kind = KindRequest
				size := 0
				switch rnd.IntN(6) {
				case 0:
				case 1, 2, 3:
					size = rnd.IntN(200)
				case 4:
					size = rnd.IntN(6000)
				case 5:
					size = rnd.IntN(40_000)
				}
				it.payload = random(rnd, size)
				cut := []int{1 + rnd.IntN(64), 1 + rnd.IntN(5000), MaxFrame}[rnd.IntN(3)]
				if size/cut > 500 {
					cut = MaxFrame
				}
				// A message in several frames is announced by a frame of its own, which is its
				// first: the reader stands in the middle of the message once it has read that one.
				begins := it.start
				if size > cut {
					it.frames = append(it.frames, begins)
					begins += HeaderSize + AnnounceSize
				}
				for at, n := 0, 0; ; at, n = at+cut, n+1 {
					it.frames = append(it.frames, begins+at+n*HeaderSize)
					if size-at <= cut {
						break
					}
				}
				_ = Send(&stream, cut, it.kind, it.number, message(it.payload))
			}
			// The reader is sent away somewhere in this item, now and then.
			switch rnd.IntN(8) {
			case 0: // in its first header, or just before it
				barriers = append(barriers, it.start+rnd.IntN(HeaderSize))
			case 1: // anywhere
				barriers = append(barriers, it.start+rnd.IntN(stream.Len()-it.start))
			}
			items = append(items, it)
		}
		src := &attackSource{data: stream.Bytes(), barriers: barriers, rnd: rnd}
		reader := NewReader(src)
		reader.Limits(MaxFrame, 1<<20)
		var last, lastCopy []byte
		check := func(where string, i int) {
			if !bytes.Equal(last, lastCopy) {
				t.Fatalf("seed %d, item %d: what the reader handed out last was changed under its caller by %s", seed, i, where)
			}
		}
	items:
		for i, it := range items {
			// A cancel is read on the side or as a message; of anything else the header may be
			// tried on the side only when the reader is sure to be sent away inside it.
			inHeader := false
			for _, b := range src.barriers {
				if b >= it.start && b < it.start+HeaderSize {
					inHeader = true
				}
			}
			onTheSide := (it.kind == KindCancel || inHeader) && rnd.IntN(2) == 0
			for tries := 0; ; tries++ {
				if tries > len(barriers)+4 {
					t.Fatalf("seed %d, item %d: the reader was sent away %d times and never went on", seed, i, tries)
				}
				if onTheSide {
					kind, number, size, err := reader.Header()
					check("a header read on the side", i)
					if err == errLate {
						// Sent away inside the header: the frame is read by either from here.
						onTheSide = it.kind == KindCancel && rnd.IntN(2) == 0
						continue
					}
					if it.kind != KindCancel {
						t.Fatalf("seed %d, item %d: the header of a request was read on the side to its end, where the reader was to be sent away inside it", seed, i)
					}
					if err != nil || kind != KindCancel || number != it.number || size != 0 {
						t.Fatalf("seed %d, item %d: a cancel of %d read on the side: kind %d, number %d, size %d, %v", seed, i, it.number, kind, number, size, err)
					}
					continue items
				}
				kind, number, payload, err := reader.Next()
				if err == errLate {
					if reader.Midway() {
						// The frames that were read are lost, as the reader says: it must then have been
						// past the first frame of a message of several.
						if len(it.frames) < 2 || src.at < it.frames[1] {
							t.Fatalf("seed %d, item %d: the reader says it stood in the middle of a message, at byte %d of a stream where the second frame begins at %v", seed, i, src.at, it.frames)
						}
						break items
					}
					continue
				}
				if err != nil || kind != it.kind || number != it.number || !bytes.Equal(payload, it.payload) {
					t.Fatalf("seed %d, item %d of %d: kind %d, number %d, %d bytes, %v; sent were kind %d, number %d, %d bytes", seed, i, len(items), kind, number, len(payload), err, it.kind, it.number, len(it.payload))
				}
				last, lastCopy = payload, bytes.Clone(payload)
				continue items
			}
		}
	}
}

// A request read into the value the request before it was read into is the same request as when
// it is read into a new value: nothing of the one before stays in it. (The server reads every
// request of a session into one value.) And what is read is written and read again as the same.
func FuzzAttackARequestReadAfterAnother(f *testing.F) {
	one := []byte(`"x"`)
	for _, req := range []*Request{
		{Op: OpPing},
		{Timeout: 7, Database: "data", Op: OpGet, Get: Get{Collection: "things", ID: "t01", Stamp: true}},
		{Database: "data", Op: OpPut, Put: Put{Collection: "things", Mode: WriteInsert, Document: []byte(`{"_id":"new"}`), Options: Write{Expiry: ExpiryIn, ExpireIn: 1e9, Meta: MetaSet, MetaJSON: []byte(`{}`)}}},
		{Database: "data", Op: OpPutBatch, PutBatch: PutBatch{Collection: "things", Documents: [][]byte{[]byte(`{"_id":"b1"}`), []byte(`{"_id":"b2"}`)}, Options: Write{Expiry: ExpiryAt, ExpireAt: Instant{Seconds: 5, Nanos: 6}, Meta: MetaNone}}},
		{Database: "data", Op: OpPatch, Patch: Patch{Collection: "things", ID: "t02", Patch: []byte(`{"n":null}`)}},
		{Database: "data", Op: OpModify, Modify: Modify{Collection: "things", ID: "t03", Stamp: []byte("12345678"), Before: []byte(`{"n":3}`), After: []byte(`{"n":4}`)}},
		{Database: "data", Op: OpQuery, Query: Query{Collection: "things", HasWhere: true, Where: Cond{Op: 1, Conds: []Cond{{Op: 5, Field: "n", Values: [][]byte{[]byte("5")}}, {Op: 15, Negated: true, Field: "body", Values: [][]byte{one}, Language: "en"}}},
			HasOrder: true, Order: []Order{{Field: "n", Descending: true}, {Relevance: true}}, HasLimit: true, Limit: 7, Offset: 1, After: "token", Answer: AnswerPage, PageSize: 3}},
		{Database: "data", Op: OpQuery, Query: Query{Collection: "things", HasText: true, Text: "n in :list", Args: []Arg{{Name: "list", List: true, Values: [][]byte{[]byte("1"), []byte("2")}}}, Unlimited: true, Answer: AnswerIDs}},
		{Database: "data", Op: OpNext, Cursor: 1},
		{Database: "data", Op: OpBegin, ReadOnly: true, Again: true},
		{Database: "data", Op: OpEnsureIndex, EnsureIndex: EnsureIndex{Collection: "things", Index: Index{Name: "by tags", Fields: []string{"tags", "n desc"}, Unique: true}}},
		{Database: "data", Op: OpDropIndex, Named: IndexRef{Collection: "things", Name: "by n"}},
		{Database: "data", Op: OpIndexes, Indexes: "things"},
		{Op: OpEnsureDatabase, Name: "more"},
		{Op: OpPutUser, PutUser: PutUser{Name: "ann", Role: 2, Password: "a password"}},
		{Op: OpRemoveUser, Name: "ann"},
		{Op: OpUsers},
		{Op: OpSetPassword, SetPassword: SetPassword{Old: "the old one", New: "the new one"}},
		{Op: OpPutIdentity, PutIdentity: PutIdentity{Name: "node-1", Role: 2, Fingerprint: "sha256:00"}},
		{Op: OpRemoveIdentity, Name: "node-1"},
		{Op: OpIdentities},
	} {
		f.Add(req.Append(nil), (&Request{Op: OpPing}).Append(nil))
		f.Add((&Request{Database: "data", Op: OpQuery, Query: Query{Collection: "c", HasText: true, Text: "x", Args: []Arg{{Name: "a", Values: [][]byte{one}}}, HasOrder: true, Order: []Order{{Field: "f"}}}}).Append(nil), req.Append(nil))
	}
	f.Fuzz(func(t *testing.T, before, message []byte) {
		var used, fresh Request
		used.Decode(before)
		// The decoders hand out the bytes of the message where they lie: the message is kept as
		// it is for as long as what was read from it is looked at.
		okUsed, okFresh := used.Decode(message), fresh.Decode(message)
		if okUsed != okFresh {
			t.Fatalf("read after another the message is a request: %v; read alone: %v", okUsed, okFresh)
		}
		if !okFresh {
			return
		}
		if a, b := attackAsked(&used), attackAsked(&fresh); a != b {
			t.Fatalf("read after another:\n%s\nread alone:\n%s", a, b)
		}
		// Written and read again.
		var again Request
		if !again.Decode(fresh.Append(nil)) {
			t.Fatalf("what was read is written as what is no request:\n%s", attackAsked(&fresh))
		}
		if a, b := attackAsked(&again), attackAsked(&fresh); a != b {
			t.Fatalf("read:\n%s\nwritten and read again:\n%s", b, a)
		}
	})
}

// attackAsked is what a request asks, as the server would look at it: the fields of every
// request and those of its own kind, and nothing of what the value holds of other kinds.
func attackAsked(m *Request) string {
	var own any
	switch m.Op {
	case OpGet:
		own = m.Get
	case OpExists:
		own = m.Exists
	case OpCount:
		own = m.Count
	case OpPut:
		own = m.Put
	case OpPutBatch:
		own = m.PutBatch
	case OpPatch:
		own = m.Patch
	case OpModify:
		own = m.Modify
	case OpDelete:
		own = m.Delete
	case OpQuery:
		// Of the two ways to ask, the one that was not chosen is not looked at: what a message
		// that states both leaves in it does not count.
		q := m.Query
		if !q.HasWhere {
			q.Where = Cond{}
		}
		if !q.HasText {
			q.Text, q.Args = "", nil
		}
		own = q
	case OpNext, OpCloseCursor:
		own = m.Cursor
	case OpBegin:
		own = [2]bool{m.ReadOnly, m.Again}
	case OpEnsureIndex:
		own = m.EnsureIndex
	case OpRebuildIndex, OpDropIndex:
		own = m.Named
	case OpIndexes:
		own = m.Indexes
	case OpDropCollection, OpEnsureDatabase, OpDropDatabase, OpRemoveUser, OpRemoveIdentity:
		own = m.Name
	case OpPutIdentity:
		own = m.PutIdentity
	case OpPutUser:
		own = m.PutUser
	case OpSetPassword:
		own = m.SetPassword
	}
	return fmt.Sprintf("timeout %d, database %q, op %d: %+v", m.Timeout, m.Database, m.Op, own)
}

// Whatever a server answers, the decoder of a driver neither panics nor reads past the message,
// with the identifiers copied or left where they lie.
func FuzzAttackAnAnswerOfAnything(f *testing.F) {
	f.Add((&Response{Result: ResultDone}).Append(nil))
	f.Add((&Response{Result: ResultError, Error: Error{Code: CodeProtocol, Message: "no"}}).Append(nil))
	f.Add((&Response{Result: ResultDocument, Document: Document{ID: "a", JSON: []byte(`{}`), Revision: 3, Modified: 4, Expires: 5, Meta: []byte(`{}`), Stamp: []byte("s")}}).Append(nil))
	f.Add((&Response{Result: ResultBatch, Batch: Batch{Documents: []Document{{ID: "a"}, {ID: "b", JSON: []byte(`1`)}}, IDs: []string{"x", "y"}, Cursor: 9, NextPage: "p"}}).Append(nil))
	f.Add((&Response{Result: ResultStored, Stored: []string{"a", "b"}}).Append(nil))
	f.Add((&Response{Result: ResultUsers, Users: []User{{Name: "root", Role: 3}, {Name: "reader", Role: 1}}}).Append(nil))
	f.Add((&Response{Result: ResultIdentities, Identities: []Identity{{Name: "node-1", Role: 3, Fingerprint: "sha256:00"}}}).Append(nil))
	f.Add((&Response{Result: ResultIndexes, Indexes: []Index{{Name: "n", Fields: []string{"f"}, Unique: true}}}).Append(nil))
	f.Add((&Response{Result: ResultStats, Stats: Stats{DiskBytes: 5, Collections: []CollectionStats{{Name: "c", Documents: 2, Indexes: []IndexStats{{Index: Index{Name: "n"}}}}}}}).Append(nil))
	f.Fuzz(func(t *testing.T, message []byte) {
		for _, alias := range []bool{false, true} {
			// The message lies at the very end of its memory, so that a read past it is seen.
			exact := make([]byte, len(message))
			copy(exact, message)
			resp := Response{AliasIDs: alias}
			ok := resp.Decode(exact)
			var again Response
			if ok2 := again.Decode(exact); ok != ok2 {
				t.Fatalf("the same answer is one (%v) and is none (%v)", ok, ok2)
			}
			if !ok {
				continue
			}
			var back Response
			if !back.Decode(resp.Append(nil)) {
				t.Fatalf("an answer that was read (result %d) is written as what is none", resp.Result)
			}
			if back.Result != resp.Result {
				t.Fatalf("an answer of result %d is written and read again as one of result %d", resp.Result, back.Result)
			}
		}
	})
}

// The requests about users are written and read as what they are, the list of users too. A
// request that carried a password lets go of it when it is told to, and of nothing else.
func TestTheRequestsAboutUsers(t *testing.T) {
	for _, req := range []*Request{
		{Timeout: 3, Op: OpPutUser, PutUser: PutUser{Name: "ann", Role: 2, Password: "a password"}},
		{Op: OpPutUser, PutUser: PutUser{Name: "ann", Role: 3}},
		{Op: OpPutUser, PutUser: PutUser{Name: "ann", Password: "another"}},
		{Op: OpRemoveUser, Name: "ann"},
		{Op: OpUsers},
		{Op: OpSetPassword, SetPassword: SetPassword{Old: "the old one", New: "the new one"}},
	} {
		var read Request
		if !read.Decode(req.Append(nil)) || read.Op != req.Op || read.Timeout != req.Timeout || read.Name != req.Name || read.PutUser != req.PutUser || read.SetPassword != req.SetPassword {
			t.Errorf("written %+v, read %+v", req, read)
		}
		if secret := req.Op == OpPutUser || req.Op == OpSetPassword; read.Secret() != secret {
			t.Errorf("op %d carries a password: %v", req.Op, read.Secret())
		}
		read.Forget()
		if read.PutUser.Password != "" || read.SetPassword != (SetPassword{}) {
			t.Errorf("op %d: a password is still held after it was to be forgotten: %+v %+v", req.Op, read.PutUser, read.SetPassword)
		}
		if read.PutUser.Name != req.PutUser.Name || read.PutUser.Role != req.PutUser.Role || read.Name != req.Name {
			t.Errorf("op %d: more than the password was forgotten", req.Op)
		}
	}
	// A request read into the value a request about a user was read into holds none of it.
	var used Request
	if !used.Decode((&Request{Op: OpPutUser, PutUser: PutUser{Name: "ann", Role: 2, Password: "a password"}}).Append(nil)) {
		t.Fatal("a request to make a user is not read")
	}
	if !used.Decode((&Request{Op: OpPutUser}).Append(nil)) || used.PutUser != (PutUser{}) {
		t.Errorf("a request to make a user that says nothing reads as %+v", used.PutUser)
	}
	if !used.Decode((&Request{Op: OpSetPassword, SetPassword: SetPassword{Old: "o", New: "n"}}).Append(nil)) || !used.Decode((&Request{Op: OpSetPassword}).Append(nil)) || used.SetPassword != (SetPassword{}) {
		t.Errorf("a request for a new password that says nothing reads as %+v", used.SetPassword)
	}

	list := &Response{Result: ResultUsers, Users: []User{{Name: "root", Role: 3}, {Name: "reader", Role: 1}, {Name: "nobody"}}}
	var read Response
	if !read.Decode(list.Append(nil)) || read.Result != ResultUsers || len(read.Users) != 3 {
		t.Fatalf("a list of users reads as %+v", read)
	}
	for i, u := range list.Users {
		if read.Users[i] != u {
			t.Errorf("user %d written %+v, read %+v", i, u, read.Users[i])
		}
	}
	// An empty list is a list, and the value it is read into holds none of the list before.
	if !read.Decode((&Response{Result: ResultUsers}).Append(nil)) || read.Result != ResultUsers || len(read.Users) != 0 {
		t.Errorf("an empty list of users reads as %+v", read)
	}
	// A list that describes more than an answer may hold is refused as too large.
	many := &Response{Result: ResultUsers}
	for i := 0; i < 1000; i++ {
		many.Users = append(many.Users, User{Name: "u", Role: 1})
	}
	small := Response{Room: 512}
	if small.Decode(many.Append(nil)) || !small.Over {
		t.Errorf("a list of a thousand users was read into room for 512 bytes: over %v", small.Over)
	}
}

// The requests about identities are written and read as what they are, the list of identities
// too.
func TestTheRequestsAboutIdentities(t *testing.T) {
	print := "sha256:" + strings.Repeat("a", 64)
	for _, req := range []*Request{
		{Timeout: 3, Op: OpPutIdentity, PutIdentity: PutIdentity{Name: "node-1", Role: 2, Fingerprint: print}},
		{Op: OpPutIdentity, PutIdentity: PutIdentity{Name: "node-1", Role: 3}},
		{Op: OpPutIdentity, PutIdentity: PutIdentity{Name: "node-1", Fingerprint: print}},
		{Op: OpRemoveIdentity, Name: "node-1"},
		{Op: OpIdentities},
	} {
		var read Request
		if !read.Decode(req.Append(nil)) || read.Op != req.Op || read.Timeout != req.Timeout || read.Name != req.Name || read.PutIdentity != req.PutIdentity {
			t.Errorf("written %+v, read %+v", req, read)
		}
		// A fingerprint is no secret: nothing of these requests is to be forgotten.
		if read.Secret() {
			t.Errorf("op %d is taken to carry a password", req.Op)
		}
	}
	// A request read into the value one about an identity was read into holds none of it.
	var used Request
	if !used.Decode((&Request{Op: OpPutIdentity, PutIdentity: PutIdentity{Name: "node-1", Role: 2, Fingerprint: print}}).Append(nil)) {
		t.Fatal("a request to make an identity is not read")
	}
	if !used.Decode((&Request{Op: OpPutIdentity}).Append(nil)) || used.PutIdentity != (PutIdentity{}) {
		t.Errorf("a request to make an identity that says nothing reads as %+v", used.PutIdentity)
	}

	list := &Response{Result: ResultIdentities, Identities: []Identity{{Name: "node-1", Role: 3, Fingerprint: print}, {Name: "node-2", Role: 1, Fingerprint: "sha256:" + strings.Repeat("b", 64)}, {Name: "nobody"}}}
	var read Response
	if !read.Decode(list.Append(nil)) || read.Result != ResultIdentities || len(read.Identities) != 3 {
		t.Fatalf("a list of identities reads as %+v", read)
	}
	for i, id := range list.Identities {
		if read.Identities[i] != id {
			t.Errorf("identity %d written %+v, read %+v", i, id, read.Identities[i])
		}
	}
	if !read.Decode((&Response{Result: ResultIdentities}).Append(nil)) || read.Result != ResultIdentities || len(read.Identities) != 0 {
		t.Errorf("an empty list of identities reads as %+v", read)
	}
	many := &Response{Result: ResultIdentities}
	for i := 0; i < 1000; i++ {
		many.Identities = append(many.Identities, Identity{Name: "n", Role: 1, Fingerprint: print})
	}
	small := Response{Room: 512}
	if small.Decode(many.Append(nil)) || !small.Over {
		t.Errorf("a list of a thousand identities was read into room for 512 bytes: over %v", small.Over)
	}
}
