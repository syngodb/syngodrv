package wire

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"os"
	"testing"
)

// The tests of this file were written by a reviewer whose task was to break the reader of the
// frames where it announces a message before it takes it (Announce, Over, Await, Held and
// Release), as a hostile peer would. A test named TestAttackFixed... shows a defect and fails
// on the code as it stood when the review was made. A test named TestAttackHeld... is an attack
// the code withstood, and passes.

// attack4Pieces is a connection that hands out what it was given, a piece to a read, and then
// says that nothing more came in time.
type attack4Pieces struct{ parts [][]byte }

func (p *attack4Pieces) Read(b []byte) (int, error) {
	for len(p.parts) > 0 && len(p.parts[0]) == 0 {
		p.parts = p.parts[1:]
	}
	if len(p.parts) == 0 {
		return 0, os.ErrDeadlineExceeded
	}
	n := copy(b, p.parts[0])
	p.parts[0] = p.parts[0][n:]
	return n, nil
}

// Announce is for whoever must have room for a message before it takes it. The reader calls it
// for a message in one frame, and for the frame that announces a message in several. Three
// frames are neither, and the reader takes each of them whole, a megabyte, before it looks at
// what it is. The first is a first frame that says the message goes on, and is no announcement:
// it is refused once it was read, for not being eight bytes long. The second is a frame that
// follows an announcement of fewer bytes than a session holds of its own, for which nothing was
// asked: it is refused once it was read, for holding more than was announced. The third is a
// frame that follows an announcement of a little more than that, for which a little was asked.
//
// The cause is in Reader.one, which reads a whole frame before Next looks at what it announces
// or at what was announced before it. The peer sends all of the frame but its last byte, and the megabyte is held for as long as a
// message may take to arrive.
func TestAttackFixedAFrameIsTakenThatNoRoomWasAskedFor(t *testing.T) {
	const over = 64 << 10
	megabyte := make([]byte, MaxFrame)
	for name, stream := range map[string][]byte{
		"a first frame that goes on and announces nothing":   frame(KindRequest|more, 7, megabyte),
		"a frame of a megabyte after one byte was announced": append(announced(KindRequest, 7, 1), frame(KindRequest, 7, megabyte)...),
		"a frame of a megabyte after 65 KiB were announced":  append(announced(KindRequest, 7, over+1024), frame(KindRequest|more, 7, megabyte)...),
	} {
		r := NewReader(&attack4Pieces{parts: [][]byte{stream[:len(stream)-1]}})
		r.Limits(MaxFrame, 33<<20)
		asked := 0
		r.Over, r.Announce = over, func(n int) error { asked += n; return nil }
		if _, _, _, err := r.Next(); err == nil {
			t.Errorf("%s: taken for a message", name)
			continue
		}
		if held := r.Held(); held > over+asked+keep {
			t.Errorf("%s: the reader holds %d bytes, and room was asked for %d (a reader holds %d without asking)", name, held, asked, over)
		}
	}
}

// Messages of every length, in frames of every length, arriving in pieces of every length: the
// reader asks once for each message longer than Over, for its exact length, and never for a
// shorter one; and when it asks it holds no more than a reader that has read nothing.
func TestAttackHeldTheReaderAsksOnceAndBeforeItTakes(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 4))
	for round := 0; round < 400; round++ {
		over := []int{0, 1, 100, 4096, 64 << 10}[rng.IntN(5)]
		size := rng.IntN(300 << 10)
		if round%7 == 0 {
			// At the edge: as long as what is held without asking, and a byte more or less.
			size = max(0, over-1+rng.IntN(3))
		}
		payload := random(rng, size)
		var stream bytes.Buffer
		if err := Send(&stream, 1+rng.IntN(64<<10), KindRequest, uint32(round+1), message(payload)); err != nil {
			t.Fatal(err)
		}
		src := &attack4Pieces{}
		for rest := stream.Bytes(); len(rest) > 0; {
			n := min(len(rest), 1+rng.IntN(40<<10))
			src.parts, rest = append(src.parts, rest[:n]), rest[n:]
		}
		r := NewReader(src)
		r.Limits(MaxFrame, 1<<20)
		calls, heldThen := 0, 0
		r.Over, r.Announce = over, func(n int) error {
			calls++
			if n != size {
				t.Errorf("round %d: a message of %d bytes was announced as one of %d", round, size, n)
			}
			heldThen = r.Held()
			return nil
		}
		kind, number, got, err := r.Next()
		if err != nil || kind != KindRequest || number != uint32(round+1) || !bytes.Equal(got, payload) {
			t.Fatalf("round %d: a message of %d bytes: kind %d, number %d, %d bytes, %v", round, size, kind, number, len(got), err)
		}
		want := 0
		if size > over {
			want = 1
		}
		if calls != want {
			t.Errorf("round %d: room was asked %d times for a message of %d bytes, where %d are held without asking", round, calls, size, over)
		}
		if heldThen > first {
			t.Errorf("round %d: the reader held %d bytes when it asked for room for a message of %d", round, heldThen, size)
		}
	}
}

// What Announce returns is the error of Next, with the kind and the number of the message, and
// the reader has then taken nothing of the message: for one in one frame and for one in
// several.
func TestAttackHeldAMessageThatIsRefusedRoomIsNotTaken(t *testing.T) {
	noRoom := errors.New("no room")
	payload := make([]byte, 3<<20)
	for name, limit := range map[string]int{"in one frame": MaxFrame, "in several": 64 << 10} {
		var stream bytes.Buffer
		if err := Send(&stream, limit, KindRequest, 41, message(payload[:min(len(payload), limit*40, MaxFrame)])); err != nil {
			t.Fatal(err)
		}
		src := &attack4Pieces{parts: [][]byte{stream.Bytes()}}
		r := NewReader(src)
		r.Limits(MaxFrame, 8<<20)
		r.Over, r.Announce = 4096, func(int) error { return noRoom }
		kind, number, got, err := r.Next()
		if !errors.Is(err, noRoom) || kind != KindRequest || number != 41 || got != nil {
			t.Errorf("%s: kind %d, number %d, %d bytes, %v", name, kind, number, len(got), err)
		}
		if held := r.Held(); held > first {
			t.Errorf("%s: the reader holds %d bytes of a message it had no room for", name, held)
		}
		if r.Midway() {
			t.Errorf("%s: the reader says it stands in the middle of a message it never began to take", name)
		}
	}
}

// A watcher reads the header of a frame on the side, and is sent away in the middle of it. The
// bytes it read are not lost when the reader is told to let go of its memory, and the message
// they begin is announced all the same, once, when Next reads it.
func TestAttackHeldAHeaderBegunOnTheSideIsAnnouncedAllTheSame(t *testing.T) {
	rng := rand.New(rand.NewPCG(4, 5))
	payload := random(rng, 100<<10)
	stream := frame(KindRequest, 9, payload)
	for cut := 1; cut < HeaderSize; cut++ {
		src := &attack4Pieces{parts: [][]byte{stream[:cut]}}
		r := NewReader(src)
		r.Limits(MaxFrame, 1<<20)
		calls := 0
		r.Over, r.Announce = 64<<10, func(n int) error {
			calls++
			if n != len(payload) {
				t.Errorf("cut at %d: announced as %d bytes", cut, n)
			}
			return nil
		}
		if _, _, _, err := r.Header(); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("cut at %d: the header on the side: %v", cut, err)
		}
		r.Release()
		src.parts = [][]byte{stream[cut:]}
		kind, number, got, err := r.Next()
		if err != nil || kind != KindRequest || number != 9 || !bytes.Equal(got, payload) {
			t.Fatalf("cut at %d: kind %d, number %d, %d bytes, %v", cut, kind, number, len(got), err)
		}
		if calls != 1 {
			t.Errorf("cut at %d: room was asked %d times", cut, calls)
		}
	}
}
