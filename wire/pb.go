package wire

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"
	"unsafe"
)

// The messages are encoded as Protocol Buffers encodes them, so that syngo.proto says all there
// is to say to whoever writes another driver. The encoders are written here by hand: the driver
// then depends on nothing but the public package, a message is read without a copy of what it
// holds, and a test holds every encoder to the code generated from syngo.proto.
//
// A field is its number with the way its value is written (a tag), then the value. A field that
// holds nothing (zero, false, empty) is not written, except the member of a "oneof": that one is
// written always, since being there is what it says.

// The ways a value is written.
const (
	asVarint  = 0 // a number, seven bits a byte
	asFixed64 = 1 // eight bytes, little endian
	asBytes   = 2 // a length, then that many bytes: texts, bytes, messages
	asFixed32 = 5 // four bytes, little endian
)

// ErrMessage is returned for what is not a message of the protocol.
var ErrMessage = errors.New("wire: not a message of the protocol")

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func varintSize(v uint64) int {
	n := 1
	for ; v >= 0x80; v >>= 7 {
		n++
	}
	return n
}

func appendTag(b []byte, field int, how byte) []byte {
	return appendVarint(b, uint64(field)<<3|uint64(how))
}

// tagSize is what the tag of a field takes: every field of the protocol has a number below 2048.
func tagSize(field int) int {
	if field < 16 {
		return 1
	}
	return 2
}

// putUint, putBool, putString, putBytes and putFixed64 write a field that is not written when it
// holds nothing.
func putUint(b []byte, field int, v uint64) []byte {
	if v == 0 {
		return b
	}
	return appendVarint(appendTag(b, field, asVarint), v)
}

func putBool(b []byte, field int, v bool) []byte {
	if !v {
		return b
	}
	return append(appendTag(b, field, asVarint), 1)
}

func putString(b []byte, field int, s string) []byte {
	if s == "" {
		return b
	}
	return append(appendVarint(appendTag(b, field, asBytes), uint64(len(s))), s...)
}

func putBytes(b []byte, field int, p []byte) []byte {
	if len(p) == 0 {
		return b
	}
	return append(appendVarint(appendTag(b, field, asBytes), uint64(len(p))), p...)
}

func putFixed64(b []byte, field int, v uint64) []byte {
	if v == 0 {
		return b
	}
	return binary.LittleEndian.AppendUint64(appendTag(b, field, asFixed64), v)
}

// setUint, setBool, setString and setBytes write a field always: the member of a oneof, and each
// value of a list.
func setUint(b []byte, field int, v uint64) []byte {
	return appendVarint(appendTag(b, field, asVarint), v)
}

func setBool(b []byte, field int, v bool) []byte {
	if v {
		return append(appendTag(b, field, asVarint), 1)
	}
	return append(appendTag(b, field, asVarint), 0)
}

func setString(b []byte, field int, s string) []byte {
	return append(appendVarint(appendTag(b, field, asBytes), uint64(len(s))), s...)
}

func setBytes(b []byte, field int, p []byte) []byte {
	return append(appendVarint(appendTag(b, field, asBytes), uint64(len(p))), p...)
}

// stringSize and bytesSize are what a field written always takes, and uintSize what a number
// that is not zero takes.
func stringSize(field int, n int) int { return tagSize(field) + varintSize(uint64(n)) + n }

func uintSize(field int, v uint64) int { return tagSize(field) + varintSize(v) }

// open begins a message inside a message, whose size is not known yet: it leaves one byte for
// the length, which is enough for nearly all of them, and returns where. shut writes the length
// there once the message is written, and makes room when one byte is not enough.
func open(b []byte, field int) ([]byte, int) {
	b = appendTag(b, field, asBytes)
	return append(b, 0), len(b)
}

func shut(b []byte, at int) []byte {
	size := len(b) - at - 1
	if size < 0x80 {
		b[at] = byte(size)
		return b
	}
	more := varintSize(uint64(size)) - 1
	for i := 0; i < more; i++ {
		b = append(b, 0)
	}
	copy(b[at+1+more:], b[at+1:at+1+size])
	binary.PutUvarint(b[at:], uint64(size))
	return b
}

// decoder goes through the fields of a message. Once it met something that is no message, bad
// is set and it yields no further field.
type decoder struct {
	b   []byte
	bad bool
	// bud is what the decoding of the whole message may still take, shared by the decoders of
	// the messages inside it, or nil when nothing is counted.
	bud *budget
}

// DefaultRoom is the most the lists of one decoded message take, beside the bytes of the message
// itself, unless the reader says otherwise.
const DefaultRoom = 8 << 20

// budget bounds what a message turns into once it is decoded. The bytes of a message are
// bounded by the session; what they are decoded into is not bounded by that. An element of a
// list that holds nothing takes two bytes of the message and a whole value of the list in
// memory: a megabyte of conditions that say nothing was 54 megabytes of conditions. So every
// element a decoder adds to a list is counted, and a message that asks for more than the room
// it is given is refused as too large, which is not the same as being no message.
type budget struct {
	room int
	over bool
}

// take counts n bytes for an element that is about to be added to a list. It reports whether
// there was room.
func (d *decoder) take(n uintptr) bool {
	if d.bud == nil {
		return true
	}
	if d.bud.room -= int(n); d.bud.room < 0 {
		d.bud.over, d.bad = true, true
		return false
	}
	return true
}

// next is the next field: its number and the way its value is written. ok is false at the end
// of the message, and when it is no message.
func (d *decoder) next() (field int, how byte, ok bool) {
	if d.bad || len(d.b) == 0 {
		return 0, 0, false
	}
	tag := d.varint()
	field, how = int(tag>>3), byte(tag&7)
	if d.bad || tag>>3 == 0 || tag>>3 > 1<<29-1 {
		d.bad = true
		return 0, 0, false
	}
	return field, how, true
}

func (d *decoder) varint() uint64 {
	// One byte is the usual case.
	if len(d.b) > 0 && d.b[0] < 0x80 {
		v := uint64(d.b[0])
		d.b = d.b[1:]
		return v
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.bad = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

// uint, bool, bytes, text and fixed64 read the value of a field that was written the way they
// expect, and pass over one that was not, as a field nobody knows: so does the generated code.
func (d *decoder) uint(how byte) uint64 {
	if how != asVarint {
		d.skip(how)
		return 0
	}
	return d.varint()
}

func (d *decoder) bool(how byte) bool { return d.uint(how) != 0 }

// small, u32 and i32 read a number that is kept in fewer bits than it travels in. One that does
// not fit is the largest that does, never another number: the kind of answer 260 must not be
// read as the kind 4, which is a removal.
func (d *decoder) small(how byte) uint8 {
	return uint8(min(d.uint(how), 0xff))
}

func (d *decoder) u32(how byte) uint32 {
	return uint32(min(d.uint(how), 0xffffffff))
}

func (d *decoder) i32(how byte) int32 {
	// A number below zero travels as the 64 bits of itself.
	return int32(min(max(int64(d.uint(how)), -1<<31), 1<<31-1))
}

// bytes returns the value where it lies: in the message, with nothing copied.
func (d *decoder) bytes(how byte) []byte {
	if how != asBytes {
		d.skip(how)
		return nil
	}
	size := d.varint()
	if d.bad || size > uint64(len(d.b)) {
		d.bad = true
		return nil
	}
	p := d.b[:size:size]
	d.b = d.b[size:]
	return p
}

// inner returns a decoder of a message that lies inside the message. ok is false when the field
// was not written the way a message is, and was passed over.
func (d *decoder) inner(how byte) (in decoder, ok bool) {
	if how != asBytes {
		d.skip(how)
		return decoder{}, false
	}
	return decoder{b: d.bytes(how), bud: d.bud}, !d.bad
}

// text returns a string of its own. A text that is not UTF-8 is no message.
func (d *decoder) text(how byte) string {
	p := d.bytes(how)
	if !utf8.Valid(p) {
		d.bad = true
		return ""
	}
	return string(p)
}

// lying returns a text where it lies in the message, with nothing copied: it holds for as long
// as the message does. A text that is not UTF-8 is no message.
func (d *decoder) lying(how byte) string {
	p := d.bytes(how)
	if !utf8.Valid(p) {
		d.bad = true
		return ""
	}
	return unsafe.String(unsafe.SliceData(p), len(p))
}

func (d *decoder) fixed64(how byte) uint64 {
	if how != asFixed64 {
		d.skip(how)
		return 0
	}
	if len(d.b) < 8 {
		d.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b)
	d.b = d.b[8:]
	return v
}

// rest passes over every field that is left: of a message that holds nothing this package
// reads, it is still asked that it be a message.
func (d *decoder) rest() {
	for {
		_, how, ok := d.next()
		if !ok {
			return
		}
		d.skip(how)
	}
}

// skip passes over the value of a field nobody knows.
func (d *decoder) skip(how byte) {
	switch how {
	case asVarint:
		d.varint()
	case asFixed64:
		if len(d.b) < 8 {
			d.bad = true
			return
		}
		d.b = d.b[8:]
	case asBytes:
		size := d.varint()
		if d.bad || size > uint64(len(d.b)) {
			d.bad = true
			return
		}
		d.b = d.b[size:]
	case asFixed32:
		if len(d.b) < 4 {
			d.bad = true
			return
		}
		d.b = d.b[4:]
	default:
		d.bad = true
	}
}

// numbers reads a message made of numbers only, by their field numbers one to six: a field that
// is not there is zero.
func (d *decoder) numbers() (v [6]uint64) {
	for {
		field, how, ok := d.next()
		if !ok {
			return v
		}
		if field >= 1 && field <= len(v) {
			v[field-1] = d.uint(how)
		} else {
			d.skip(how)
		}
	}
}

// grow adds one element to the end of s, which holds what it held before when s has room.
func grow[T any](s []T) []T {
	if len(s) < cap(s) {
		return s[:len(s)+1]
	}
	var zero T
	return append(s, zero)
}
