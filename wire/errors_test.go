package wire

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/syngodb/syngo"
)

// Every error of the public package crosses the wire and comes out as itself: the same for
// errors.Is, of the same type with the same fields when it has one, and saying the same.
func TestErrorsCrossTheWireAsThemselves(t *testing.T) {
	sentinels := []error{
		syngo.ErrNotFound, syngo.ErrExists, syngo.ErrDuplicate, syngo.ErrConflict, syngo.ErrReadOnly, syngo.ErrTxDone, syngo.ErrTxExpired,
		syngo.ErrClosed, syngo.ErrInvalidDocument, syngo.ErrInvalidQuery, syngo.ErrInvalidIndex, syngo.ErrInvalidName, syngo.ErrFailed,
		syngo.ErrTooLarge, syngo.ErrCorrupt,
	}
	cross := func(err error) error {
		t.Helper()
		described, ok := FromError(err)
		if !ok {
			t.Fatalf("%v is not described", err)
		}
		// Through the encoder and back, as a response.
		var read Response
		if !read.Decode((&Response{Result: ResultError, Error: described}).Append(nil)) || read.Result != ResultError {
			t.Fatalf("%v: the response is not read back", err)
		}
		return read.Error.Err()
	}
	seen := map[uint32]bool{}
	for _, want := range sentinels {
		for _, err := range []error{want, fmt.Errorf("%w: and what the library says beside it", want)} {
			got := cross(err)
			if got == nil || got.Error() != err.Error() {
				t.Errorf("%q comes out as %q", err, got)
			}
			for _, other := range sentinels {
				if errors.Is(got, other) != errors.Is(err, other) {
					t.Errorf("%q: errors.Is(%q) is %v before and %v after", err, other, errors.Is(err, other), errors.Is(got, other))
				}
			}
		}
		described, _ := FromError(want)
		seen[described.Code] = true
	}
	if len(seen) != len(sentinels) {
		t.Errorf("%d errors share %d codes", len(sentinels), len(seen))
	}
	// The ones that say more keep what they say.
	for _, want := range []error{
		&syngo.DuplicateError{Collection: "users", Index: "by email", Holder: "u1"},
		&syngo.DocumentError{Reason: "a document is a JSON object", Offset: -1},
		&syngo.DocumentError{Reason: "an unexpected character", Offset: 17},
		&syngo.QueryError{Reason: "the field has no name"},
		&syngo.QueryError{Reason: "an operator is expected", Line: 2, Column: 9, Text: "age >> 3"},
		&syngo.ConflictError{Attempts: 8},
	} {
		for _, err := range []error{want, fmt.Errorf("wrapped by the library: %w", want)} {
			got := cross(err)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%#v comes out as %#v", want, got)
			}
		}
	}
	// What is no error of the public package is left to whoever has it.
	for _, err := range []error{nil, errors.New("something else"), syngo.ErrOutcomeUnknown} {
		if described, ok := FromError(err); ok {
			t.Errorf("%v is described as %+v", err, described)
		}
	}
	for _, code := range []uint32{0, CodeUnauthenticated, CodeForbidden, CodeNoDatabase, CodeStale, CodeBusy, CodeCancelled, CodeUnsupported, CodeProtocol, CodeInternal, 99} {
		if err := (&Error{Code: code, Message: "said by the server"}).Err(); err != nil {
			t.Errorf("the code %d is taken for the error %v of the public package", code, err)
		}
	}
	// A code alone, with nothing said, is the plain error.
	if err := (&Error{Code: CodeNotFound}).Err(); err != syngo.ErrNotFound {
		t.Errorf("the code of not found alone is %v", err)
	}
	// A code whose detail is missing is still the error of its code.
	if err := (&Error{Code: CodeDuplicate, Message: "taken"}).Err(); !errors.Is(err, syngo.ErrDuplicate) || err.Error() != "taken" {
		t.Errorf("a duplicate without its detail is %v", err)
	}
}
