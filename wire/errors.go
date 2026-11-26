package wire

import (
	"errors"

	"github.com/syngodb/syngo"
)

// The errors of the public package cross the wire as their code, with what the ones that say
// more carry. This file is the one place that knows which error is which code, in both
// directions: a server writes with FromError what a driver reads back with Err, so a program
// meets the same error whether its database is embedded or behind a server.

// plain are the errors that are a code and nothing more.
var plain = []struct {
	code uint32
	err  error
}{
	{CodeNotFound, syngo.ErrNotFound},
	{CodeExists, syngo.ErrExists},
	{CodeReadOnly, syngo.ErrReadOnly},
	{CodeTxDone, syngo.ErrTxDone},
	{CodeTxExpired, syngo.ErrTxExpired},
	{CodeClosed, syngo.ErrClosed},
	{CodeInvalidIndex, syngo.ErrInvalidIndex},
	{CodeInvalidName, syngo.ErrInvalidName},
	{CodeFailed, syngo.ErrFailed},
	{CodeTooLarge, syngo.ErrTooLarge},
	{CodeCorrupt, syngo.ErrCorrupt},
	// The four below have a type that says more; an error that is one of them without being of
	// that type crosses as the code alone.
	{CodeDuplicate, syngo.ErrDuplicate},
	{CodeConflict, syngo.ErrConflict},
	{CodeInvalidDocument, syngo.ErrInvalidDocument},
	{CodeInvalidQuery, syngo.ErrInvalidQuery},
}

// FromError describes an error of the public package. ok is false for any other error, which
// is the caller's to describe.
func FromError(err error) (e Error, ok bool) {
	var (
		duplicate *syngo.DuplicateError
		document  *syngo.DocumentError
		query     *syngo.QueryError
		conflict  *syngo.ConflictError
	)
	switch {
	case err == nil:
		return Error{}, false
	case errors.As(err, &duplicate):
		return Error{Code: CodeDuplicate, Message: err.Error(), Detail: DetailDuplicate,
			Duplicate: Duplicate{Collection: duplicate.Collection, Index: duplicate.Index, Holder: duplicate.Holder}}, true
	case errors.As(err, &document):
		return Error{Code: CodeInvalidDocument, Message: err.Error(), Detail: DetailDocument,
			BadDocument: BadDocument{Reason: document.Reason, Offset: int32(max(document.Offset, -1))}}, true
	case errors.As(err, &query):
		return Error{Code: CodeInvalidQuery, Message: err.Error(), Detail: DetailQuery,
			BadQuery: BadQuery{Reason: query.Reason, Line: uint32(max(query.Line, 0)), Column: uint32(max(query.Column, 0)), Text: query.Text}}, true
	case errors.As(err, &conflict):
		return Error{Code: CodeConflict, Message: err.Error(), Detail: DetailConflict, Attempts: uint32(max(conflict.Attempts, 0))}, true
	}
	for _, p := range plain {
		if errors.Is(err, p.err) {
			return Error{Code: p.code, Message: err.Error()}, true
		}
	}
	return Error{}, false
}

// Err is the error of the public package an Error stands for: of the very type the embedded
// library returns when the Error carries what that type holds, and otherwise an error that is
// (for errors.Is) the one of its code and reads as the server wrote it. It is nil when the code
// is not one of the public package's.
func (e *Error) Err() error {
	switch {
	case e.Code == CodeDuplicate && e.Detail == DetailDuplicate:
		return &syngo.DuplicateError{Collection: e.Duplicate.Collection, Index: e.Duplicate.Index, Holder: e.Duplicate.Holder}
	case e.Code == CodeInvalidDocument && e.Detail == DetailDocument:
		return &syngo.DocumentError{Reason: e.BadDocument.Reason, Offset: int(e.BadDocument.Offset)}
	case e.Code == CodeInvalidQuery && e.Detail == DetailQuery:
		return &syngo.QueryError{Reason: e.BadQuery.Reason, Line: int(e.BadQuery.Line), Column: int(e.BadQuery.Column), Text: e.BadQuery.Text}
	case e.Code == CodeConflict && e.Detail == DetailConflict:
		return &syngo.ConflictError{Attempts: int(e.Attempts)}
	}
	for _, p := range plain {
		if e.Code == p.code {
			if e.Message == "" || e.Message == p.err.Error() {
				return p.err
			}
			return &Remote{Message: e.Message, Of: p.err}
		}
	}
	return nil
}

// Remote is an error a server reported: what the server said of it, and the error of the public
// package it is.
type Remote struct {
	Message string
	Of      error
}

// Error is what the server said.
func (r *Remote) Error() string { return r.Message }

// Unwrap is the error of the public package this one is.
func (r *Remote) Unwrap() error { return r.Of }
