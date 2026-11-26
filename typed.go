package syngodrv

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/syngodb/syngo"
	"github.com/syngodb/syngodrv/wire"
)

// TypedCollection is a view of a collection through one Go type: the same collection, the same
// documents, read and written as values of T by their json tags. Nothing about T is stored: two
// views of different types, and the untyped Collection itself, can be used on one collection at
// the same time.
type TypedCollection[T any] struct {
	c *Collection
}

// Typed views a collection through the type T: documents go in and come out as values of T, by
// its json tags. The identifier of a document is the field tagged `json:"_id"`:
//
//	type User struct {
//		ID   string `json:"_id"`
//		Name string `json:"name"`
//	}
//
// A value stored with that field empty is given a new identifier, which Put and Insert return.
func Typed[T any](c *Collection) *TypedCollection[T] { return &TypedCollection[T]{c: c} }

// Collection is the collection under the view.
func (t *TypedCollection[T]) Collection() *Collection { return t.c }

// Put stores a value, replacing the document that has its identifier if there is one, and returns
// the identifier.
func (t *TypedCollection[T]) Put(ctx context.Context, v T, options ...syngo.WriteOption) (id string, err error) {
	return t.c.Put(ctx, any(v), options...)
}

// Insert stores a new value and returns its identifier, or syngo.ErrExists.
func (t *TypedCollection[T]) Insert(ctx context.Context, v T, options ...syngo.WriteOption) (id string, err error) {
	return t.c.Insert(ctx, any(v), options...)
}

// Replace stores a value in the place of the document that has its identifier, or returns
// syngo.ErrNotFound.
func (t *TypedCollection[T]) Replace(ctx context.Context, v T, options ...syngo.WriteOption) error {
	return t.c.Replace(ctx, any(v), options...)
}

// PutMany stores values, all of them or none, and returns their identifiers in the order given.
func (t *TypedCollection[T]) PutMany(ctx context.Context, values []T, options ...syngo.WriteOption) ([]string, error) {
	docs := make([]any, len(values))
	for i := range values {
		docs[i] = values[i]
	}
	return t.c.PutMany(ctx, docs, options...)
}

// Patch changes some members of a document; see Collection.Patch.
func (t *TypedCollection[T]) Patch(ctx context.Context, id string, patch any, options ...syngo.WriteOption) error {
	return t.c.Patch(ctx, id, patch, options...)
}

// Delete removes the document with the identifier.
func (t *TypedCollection[T]) Delete(ctx context.Context, id string) error { return t.c.Delete(ctx, id) }

// undecoded is the error of a document that does not fit the Go value it was to be decoded into:
// it names the document, which the decoder's own error does not.
func undecoded(collection, id string, err error) error {
	return fmt.Errorf("syngo: the document %q of %q does not decode into the value given: %w", id, collection, err)
}

// Get returns the document with the identifier as a T, or syngo.ErrNotFound.
func (t *TypedCollection[T]) Get(ctx context.Context, id string) (v T, err error) {
	var failed error
	err = t.c.call(ctx, true, func(req *wire.Request) {
		req.Op, req.Get = wire.OpGet, wire.Get{Collection: t.c.name, ID: id}
	}, func(resp *wire.Response) {
		// The value is read where the document lies: no copy of the text is made for it.
		if err := syngo.DecodeJSON(resp.Document.JSON, &v); err != nil {
			failed = undecoded(t.c.name, id, err)
		}
	})
	if err == nil {
		err = failed
	}
	if err != nil {
		var zero T
		return zero, err
	}
	return v, nil
}

// Exists says whether a document has the identifier.
func (t *TypedCollection[T]) Exists(ctx context.Context, id string) (bool, error) {
	return t.c.Exists(ctx, id)
}

// Count is the number of documents in the collection, exactly.
func (t *TypedCollection[T]) Count(ctx context.Context) (int64, error) { return t.c.Count(ctx) }

// EnsureIndex makes sure the collection has the index.
func (t *TypedCollection[T]) EnsureIndex(ctx context.Context, def syngo.Index) error {
	return t.c.EnsureIndex(ctx, def)
}

// RebuildIndex builds an index again from the documents.
func (t *TypedCollection[T]) RebuildIndex(ctx context.Context, name string) error {
	return t.c.RebuildIndex(ctx, name)
}

// DropIndex removes an index.
func (t *TypedCollection[T]) DropIndex(ctx context.Context, name string) error {
	return t.c.DropIndex(ctx, name)
}

// Indexes lists the collection's indexes, by name.
func (t *TypedCollection[T]) Indexes(ctx context.Context) ([]syngo.Index, error) {
	return t.c.Indexes(ctx)
}

// staleTries is how many times Modify reads a document and runs its function again, because the
// document was written in between, before it does the same inside a transaction, which is sure
// to finish.
const staleTries = 3

// Modify reads a document, hands it to fn as a value of T to change, and stores the document with
// the changes fn made, as one: nobody can change the document in between.
//
//	err := users.Modify(ctx, "u1", func(u *User) error { u.Visits++; return nil })
//
// What fn did not change stays exactly as it was stored, what T does not declare included. A
// field fn gave another value takes it; a field fn emptied, which T leaves out when it is empty,
// is removed from the document; a list fn changed is replaced whole.
//
// The function runs in this program, and the server holds nothing while it does: the document
// is read with a stamp, and the change is taken by the server only if the document still has
// it. When it was written in between, the document is read again and fn runs again: fn may run
// more than once. It returns syngo.ErrNotFound when there is no such document, and whatever
// error fn returns, in which case nothing is written.
func (t *TypedCollection[T]) Modify(ctx context.Context, id string, fn func(v *T) error, options ...syngo.WriteOption) error {
	stated, err := syngo.PlainWrite(options)
	if err != nil {
		return err
	}
	if t.c.tx != nil {
		// A transaction already makes the read and the write one.
		return t.modify(ctx, t.c, id, fn, &stated, false)
	}
	for try := 0; try < staleTries; try++ {
		if err := t.modify(ctx, t.c, id, fn, &stated, true); !errors.Is(err, ErrStale) {
			return err
		}
	}
	return t.c.db.Update(ctx, func(tx *Tx) error {
		return t.modify(ctx, tx.Collection(t.c.name), id, fn, &stated, false)
	})
}

// modify is one attempt: read, run the function, send what it changed.
func (t *TypedCollection[T]) modify(ctx context.Context, c *Collection, id string, fn func(v *T) error, stated *syngo.Write, stamped bool) error {
	var (
		v      T
		stamp  []byte
		before []byte
		failed error
	)
	err := c.call(ctx, true, func(req *wire.Request) {
		req.Op, req.Get = wire.OpGet, wire.Get{Collection: c.name, ID: id, Stamp: stamped}
	}, func(resp *wire.Response) {
		stamp = append(stamp, resp.Document.Stamp...)
		if err := syngo.DecodeJSON(resp.Document.JSON, &v); err != nil {
			failed = undecoded(c.name, id, err)
			return
		}
		// The value as T writes it, before fn and after: the difference between the two is what
		// fn did, and it is that difference the stored document is given.
		if before, failed = syngo.EncodeJSON(&v); failed != nil {
			failed = &syngo.DocumentError{Reason: "the value cannot be encoded as JSON: " + failed.Error(), Offset: -1}
		}
	})
	if err == nil {
		err = failed
	}
	if err != nil {
		return err
	}
	if stamped && len(stamp) == 0 {
		return fmt.Errorf("%w: the server sent the document without the stamp it was asked for", ErrProtocol)
	}
	if err := fn(&v); err != nil {
		return err
	}
	after, err := syngo.EncodeJSON(&v)
	if err != nil {
		return &syngo.DocumentError{Reason: "the value cannot be encoded as JSON: " + err.Error(), Offset: -1}
	}
	return c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.Modify = wire.OpModify, wire.Modify{Collection: c.name, ID: id, Stamp: stamp, Before: before, After: after, Options: writeOf(stated)}
	}, nil)
}

// Where asks for the documents that satisfy every condition, as values of T.
func (t *TypedCollection[T]) Where(conds ...syngo.Cond) *TypedQuery[T] {
	return &TypedQuery[T]{q: t.c.Where(conds...)}
}

// Query asks the collection a question written in SyngoQL, answered as values of T.
func (t *TypedCollection[T]) Query(text string, args ...syngo.Args) *TypedQuery[T] {
	return &TypedQuery[T]{q: t.c.Query(text, args...)}
}

// TypedQuery is a Query whose answers are values of T.
type TypedQuery[T any] struct {
	q *Query
}

// Query is the query under the view.
func (t *TypedQuery[T]) Query() *Query { return t.q }

// OrderBy orders the answer by the fields, in turn.
func (t *TypedQuery[T]) OrderBy(order ...syngo.Order) *TypedQuery[T] {
	return &TypedQuery[T]{q: t.q.OrderBy(order...)}
}

// Limit keeps at most n documents of the answer.
func (t *TypedQuery[T]) Limit(n int) *TypedQuery[T] { return &TypedQuery[T]{q: t.q.Limit(n)} }

// Offset skips the first n documents of the answer.
func (t *TypedQuery[T]) Offset(n int) *TypedQuery[T] { return &TypedQuery[T]{q: t.q.Offset(n)} }

// After resumes the answer after the place a token stands for; see Query.After.
func (t *TypedQuery[T]) After(token string) *TypedQuery[T] {
	return &TypedQuery[T]{q: t.q.After(token)}
}

// TypedPage is one page of an answer, as values.
type TypedPage[T any] struct {
	// Items are the values of the page, in order.
	Items []T
	// Next is the token After takes to read the page that follows; it is empty when this page is
	// the last.
	Next string
}

// Page answers with the first size values of the answer and the token to go on from there; see
// Query.Page.
func (t *TypedQuery[T]) Page(ctx context.Context, size int) (*TypedPage[T], error) {
	page, err := t.q.Page(ctx, size)
	if err != nil {
		return nil, err
	}
	out := &TypedPage[T]{Items: make([]T, len(page.Items)), Next: page.Next}
	for i, doc := range page.Items {
		if err := doc.Decode(&out.Items[i]); err != nil {
			return nil, undecoded(t.q.c.name, doc.ID(), err)
		}
	}
	return out, nil
}

// All answers the query as a sequence of values: for v, err := range q.All(ctx).
func (t *TypedQuery[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		for doc, err := range t.q.All(ctx) {
			if err != nil {
				yield(zero, err)
				return
			}
			var v T
			if err := doc.Decode(&v); err != nil {
				yield(zero, undecoded(t.q.c.name, doc.ID(), err))
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}

// Slice answers the query as a slice of values.
func (t *TypedQuery[T]) Slice(ctx context.Context) ([]T, error) {
	var out []T
	for v, err := range t.All(ctx) {
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// First answers with the first value of the answer, or syngo.ErrNotFound when the answer is
// empty.
func (t *TypedQuery[T]) First(ctx context.Context) (T, error) {
	var zero T
	doc, err := t.q.First(ctx)
	if err != nil {
		return zero, err
	}
	var v T
	if err := doc.Decode(&v); err != nil {
		return zero, undecoded(t.q.c.name, doc.ID(), err)
	}
	return v, nil
}

// IDs answers with the identifiers of the documents, in the order of the answer.
func (t *TypedQuery[T]) IDs(ctx context.Context) ([]string, error) { return t.q.IDs(ctx) }

// Count is the number of documents in the answer.
func (t *TypedQuery[T]) Count(ctx context.Context) (int64, error) { return t.q.Count(ctx) }

// Explain says how the server would answer the query.
func (t *TypedQuery[T]) Explain(ctx context.Context) (string, error) { return t.q.Explain(ctx) }
