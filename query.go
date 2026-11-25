package syngodrv

import (
	"context"
	"iter"

	"github.com/syngodb/syngo"
)

// Query is a question asked of a collection: conditions or a text, an order, a window. It is a
// description, and nothing is sent until one of the methods that answer is called. Each method
// that refines a query returns a new one and leaves the first untouched.
type Query struct {
	c *Collection
	// What is asked: a condition, or a text with the values of its parameters.
	where  syngo.Cond
	text   string
	args   []syngo.Args
	asText bool
	// What refines it. Each travels as a field of its own, never inside the text: ordered says an
	// order was given here (even an empty one), which replaces the one the text states, and
	// limited the same for the limit.
	order   []syngo.Order
	ordered bool
	limit   int
	limited bool
	// unlimited says the limit the text states is taken away.
	unlimited bool
	offset    int
	after     string
}

// Where asks for the documents that satisfy every condition; with no conditions, for every
// document.
func (c *Collection) Where(conds ...syngo.Cond) *Query {
	q := &Query{c: c}
	switch len(conds) {
	case 0:
	case 1:
		q.where = conds[0]
	default:
		q.where = syngo.And(conds...)
	}
	return q
}

// Query asks the collection a question written in SyngoQL, with the values of its parameters.
// The text is read by the server: one that is not a query makes every answer fail with a
// *syngo.QueryError.
func (c *Collection) Query(text string, args ...syngo.Args) *Query {
	return &Query{c: c, text: text, args: append([]syngo.Args(nil), args...), asText: true}
}

// OrderBy orders the answer by the fields, in turn. It replaces the order the query had, the one
// its text stated included; with no argument it takes the order away.
func (q *Query) OrderBy(order ...syngo.Order) *Query {
	next := *q
	next.order, next.ordered = append([]syngo.Order(nil), order...), true
	return &next
}

// Limit keeps at most n documents of the answer: none of them when n is zero. A negative n takes
// away the limit the query had, the one its text stated included.
func (q *Query) Limit(n int) *Query {
	next := *q
	next.limit, next.limited, next.unlimited = max(n, 0), n >= 0, n < 0
	return &next
}

// Offset skips the first n documents of the answer.
func (q *Query) Offset(n int) *Query {
	next := *q
	next.offset = max(n, 0)
	return &next
}

// After resumes the answer after the place a token stands for: the Next of the page before.
func (q *Query) After(token string) *Query {
	next := *q
	next.after = token
	return &next
}

// err is the error the query already has before anything is sent: a condition that cannot be
// one says so here, as it does embedded.
func (q *Query) err() error {
	if !q.asText {
		return q.where.Err()
	}
	return nil
}

func (q *Query) send() error {
	if err := q.err(); err != nil {
		return err
	}
	return ErrNotBuilt
}

// All answers the query as a sequence: for doc, err := range q.All(ctx). The answer is pulled
// from the server a batch at a time, as far as the loop goes.
func (q *Query) All(ctx context.Context) iter.Seq2[*syngo.Document, error] {
	return func(yield func(*syngo.Document, error) bool) {
		yield(nil, q.send())
	}
}

// Slice answers the query as a slice.
func (q *Query) Slice(ctx context.Context) ([]*syngo.Document, error) { return nil, q.send() }

// First answers with the first document of the answer, or syngo.ErrNotFound when the answer is
// empty.
func (q *Query) First(ctx context.Context) (*syngo.Document, error) { return nil, q.send() }

// IDs answers with the identifiers of the documents, in the order of the answer.
func (q *Query) IDs(ctx context.Context) ([]string, error) { return nil, q.send() }

// Count is the number of documents in the answer.
func (q *Query) Count(ctx context.Context) (int64, error) { return 0, q.send() }

// Page answers with the next size documents of the answer, and the token to go on from there.
func (q *Query) Page(ctx context.Context, size int) (*syngo.Page, error) {
	if size <= 0 {
		return nil, &syngo.QueryError{Reason: "a page holds at least one document"}
	}
	return nil, q.send()
}

// Delete removes the documents of the answer, and returns how many it removed.
func (q *Query) Delete(ctx context.Context) (int64, error) { return 0, q.send() }

// Explain says how the server would answer the query.
func (q *Query) Explain(ctx context.Context) (string, error) { return "", q.send() }
