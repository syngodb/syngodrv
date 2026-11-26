package syngodrv

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sort"
	"strings"

	"github.com/syngodb/syngo"
	"github.com/syngodb/syngodrv/wire"
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
	// skipped says an offset was given here, even of zero, which replaces the one the text
	// states.
	offset  int
	skipped bool
	after   string
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
	next.offset, next.skipped = max(n, 0), true
	return &next
}

// After resumes the answer after the place a token stands for: the Next of the page before.
func (q *Query) After(token string) *Query {
	next := *q
	next.after = token
	return &next
}

// described is the query as it travels, asked for one kind of answer. What cannot be a query is
// refused here, before anything is sent, as the embedded library refuses it.
func (q *Query) described(answer wire.Answer) (*wire.Query, error) {
	m := &wire.Query{Collection: q.c.name, HasOffset: q.skipped, Offset: uint64(q.offset), After: q.after, Answer: answer}
	if q.asText {
		m.HasText, m.Text = true, q.text
		// Of two values given for one parameter, the one given first counts, as embedded.
		seen := map[string]bool{}
		for _, args := range q.args {
			for name, v := range args {
				if seen[name] {
					continue
				}
				seen[name] = true
				arg := wire.Arg{Name: name}
				if list, ok := syngo.PlainList(v); ok {
					arg.List = true
					unasked := false
					for i, element := range list {
						text, err := syngo.PlainValue(element)
						if err != nil {
							if unasked = !names(q.text, name); unasked {
								break
							}
							return nil, &syngo.QueryError{Reason: fmt.Sprintf("the value given for :%s, element %d: %v", name, i, err)}
						}
						arg.Values = append(arg.Values, text)
					}
					if unasked {
						continue
					}
				} else {
					text, err := syngo.PlainValue(v)
					if err != nil {
						if !names(q.text, name) {
							// A value the text never asks for is not looked at, as embedded.
							continue
						}
						return nil, &syngo.QueryError{Reason: fmt.Sprintf("the value given for :%s: %v", name, err)}
					}
					arg.Values = [][]byte{text}
				}
				m.Args = append(m.Args, arg)
			}
		}
		sort.Slice(m.Args, func(i, j int) bool { return m.Args[i].Name < m.Args[j].Name })
	} else {
		if err := q.where.Err(); err != nil {
			return nil, err
		}
		m.HasWhere = true
		condOf(&m.Where, q.where)
	}
	if q.ordered {
		m.HasOrder = true
		for _, o := range q.order {
			m.Order = append(m.Order, wire.Order{Field: o.Field(), Descending: o.Descending(), Relevance: o.ByRelevance()})
		}
	}
	switch {
	case q.limited:
		m.HasLimit, m.Limit = true, uint64(q.limit)
	case q.unlimited:
		m.Unlimited = true
	}
	return m, nil
}

// condOf writes a condition as it travels. No condition is combined deeper than a message may
// be: the public package makes none (syngo.MaxCondDepth, which is wire.CondDepth).
func condOf(into *wire.Cond, c syngo.Cond) {
	*into = wire.Cond{Op: uint8(c.Op()), Negated: c.Negated(), Field: c.Field(), Values: c.Values(), Language: c.Language()}
	if conds := c.Conds(); len(conds) > 0 {
		into.Conds = make([]wire.Cond, len(conds))
		for i := range conds {
			condOf(&into.Conds[i], conds[i])
		}
	}
}

// ask sends the query for an answer that comes whole in one message.
func (q *Query) ask(ctx context.Context, answer wire.Answer, change bool, size int, read func(resp *wire.Response)) error {
	m, err := q.described(answer)
	if err != nil {
		return err
	}
	m.PageSize = uint32(size)
	return q.c.call(ctx, !change, func(req *wire.Request) { req.Op, req.Query = wire.OpQuery, *m }, read)
}

// errTooManyAnswers is returned for one answer too many pulled at once inside a transaction.
var errTooManyAnswers = errors.New("syngodrv: too many answers are being read at once in one transaction")

// pulled is an answer that is being pulled from the server, a batch at a time. Outside a
// transaction it keeps a connection of the pool to itself until it ends; inside one it is one
// of the things the transaction's connection is busy with, and the transaction goes on with
// others between two batches.
type pulled struct {
	q      *Query
	cn     *conn
	cursor uint64
	// own says the connection was taken from the pool for this answer.
	own bool
}

// open sends the query and returns its first batch, made into values of the program's own.
func (q *Query) open(ctx context.Context, ids bool) (p *pulled, docs []*syngo.Document, names []string, err error) {
	answer := wire.AnswerDocuments
	if ids {
		answer = wire.AnswerIDs
	}
	m, err := q.described(answer)
	if err != nil {
		return nil, nil, nil, err
	}
	p = &pulled{q: q}
	tx := q.c.tx
	if tx != nil {
		if tx.cn == nil {
			return nil, nil, nil, syngo.ErrTxDone
		}
		if tx.cursors >= int(tx.cn.welcome.MaxCursors) {
			return nil, nil, nil, fmt.Errorf("%w: the server allows %d", errTooManyAnswers, tx.cn.welcome.MaxCursors)
		}
		p.cn = tx.cn
	} else {
		client := q.c.db.client
		if p.cn, err = client.take(ctx); err != nil {
			return nil, nil, nil, err
		}
		p.own = true
	}
	send := func() error {
		req := p.cn.begin(q.c.db.name)
		req.Op, req.Query = wire.OpQuery, *m
		return p.cn.exchange(ctx)
	}
	err = send()
	if err != nil && p.own && !p.cn.fresh && (errors.Is(err, errUnsent) || (errors.Is(err, errUnanswered) && ctx.Err() == nil)) {
		// A connection that died in the pool: a query changes nothing, and is asked again.
		if p.cn, err = q.c.db.client.again(ctx, p.cn); err != nil {
			return nil, nil, nil, err
		}
		err = send()
	}
	if err != nil {
		p.fail()
		return nil, nil, nil, lost(err, true)
	}
	docs, names = p.batch()
	return p, docs, names, nil
}

// batch makes the batch that was just received into values of the program's own, and notes
// whether the answer goes on. The memory of the connection is free again when it returns.
func (p *pulled) batch() (docs []*syngo.Document, names []string) {
	b := &p.cn.resp.Batch
	if len(b.Documents) > 0 {
		docs = make([]*syngo.Document, len(b.Documents))
		for i := range b.Documents {
			docs[i] = documentOf(&b.Documents[i])
		}
	}
	if len(b.IDs) > 0 {
		names = append([]string(nil), b.IDs...)
	}
	was := p.cursor
	p.cursor = b.Cursor
	if tx := p.q.c.tx; tx != nil {
		switch {
		case was == 0 && p.cursor != 0:
			tx.cursors++
		case was != 0 && p.cursor == 0:
			tx.cursors--
		}
	}
	if p.cursor == 0 {
		p.done()
	}
	return docs, names
}

// next pulls the next batch.
func (p *pulled) next(ctx context.Context) (docs []*syngo.Document, names []string, err error) {
	if tx := p.q.c.tx; tx != nil && tx.cn != p.cn {
		// The transaction ended inside the loop that reads its answer.
		p.cursor = 0
		return nil, nil, syngo.ErrTxDone
	}
	req := p.cn.begin(p.q.c.db.name)
	req.Op, req.Cursor = wire.OpNext, p.cursor
	if err := p.cn.exchange(ctx); err != nil {
		p.fail()
		return nil, nil, lost(err, true)
	}
	docs, names = p.batch()
	return docs, names, nil
}

// close gives up an answer that was not read to its end.
func (p *pulled) close() {
	if p.cursor == 0 {
		return
	}
	tx := p.q.c.tx
	if tx != nil && tx.cn != p.cn {
		p.cursor = 0
		return
	}
	req := p.cn.begin(p.q.c.db.name)
	req.Op, req.Cursor = wire.OpCloseCursor, p.cursor
	if p.cn.exchange(context.Background()) != nil {
		p.cn.broken = true
	}
	p.cursor = 0
	if tx != nil {
		tx.cursors--
		if p.cn.broken {
			tx.release()
		}
	}
	p.done()
}

// fail ends an answer whose request failed.
func (p *pulled) fail() {
	tx := p.q.c.tx
	if tx != nil {
		if p.cursor != 0 {
			tx.cursors--
		}
		if p.cn.broken && tx.cn == p.cn {
			tx.release()
		}
	}
	p.cursor = 0
	p.done()
}

// done gives the connection of the answer back, when the answer had one to itself.
func (p *pulled) done() {
	if p.own && p.cn != nil {
		p.q.c.db.client.give(p.cn)
		p.cn, p.own = nil, false
	}
}

// All answers the query as a sequence: for doc, err := range q.All(ctx). The answer is pulled
// from the server a batch at a time, as far as the loop goes, and is the database at one moment
// however long the loop takes. The sequence ends after the first error.
func (q *Query) All(ctx context.Context) iter.Seq2[*syngo.Document, error] {
	return func(yield func(*syngo.Document, error) bool) {
		p, docs, _, err := q.open(ctx, false)
		if err != nil {
			yield(nil, err)
			return
		}
		defer p.close()
		for {
			for _, doc := range docs {
				if !yield(doc, nil) {
					return
				}
			}
			if p.cursor == 0 {
				return
			}
			if docs, _, err = p.next(ctx); err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

// Slice answers the query as a slice.
func (q *Query) Slice(ctx context.Context) ([]*syngo.Document, error) {
	p, docs, _, err := q.open(ctx, false)
	if err != nil {
		return nil, err
	}
	defer p.close()
	out := docs
	for p.cursor != 0 {
		if docs, _, err = p.next(ctx); err != nil {
			return nil, err
		}
		out = append(out, docs...)
	}
	return out, nil
}

// First answers with the first document of the answer, or syngo.ErrNotFound when the answer is
// empty.
func (q *Query) First(ctx context.Context) (doc *syngo.Document, err error) {
	// Which document is first is for the server to say: a text may limit its answer to no
	// document, and then there is none.
	err = q.ask(ctx, wire.AnswerFirst, false, 0, func(resp *wire.Response) { doc = documentOf(&resp.Document) })
	return doc, err
}

// names says whether the text of a query asks for the parameter of that name: a colon, the
// name in any case, and nothing after it that could go on being a name. It is asked only of a
// value that cannot be made plain, to know whether that matters. (A text that holds the same
// characters inside a quoted string is taken to ask for it: such a value is then refused here,
// where embedded it would not be looked at.)
func names(text, name string) bool {
	for at := 0; at+1+len(name) <= len(text); at++ {
		if text[at] != ':' || !strings.EqualFold(text[at+1:at+1+len(name)], name) {
			continue
		}
		rest := text[at+1+len(name):]
		if rest == "" {
			return true
		}
		if c := rest[0]; !(c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80) {
			return true
		}
	}
	return false
}

// IDs answers with the identifiers of the documents, in the order of the answer.
func (q *Query) IDs(ctx context.Context) ([]string, error) {
	p, _, ids, err := q.open(ctx, true)
	if err != nil {
		return nil, err
	}
	defer p.close()
	out := ids
	for p.cursor != 0 {
		if _, ids, err = p.next(ctx); err != nil {
			return nil, err
		}
		out = append(out, ids...)
	}
	return out, nil
}

// Count is the number of documents in the answer.
func (q *Query) Count(ctx context.Context) (n int64, err error) {
	err = q.ask(ctx, wire.AnswerCount, false, 0, func(resp *wire.Response) { n = resp.Number })
	return n, err
}

// Page answers with the next size documents of the answer, and the token to go on from there.
func (q *Query) Page(ctx context.Context, size int) (page *syngo.Page, err error) {
	if size <= 0 {
		return nil, &syngo.QueryError{Reason: "a page holds at least one document"}
	}
	err = q.ask(ctx, wire.AnswerPage, false, size, func(resp *wire.Response) {
		page = &syngo.Page{Next: resp.Batch.NextPage}
		for i := range resp.Batch.Documents {
			page.Items = append(page.Items, documentOf(&resp.Batch.Documents[i]))
		}
	})
	return page, err
}

// Delete removes the documents of the answer, and returns how many it removed.
func (q *Query) Delete(ctx context.Context) (n int64, err error) {
	err = q.ask(ctx, wire.AnswerDelete, true, 0, func(resp *wire.Response) { n = resp.Number })
	return n, err
}

// Explain says how the server would answer the query.
func (q *Query) Explain(ctx context.Context) (text string, err error) {
	err = q.ask(ctx, wire.AnswerExplain, false, 0, func(resp *wire.Response) { text = resp.Text })
	return text, err
}
