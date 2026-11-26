package syngodrv

import (
	"bytes"
	"context"
	"unsafe"

	"github.com/syngodb/syngo"
	"github.com/syngodb/syngodrv/wire"
)

// Collection is a handle on one collection of a database. A handle costs nothing to make and may
// be kept, copied and shared between goroutines.
type Collection struct {
	db   *DB
	name string
	// tx is the transaction the handle works in, or nil.
	tx *Tx
}

// Name is the collection's name.
func (c *Collection) Name() string { return c.name }

// call sends a request about the collection: on the connection of its transaction when it has
// one, on a connection of the pool otherwise. safe says the request changes nothing.
func (c *Collection) call(ctx context.Context, safe bool, fill func(req *wire.Request), read func(resp *wire.Response)) error {
	if c.tx != nil {
		return c.tx.call(ctx, fill, read)
	}
	return c.db.client.call(ctx, c.db.name, safe, fill, read)
}

// documentOf is a document of the public package, made from one that was received. What was
// received lies in the memory of the connection: the document has its own copy.
func documentOf(d *wire.Document) *syngo.Document {
	info := syngo.DocumentInfo{Revision: d.Revision, Modified: d.Modified, Expires: d.Expires}
	if len(d.Meta) > 0 {
		info.Meta = bytes.Clone(d.Meta)
	}
	// The identifier and the text are copied into one piece of memory, which is the document's
	// and is never written again: the identifier is a string over its first bytes.
	own := make([]byte, len(d.ID)+len(d.JSON))
	n := copy(own, d.ID)
	copy(own[n:], d.JSON)
	return syngo.NewDocument(unsafe.String(unsafe.SliceData(own), n), own[n:len(own):len(own)], info)
}

// writeOf is what the options of a write come to, as it travels.
func writeOf(w *syngo.Write) (out wire.Write) {
	if w.Timed {
		switch {
		case !w.ExpireAt.IsZero():
			out.Expiry, out.ExpireAt = wire.ExpiryAt, wire.Instant{Seconds: w.ExpireAt.Unix(), Nanos: int32(w.ExpireAt.Nanosecond())}
		case w.ExpireIn != 0:
			out.Expiry, out.ExpireIn = wire.ExpiryIn, int64(w.ExpireIn)
		default:
			out.Expiry = wire.ExpiryNever
		}
	}
	if w.HasMeta {
		if w.Meta == nil {
			out.Meta = wire.MetaNone
		} else {
			out.Meta, out.MetaJSON = wire.MetaSet, w.Meta
		}
	}
	return out
}

// Get returns the document with the identifier, or syngo.ErrNotFound.
func (c *Collection) Get(ctx context.Context, id string) (doc *syngo.Document, err error) {
	err = c.call(ctx, true, func(req *wire.Request) {
		req.Op, req.Get = wire.OpGet, wire.Get{Collection: c.name, ID: id}
	}, func(resp *wire.Response) {
		doc = documentOf(&resp.Document)
	})
	return doc, err
}

// Exists says whether a document has the identifier.
func (c *Collection) Exists(ctx context.Context, id string) (found bool, err error) {
	err = c.call(ctx, true, func(req *wire.Request) {
		req.Op, req.Exists = wire.OpExists, wire.Exists{Collection: c.name, ID: id}
	}, func(resp *wire.Response) {
		found = resp.Found
	})
	return found, err
}

// Count is the number of documents in the collection, exactly.
func (c *Collection) Count(ctx context.Context) (n int64, err error) {
	err = c.call(ctx, true, func(req *wire.Request) {
		req.Op, req.Count = wire.OpCount, wire.Count{Collection: c.name}
	}, func(resp *wire.Response) {
		n = resp.Number
	})
	return n, err
}

// store is what Put, Insert and Replace have in common. What the program handed in is made
// plain by the public package, as the embedded library has it made: the text of the document,
// and what the options come to.
func (c *Collection) store(ctx context.Context, mode uint8, doc any, options []syngo.WriteOption) (id string, err error) {
	text, err := syngo.PlainDocument(doc)
	if err != nil {
		return "", err
	}
	stated, err := syngo.PlainWrite(options)
	if err != nil {
		return "", err
	}
	err = c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.Put = wire.OpPut, wire.Put{Collection: c.name, Mode: mode, Document: text, Options: writeOf(&stated)}
	}, func(resp *wire.Response) {
		if len(resp.Stored) == 1 {
			id = resp.Stored[0]
		}
	})
	return id, err
}

// Put stores a document, replacing the one that has its identifier if there is one, and returns
// the identifier.
func (c *Collection) Put(ctx context.Context, doc any, options ...syngo.WriteOption) (id string, err error) {
	return c.store(ctx, wire.WritePut, doc, options)
}

// Insert stores a new document and returns its identifier, or syngo.ErrExists.
func (c *Collection) Insert(ctx context.Context, doc any, options ...syngo.WriteOption) (id string, err error) {
	return c.store(ctx, wire.WriteInsert, doc, options)
}

// Replace stores a document in the place of the one that has its identifier, or returns
// syngo.ErrNotFound.
func (c *Collection) Replace(ctx context.Context, doc any, options ...syngo.WriteOption) error {
	_, err := c.store(ctx, wire.WriteReplace, doc, options)
	return err
}

// PutMany stores documents, all of them or none, and returns their identifiers in the order
// given. A set too large for one message travels in several, inside one transaction.
func (c *Collection) PutMany(ctx context.Context, docs []any, options ...syngo.WriteOption) ([]string, error) {
	texts := make([][]byte, len(docs))
	for i, doc := range docs {
		text, err := syngo.PlainDocument(doc)
		if err != nil {
			return nil, err
		}
		texts[i] = text
	}
	stated, err := syngo.PlainWrite(options)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(docs))
	send := func(in *Collection, part [][]byte) error {
		return in.call(ctx, false, func(req *wire.Request) {
			req.Op, req.PutBatch = wire.OpPutBatch, wire.PutBatch{Collection: c.name, Documents: part, Options: writeOf(&stated)}
		}, func(resp *wire.Response) {
			ids = append(ids, resp.Stored...)
		})
	}
	parts := split(texts, c.db.client.room())
	switch {
	case len(parts) == 1:
		err = send(c, texts)
	case c.tx != nil:
		for _, part := range parts {
			if err = send(c, part); err != nil {
				break
			}
		}
	default:
		err = c.db.Update(ctx, func(tx *Tx) error {
			ids = ids[:0]
			for _, part := range parts {
				if err := send(tx.Collection(c.name), part); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// split cuts a set of documents into parts that each fit in room bytes, and whose answer does:
// the identifiers come back in one message too, and a document that is given one takes more
// there than it took on its way. A document larger than the room is a part by itself: the
// server says what it thinks of it.
func split(texts [][]byte, room int) (parts [][][]byte) {
	from, size := 0, 0
	for i, text := range texts {
		takes := max(len(text), 40) + 8
		if size+takes > room && i > from {
			parts = append(parts, texts[from:i])
			from, size = i, 0
		}
		size += takes
	}
	return append(parts, texts[from:])
}

// Patch changes some members of a document and leaves the others as they are.
func (c *Collection) Patch(ctx context.Context, id string, patch any, options ...syngo.WriteOption) error {
	text, err := syngo.PlainDocument(patch)
	if err != nil {
		return err
	}
	stated, err := syngo.PlainWrite(options)
	if err != nil {
		return err
	}
	return c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.Patch = wire.OpPatch, wire.Patch{Collection: c.name, ID: id, Patch: text, Options: writeOf(&stated)}
	}, nil)
}

// Delete removes the document with the identifier.
func (c *Collection) Delete(ctx context.Context, id string) error {
	return c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.Delete = wire.OpDelete, wire.Delete{Collection: c.name, ID: id}
	}, nil)
}

// EnsureIndex makes sure the collection has the index, and returns once the index is ready.
func (c *Collection) EnsureIndex(ctx context.Context, def syngo.Index) error {
	return c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.EnsureIndex = wire.OpEnsureIndex, wire.EnsureIndex{Collection: c.name, Index: wire.Index{Name: def.Name, Fields: def.Fields, Unique: def.Unique}}
	}, nil)
}

// RebuildIndex builds an index again from the documents.
func (c *Collection) RebuildIndex(ctx context.Context, name string) error {
	return c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.Named = wire.OpRebuildIndex, wire.IndexRef{Collection: c.name, Name: name}
	}, nil)
}

// DropIndex removes an index.
func (c *Collection) DropIndex(ctx context.Context, name string) error {
	return c.call(ctx, false, func(req *wire.Request) {
		req.Op, req.Named = wire.OpDropIndex, wire.IndexRef{Collection: c.name, Name: name}
	}, nil)
}

// Indexes lists the collection's indexes, by name.
func (c *Collection) Indexes(ctx context.Context) (list []syngo.Index, err error) {
	err = c.call(ctx, true, func(req *wire.Request) {
		req.Op, req.Indexes = wire.OpIndexes, c.name
	}, func(resp *wire.Response) {
		for _, ix := range resp.Indexes {
			list = append(list, syngo.Index{Name: ix.Name, Fields: append([]string(nil), ix.Fields...), Unique: ix.Unique})
		}
	})
	return list, err
}
