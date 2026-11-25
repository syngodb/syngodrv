package syngodrv

import (
	"context"

	"github.com/syngodb/syngo"
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

// Get returns the document with the identifier, or syngo.ErrNotFound.
func (c *Collection) Get(ctx context.Context, id string) (*syngo.Document, error) {
	return nil, ErrNotBuilt
}

// Exists says whether a document has the identifier.
func (c *Collection) Exists(ctx context.Context, id string) (bool, error) { return false, ErrNotBuilt }

// Count is the number of documents in the collection, exactly.
func (c *Collection) Count(ctx context.Context) (int64, error) { return 0, ErrNotBuilt }

// store is what Put, Insert and Replace have in common. What the program handed in is made
// plain by the public package, as the embedded library has it made: the text of the document,
// and what the options come to.
func (c *Collection) store(ctx context.Context, doc any, options []syngo.WriteOption) (string, error) {
	if _, err := syngo.PlainDocument(doc); err != nil {
		return "", err
	}
	if _, err := syngo.PlainWrite(options); err != nil {
		return "", err
	}
	return "", ErrNotBuilt
}

// Put stores a document, replacing the one that has its identifier if there is one, and returns
// the identifier.
func (c *Collection) Put(ctx context.Context, doc any, options ...syngo.WriteOption) (id string, err error) {
	return c.store(ctx, doc, options)
}

// Insert stores a new document and returns its identifier, or syngo.ErrExists.
func (c *Collection) Insert(ctx context.Context, doc any, options ...syngo.WriteOption) (id string, err error) {
	return c.store(ctx, doc, options)
}

// Replace stores a document in the place of the one that has its identifier, or returns
// syngo.ErrNotFound.
func (c *Collection) Replace(ctx context.Context, doc any, options ...syngo.WriteOption) error {
	_, err := c.store(ctx, doc, options)
	return err
}

// PutMany stores documents, all of them or none, and returns their identifiers in the order
// given.
func (c *Collection) PutMany(ctx context.Context, docs []any, options ...syngo.WriteOption) ([]string, error) {
	for _, doc := range docs {
		if _, err := syngo.PlainDocument(doc); err != nil {
			return nil, err
		}
	}
	return nil, ErrNotBuilt
}

// Patch changes some members of a document and leaves the others as they are.
func (c *Collection) Patch(ctx context.Context, id string, patch any, options ...syngo.WriteOption) error {
	if _, err := syngo.PlainDocument(patch); err != nil {
		return err
	}
	return ErrNotBuilt
}

// Delete removes the document with the identifier.
func (c *Collection) Delete(ctx context.Context, id string) error { return ErrNotBuilt }

// EnsureIndex makes sure the collection has the index, and returns once the index is ready.
func (c *Collection) EnsureIndex(ctx context.Context, def syngo.Index) error { return ErrNotBuilt }

// RebuildIndex builds an index again from the documents.
func (c *Collection) RebuildIndex(ctx context.Context, name string) error { return ErrNotBuilt }

// DropIndex removes an index.
func (c *Collection) DropIndex(ctx context.Context, name string) error { return ErrNotBuilt }

// Indexes lists the collection's indexes, by name.
func (c *Collection) Indexes(ctx context.Context) ([]syngo.Index, error) { return nil, ErrNotBuilt }
