package syngodrv

import (
	"context"
	"errors"
	"testing"

	"github.com/syngodb/syngo"
)

// Code written against the contracts takes the driver's types, with nothing said about them:
// the compiler finds the type parameters by itself.
func ask[D syngo.DB[C, T], C syngo.Collection[Q], Q syngo.Query[Q], T syngo.Tx[C]](ctx context.Context, db D) (int64, error) {
	return db.Collection("users").Where(syngo.Eq("status", "active")).OrderBy(syngo.Desc("age")).Limit(10).Count(ctx)
}

func TestTheSkeletonKeepsTheContracts(t *testing.T) {
	ctx := context.Background()
	db := &DB{}
	if _, err := ask(ctx, db); !errors.Is(err, ErrNotBuilt) {
		t.Errorf("got %v", err)
	}
	if _, err := Open(ctx, "anywhere"); !errors.Is(err, ErrNotBuilt) {
		t.Errorf("Open: %v", err)
	}
	if _, err := Update(ctx, db, func(tx *Tx) (int, error) { return 1, nil }); !errors.Is(err, ErrNotBuilt) {
		t.Errorf("Update: %v", err)
	}
}

// What can be refused before anything is sent is refused as the embedded library refuses it.
func TestRefusedBeforeAnythingIsSent(t *testing.T) {
	ctx := context.Background()
	c := (&DB{}).Collection("users")
	if _, err := c.Where(syngo.Eq("name", "\xff")).Count(ctx); !errors.Is(err, syngo.ErrInvalidQuery) {
		t.Errorf("a condition that cannot be one: %v", err)
	}
	for doc, err := range c.Where(syngo.In("age", make(chan int))).All(ctx) {
		if doc != nil || !errors.Is(err, syngo.ErrInvalidQuery) {
			t.Errorf("a condition that cannot be one, in a loop: %v, %v", doc, err)
		}
	}
	if _, err := c.Put(ctx, nil); !errors.Is(err, syngo.ErrInvalidDocument) {
		t.Errorf("no document: %v", err)
	}
	if _, err := c.Insert(ctx, make(chan int)); !errors.Is(err, syngo.ErrInvalidDocument) {
		t.Errorf("a document that cannot be encoded: %v", err)
	}
	if _, err := c.Put(ctx, `{"_id":"a"}`, syngo.Meta(func() {})); !errors.Is(err, syngo.ErrInvalidDocument) {
		t.Errorf("metadata that cannot be encoded: %v", err)
	}
	if _, err := c.Where().Page(ctx, 0); !errors.Is(err, syngo.ErrInvalidQuery) {
		t.Errorf("a page of nothing: %v", err)
	}
}

// Refining a query makes a new one and leaves the first as it was.
func TestRefiningLeavesTheQueryAsItWas(t *testing.T) {
	c := (&DB{}).Collection("users")
	q := c.Where(syngo.Eq("a", 1))
	order := []syngo.Order{syngo.Asc("a"), syngo.Desc("b")}
	ordered := q.OrderBy(order...)
	order[0] = syngo.Desc("z")
	limited := ordered.Limit(5).Offset(2).After("token")
	if q.ordered || q.limited || q.offset != 0 || q.after != "" {
		t.Errorf("the first query changed: %+v", q)
	}
	if len(ordered.order) != 2 || ordered.order[0].Field() != "a" || ordered.limited {
		t.Errorf("the ordered query: %+v", ordered)
	}
	if !limited.limited || limited.limit != 5 || limited.offset != 2 || limited.after != "token" || len(limited.order) != 2 {
		t.Errorf("the refined query: %+v", limited)
	}
	if free := limited.Limit(-1); free.limited || !free.unlimited || !limited.limited {
		t.Errorf("a limit taken away: %+v", free)
	}
	args := []syngo.Args{{"min": 18}}
	text := c.Query("age >= :min", args...)
	args[0] = syngo.Args{"min": 99}
	if !text.asText || text.args[0]["min"] != 18 {
		t.Errorf("a query written as text followed the arguments it was made from: %+v", text.args)
	}
}
