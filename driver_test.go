package syngodrv

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/syngodb/syngo"
)

// Code written against the contracts takes the driver's types, with nothing said about them:
// the compiler finds the type parameters by itself.
func ask[D syngo.DB[C, T], C syngo.Collection[Q], Q syngo.Query[Q], T syngo.Tx[C]](ctx context.Context, db D) (int64, error) {
	return db.Collection("users").Where(syngo.Eq("status", "active")).OrderBy(syngo.Desc("age")).Limit(10).Count(ctx)
}

// An address says where the server is, and never who logs in.
func TestAddresses(t *testing.T) {
	for text, want := range map[string]address{
		"syngodbs://db.example.com":                 {network: "tcp", address: "db.example.com:31415", host: "db.example.com", secure: true},
		"syngodbs://127.0.0.1:41415":                {network: "tcp", address: "127.0.0.1:41415", host: "127.0.0.1", secure: true},
		"syngodbs://127.0.0.1:41415?insecure=true":  {network: "tcp", address: "127.0.0.1:41415", host: "127.0.0.1", secure: true, insecure: true},
		"syngodbs://127.0.0.1:41415?insecure=false": {network: "tcp", address: "127.0.0.1:41415", host: "127.0.0.1", secure: true},
		"syngodbs://db/SS_default?insecure=true":    {network: "tcp", address: "db:31415", host: "db", database: "SS_default", secure: true, insecure: true},
		"syngodb://127.0.0.1:41415":                 {network: "tcp", address: "127.0.0.1:41415", host: "127.0.0.1"},
		"syngodb://[::1]:41415/orders":              {network: "tcp", address: "[::1]:41415", host: "::1", database: "orders"},
		"syngodbs://db.example.com/SS_default":      {network: "tcp", address: "db.example.com:31415", host: "db.example.com", database: "SS_default", secure: true},
		"syngodbs://db/a%20name%20with%20spaces":    {network: "tcp", address: "db:31415", host: "db", database: "a name with spaces", secure: true},
		"syngodb+unix:///run/syngod.sock":           {network: "unix", address: socketPath("/run/syngod.sock", runtime.GOOS == "windows")},
		"syngodb+unix:///run/s.sock?database=main":  {network: "unix", address: socketPath("/run/s.sock", runtime.GOOS == "windows"), database: "main"},
	} {
		if got, err := parse(text); err != nil || got != want {
			t.Errorf("%s: %+v, %v", text, got, err)
		}
	}
	for _, text := range []string{
		"", "db.example.com", "http://db.example.com", "syngo://db.example.com", "syngodbs://", "syngodb:///orders",
		"syngodbs://user:password@db.example.com/orders", "syngodb://user@db.example.com", "syngodbs://db.example.com/orders?tls=off",
		"syngodbs://db.example.com?insecure=yes", "syngodbs://db.example.com?insecure=true&insecure=true", "syngodb://db.example.com?insecure=true",
		"syngodb+unix://host/run/syngod.sock", "syngodb+unix://", "syngodb+unix:///run/s.sock?other=1", "syngodb+unix:///run/s.sock?database=a&b=c",
		"syngodb+unix:///run/s.sock?insecure=true",
	} {
		if got, err := parse(text); err == nil {
			t.Errorf("%q is taken for the address %+v", text, got)
		}
	}
	// The path of a socket, as each system takes it. On Windows it is written as in the address
	// of any file, and the path the system is given has its drive first and its own strokes.
	for _, c := range []struct {
		path    string
		windows bool
		want    string
	}{
		{"/run/syngod.sock", false, "/run/syngod.sock"},
		{"/C:/odd/name.sock", false, "/C:/odd/name.sock"},
		{"/C:/ProgramData/Syncplify/syngod.sock", true, `C:\ProgramData\Syncplify\syngod.sock`},
		{"/d:/data/s.sock", true, `d:\data\s.sock`},
		{"/run/s.sock", true, `\run\s.sock`},
		{"/1:/s.sock", true, `\1:\s.sock`},
	} {
		if got := socketPath(c.path, c.windows); got != c.want {
			t.Errorf("the socket of the path %q, on Windows %v: %q", c.path, c.windows, got)
		}
	}
	if got, err := parse("syngodb+unix:///C:/data/s.sock?database=main"); err != nil || got.address != socketPath("/C:/data/s.sock", runtime.GOOS == "windows") || got.database != "main" {
		t.Errorf("an address with a drive: %+v, %v", got, err)
	}
	// A password that was put in an address is not repeated by the error that refuses it.
	if _, err := parse("syngodbs://root:the-secret@db.example.com"); err == nil || strings.Contains(err.Error(), "the-secret") {
		t.Errorf("the error for an address with a password: %v", err)
	}
	ctx := context.Background()
	if _, err := Connect(ctx, "syngodbs://db.example.com/orders"); err == nil {
		t.Error("Connect took an address that names a database")
	}
	if _, err := Open(ctx, "syngodbs://db.example.com"); err == nil {
		t.Error("Open took an address that names no database")
	}
}

// Code written against the contracts takes the driver's types: a server that is not there is
// said to be so, by the error of this package for it.
func TestTheDriverKeepsTheContracts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// A port of this machine on which nothing listens.
	if _, err := Connect(ctx, "syngodb://127.0.0.1:1"); !errors.Is(err, ErrConnection) {
		t.Errorf("a server that is not there: %v", err)
	}
	var db *DB
	_ = func() (int64, error) { return ask(ctx, db) }
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
