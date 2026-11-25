// Package syngodrv is the Go driver of SyngoDB Server: a database behind a server, used the way
// an embedded one is.
//
//	db, err := syngodrv.Open(ctx, address)
//	defer db.Close()
//
//	users := db.Collection("users")
//	q := users.Where(syngo.Eq("age", 30)).OrderBy(syngo.Asc("name"))
//	if errors.Is(err, syngo.ErrNotFound) { ... }
//
// What a program writes with (conditions, documents, errors) is the public package
// github.com/syngodb/syngo, and DB, Collection, Query and Tx keep the contracts that package
// states, which the compiler checks. A program therefore reads the same as one that embeds its
// database, but for the line that opens.
//
// A SKELETON, for now. This is Phase 1 of the Server 7 plan: the types and their methods are
// here so that the public package is shaped against its second implementer before it is frozen.
// A query is described and refined for real; whatever would speak to a server returns
// ErrNotBuilt. The protocol the driver will speak is in doc/protocol.md and wire/syngo.proto.
package syngodrv
