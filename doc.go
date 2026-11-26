// Package syngodrv is the Go driver of SyngoDB Server: a database behind a server, used the way
// an embedded one is.
//
//	db, err := syngodrv.Open(ctx, "syngodbs://db.example.com/orders", syngodrv.Login(user, password))
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
// A program that uses several databases of one server makes a Client, and takes its databases
// from it: they share one pool of connections.
//
//	client, err := syngodrv.Connect(ctx, "syngodbs://db.example.com", syngodrv.Login(user, password))
//	orders := client.Database("orders")
//
// How the driver and the server speak is in doc/protocol.md and wire/syngo.proto.
package syngodrv
