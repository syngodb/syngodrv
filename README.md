# syngodrv

Go driver (client) for SyngoDB Server.

A database behind a server, used the way an embedded one is. What a program writes with
(conditions, documents, errors) is the public package
[`syngo`](https://github.com/syngodb/syngo), and the driver keeps the contracts that package
states.

```go
client, err := syngodrv.Connect(ctx, "syngodbs://db.example.com", syngodrv.Login(user, password))
orders := client.Database("orders").Collection("orders")

// A program with one database: the same in one line.
db, err := syngodrv.Open(ctx, "syngodbs://db.example.com/orders", syngodrv.Login(user, password))
```

**Not released yet.** The driver speaks to SyngoDB Server 7, which is being built with it, and
nothing here is promised before both are released. It depends on the public package and on
nothing else.

| | |
| --- | --- |
| [doc/protocol.md](doc/protocol.md) | the protocol the driver and the server speak |
| [wire/syngo.proto](wire/syngo.proto) | its messages |
