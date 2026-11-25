# syngodrv

Go driver (client) for SyngoDB Server.

A database behind a server, used the way an embedded one is. What a program writes with
(conditions, documents, errors) is the public package
[`syngo`](https://github.com/syngodb/syngo), and the driver keeps the contracts that package
states.

**A skeleton, for now.** The types and their methods are here, and the compiler holds them to
the contracts. Nothing speaks to a server yet.

| | |
| --- | --- |
| [doc/protocol.md](doc/protocol.md) | the protocol the driver and the server will speak, as a draft |
| [wire/syngo.proto](wire/syngo.proto) | its messages |
