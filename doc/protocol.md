# The SyngoDB Server protocol, version 1

**A draft that is spoken.** Written on paper in Phase 1, and since Phase 3 what this driver and
SyngoDB Server speak. It is not promised before a public release: what the next phases teach
may still change this page. The messages are defined in [`wire/syngo.proto`](../wire/syngo.proto),
and the driver's package `wire` writes and reads them.

It is our own protocol. The measurements that decided it are in the server's plan: a read costs
10 µs through it and 46 µs through gRPC.

## In one page

- A client opens a connection, says hello, and logs in once. The connection is then a
  **session** with the server, and every request names the database it is for: a program that
  uses several databases of one server needs one pool of connections, not one for each.
- A session carries **one request at a time**: a request, then its answer. A driver that wants
  more at once keeps more connections (a pool).
- A **transaction lives on its session**: from `Begin` to `Commit`, everything the session sends
  is part of it, in the database its `Begin` named.
- An answer too large for one message is **pulled**, batch by batch. Between two batches the
  session can send something else, so a program can write inside the loop that reads a query.
- A condition travels **as a description**, a query text **as text with its arguments beside
  it**. Nothing a program gives is ever pasted into a text.
- Every value (an operand, an argument, a document) is **JSON text**. The server checks it.
- An error has a **code** a program can tell apart, which the driver turns back into the very
  error the embedded library returns.

## Transport

| | |
| --- | --- |
| TCP | with TLS. Without TLS only on the loopback address, unless the server is told otherwise |
| Unix socket | optional, without TLS: the file's permissions say who may connect |

A client certificate, when the server accepts or requires one, is shown when the connection is
made. It is then the identity of the session, and `Hello` carries no password.

A TLS connection asks for the protocol by its name, `syngo` (ALPN). A client of anything else,
the driver of SyngoDB Server 6 among them, is refused while the connection is made, and never
left to wait for an answer that would not come.

## Frames

Everything travels in frames. A frame is a header of 9 bytes and a payload:

| Bytes | What |
| --- | --- |
| 4 | the length of the payload, big endian |
| 1 | the kind of frame, and one flag: "the message goes on in the next frame" |
| 4 | the number of the request the frame belongs to, big endian |

| Kind | From | Payload |
| --- | --- | --- |
| `HELLO` | client | a `Hello` |
| `WELCOME` | server | a `Welcome` |
| `REQUEST` | client | a `Request` |
| `RESPONSE` | server | a `Response` |
| `CANCEL` | client | none: the request of that number is to stop |

- **A message larger than a frame** is cut into several frames of the same kind and number, all
  but the last with the flag set. A frame is at most 1 MiB (64 KiB before the login), so that
  no connection holds more than that for a peer that has said nothing useful yet. A whole
  message is at most what `Welcome` says, which follows the server's limit for a document.
- **Such a message is announced.** Before its frames goes one more, of the same kind and
  number and with the flag set, whose payload is the length of the whole message: 8 bytes, big
  endian. The reader knows what is coming before it has taken any of it: a message longer than
  it accepts is refused there, and a server that has no room for a large request leaves it in
  the network until it has. The frames that follow hold exactly what was announced: more, or
  less, is not a message.
- **Request numbers** are chosen by the client, each greater than the one before. An answer
  carries the number of its request.
- A frame that is not one of these, or that comes out of turn, ends the session.

## The session

1. The client sends `Hello`: the versions of the protocol it speaks, who it is (for the log), and
   a user and password unless its certificate is its identity.
2. The server answers `Welcome`: the version both will speak, the role of the session, and the
   limits the client must keep to (frame, message, idle time, open answers). Or it answers an
   error and closes.
3. Requests and answers follow, one at a time.

- **The role is read at every request**, a `Commit` included. A user whose role changes loses
  what they lost at once, on sessions that are already open. A user who is removed loses the
  session: its next request, whatever it is, is answered `UNAUTHENTICATED`, the session is
  closed, and a transaction it had open is given up.
- **Keep alive.** A session silent for longer than `Welcome` says is closed. `Ping` is a
  request like any other, and is how a pool keeps an idle connection and learns that one died.
  A `CANCEL` frame is not the session speaking, and does not count.
- **A failed login** is answered after a delay that grows, never by locking anyone out. The
  delay is counted for the address the logins come from: 100 ms after one failure, twice as
  long after each more, 5 seconds at most, and forgotten a minute after the last. A right
  password is always let in, after the wait.
- **Passwords are checked a few at a time.** A login that could not be looked at within the
  time a connection has to say who it is (10 seconds) is answered `BUSY`.
- **An answer must be taken.** A client that takes no part of an answer for 30 seconds loses
  the session.

## Requests

| Request | Answer | Embedded call |
| --- | --- | --- |
| `Get` | a document | `Get` |
| `Exists` | yes or no | `Exists` |
| `Count` | a number | `Count` of a collection |
| `Put` with a mode | the identifier | `Put`, `Insert`, `Replace` |
| `PutBatch` | the identifiers | `PutMany` |
| `Patch` | done | `Patch` |
| `Modify` | done | the second half of `Modify` (below) |
| `Delete` | done | `Delete` of a document |
| `Query` | a batch, a number, a page or a text | every way a query answers |
| `Next`, `CloseCursor` | the next batch, done | the loop over an answer |
| `Begin`, `Commit`, `Rollback` | done | the same |
| `EnsureIndex`, `RebuildIndex`, `DropIndex`, `Indexes` | done, the list | the same |
| `Collections`, `DropCollection`, `Stats` | the list, done, the figures | the same |
| `EnsureDatabase`, `DropDatabase`, `Databases` | done, the list | none: an embedded database is a directory |

Every request but the last three names its database. **A database is made on purpose**, with
`EnsureDatabase`: one that is only named is answered `NO_DATABASE`.

What else belongs to the server as a whole (users and identities, backup and restore) comes with
the phases that build it, as more requests of the same shape.

**What a request may be.** Each bound is a setting of the server with a default, and a request
beyond one is answered `TOO_LARGE`, with nothing done and the session kept:

- **What it describes.** The lists of a request (documents, conditions, arguments) take 8 MiB
  at most once they are read, beside the bytes of the message. A message of a few megabytes
  that says "two million conditions" is refused before the memory is taken.
- **What its answer would be.** `PutBatch` answers with an identifier for each document, in
  one message: a batch whose identifiers would not fit is refused before anything is stored.
  This driver cuts what `PutMany` is given into batches that fit, inside one transaction.
- **Conditions** nest 200 deep at most. It is the bound of the public package
  (`syngo.MaxCondDepth`): a program cannot make a condition that cannot travel, and an
  embedded database refuses the same ones.
- **A user who may only read** sends no request longer than those lists and a megabyte (9 MiB
  with the defaults): a longer one is refused when it is announced.

**What a server may make a client wait for, and what it ends.** All of these are settings of
the server, and a client needs to know only what it is told:

- **Room.** The server has a budget of memory for the requests it serves at once. A message
  longer than 64 KiB waits for room before the server reads it, and again before it is served;
  an answer waits before it is made. The client sees a request that takes longer. A request
  that finds no room for as long as a session may stay silent is answered `BUSY`, with nothing
  done: it is sent again later.
- **A rate.** A user who makes more requests a second than it may is answered `BUSY` with the
  time to wait, and nothing is done. This driver waits that long and sends the request again.
- **A full server** answers the hello with `BUSY` and closes the connection.
- **Time.** A message that has begun has 30 seconds for each megabyte, to arrive and to be
  taken: a client that stops in the middle of one loses its session. A transaction that is
  asked nothing for 2 minutes, or is kept for more than an hour, is ended by the server: what
  is asked in it afterwards is answered `TX_EXPIRED`, until the client commits it (which is
  answered so too) or rolls it back. An answer that is not pulled further for 5 minutes is
  given up, and pulling it then is answered `STALE`.
- **What transactions hold.** What a transaction wrote and what it remembers of its reads stay
  in the server's memory until it ends. One transaction has a bound of its own (64 MiB by
  default), past which its next read or write is answered `TOO_LARGE`. What all of them hold
  together, beyond 64 KiB each, is counted in the budget above: a transaction that finds no
  room there is ended by the server, and what is asked in it afterwards is answered
  `TOO_LARGE`, until the client commits it (which is answered so too) or rolls it back. One
  transaction alone is never ended for this. The commit of a transaction that is counted
  waits for room as a long message does, and is answered `BUSY` when it finds none in time:
  nothing was done, the transaction is still open, and the commit is sent again.

## Queries

A `Query` says what is asked and what kind of answer is wanted:

- **What is asked**: conditions (`Cond`, a tree of comparisons exactly as `syngo.Cond` describes
  it), or a text of SyngoQL with a value for each of its parameters. Then the order, the limit,
  the offset and the page token, each as a field of its own.
- **The answer wanted**: the documents, their identifiers, their number, one page, the first
  document alone, their removal, or the explanation of how the query would be answered.
- **What is set replaces what the text stated**, also when it is set to nothing: an order of no
  field, an offset of zero, a limit taken away. Each of the three says whether it was set.
- **The first document is the server's to find.** A text may limit its answer to no document,
  which a client that added "one at most" would overrule without knowing.

**Pulled answers.** Documents and identifiers come in batches. A batch holds as many as fit in
about 256 KiB, and at least one. When there are more, the batch names a **cursor**:

- `Next` pulls the following batch, `CloseCursor` gives the answer up.
- The whole answer is read from one moment of the database, however long the client takes.
- A cursor belongs to its session. Outside a transaction a driver keeps the connection out of
  its pool until the answer ends. Inside a transaction the session goes on with other requests
  between two batches: that is what lets a program write inside its loop.
- The server bounds how many cursors a session holds (`Welcome` says how many) and how long one
  may stay unread. A cursor it gave up answers `STALE`.

**Pages.** A page token is made by the server and signed by it: a token that was changed on its
way through a browser is refused, not obeyed. A page is one message: it holds 10,000 documents
at most, and one whose documents would not fit in a message is answered `TOO_LARGE`, for the
program to ask for smaller pages. A page is read at one moment of the database, like any answer.

## Transactions

- `Begin` puts the session in a transaction; `Commit` or `Rollback` ends it. A transaction can
  be begun read only: it is then never checked against others.
- `Update` is the driver's: it begins, runs the program's function, commits, and when the
  commit answers `CONFLICT` it runs the function again, as the embedded library does.
- **A function that keeps losing still finishes.** The `Begin` of an attempt that runs again
  says so (`again`), on the session whose last commit lost. The server then gives it its turn,
  as the embedded library gives one to a function it runs again: it waits for the turn of what
  it lost on, and after a few losses in a row it runs alone. The turn is held for a moment
  only (50 ms): the function runs a network away.
- A transaction left open is ended by the server after a time, or when its connection dies. A
  transaction that lived longer than the server could follow answers `TX_EXPIRED`.
- **A commit whose answer never came** (the connection died after the request left) is reported
  as `syngo.ErrOutcomeUnknown`, and is never sent again by the driver: the write may have been
  made. The same holds for any single write.

## `Modify`

Embedded, `Modify` reads a document, runs the program's function on it and writes the result as
one. Through a server the function runs at the client, so:

1. The driver sends `Get` and asks for the **stamp** of the document: a few bytes the server
   makes from the stored document, which mean nothing to the client.
2. It decodes the document into the program's value, and encodes that value **before** and
   **after** the function ran.
3. It sends `Modify` with the stamp and the two texts. The server gives the stored document the
   difference between them, exactly as the embedded library does, **if the document still has
   that stamp**. What the program's type does not declare stays in the document.
4. Otherwise the server answers `STALE`, and the driver starts again at 1. After three such
   answers it does the same inside an `Update`, which is sure to finish.

Two round trips, and the server holds nothing between them. The stamp is not the revision: a
revision begins again at 1 when a document is deleted and stored anew.

## Errors

An `Error` has a code, a message for people, and for some codes a detail (which index and which
document for `DUPLICATE`, where in the text for `INVALID_QUERY`).

| Code | The driver returns |
| --- | --- |
| `NOT_FOUND` to `CORRUPT` | the error of the public package with that name (`syngo.ErrNotFound`, `*syngo.DuplicateError`, ...) |
| `UNAUTHENTICATED`, `FORBIDDEN`, `NO_DATABASE` | errors of the driver |
| `STALE` | the driver's concern for a stamp; for a cursor, a token or a transaction, an error of the driver that says what was lost |
| `BUSY` | nothing: the driver waits as long as the answer says, and sends again |
| `CANCELLED` | the error of the program's context |
| `UNSUPPORTED`, `PROTOCOL`, `INTERNAL` | errors of the driver, with the server's message |

## Stopping a request

- A request says how long its client will wait. The server stops working when that has passed.
- A `CANCEL` frame stops the request of the number it carries, which then answers `CANCELLED`.
  One that arrives after the answer left changes nothing. The session goes on either way.
- A connection that ends while a request is served stops the request: nobody waits for it.
- `CANCEL` is the only thing a client may send before its answer. Any other frame ends the
  session: a client that sends its next request without waiting is refused at the second.

## Versions

- `Hello` states a range of versions and `Welcome` picks the highest both know. A server that
  shares none answers an error that names what it speaks.
- Inside a version, messages only grow: a field that is added is ignored by whoever does not
  know it, and a request the server does not know is answered `UNSUPPORTED`.

## How the messages are written

As Protocol Buffers writes them, fields in the order of their numbers. This driver writes and
reads them with code written by hand (package `wire`), which a test holds to the code generated
from `syngo.proto`: over messages made at random, what either writes the other reads as the same
message. A driver in another language can use whatever its Protocol Buffers library generates.

## Settled in Phase 3

| Question | Answer |
| --- | --- |
| The encoder for the messages | written by hand. Measured: the messages of a read cost 0.26 µs against 1.49 with generated code, those of a query answered by 200 documents 23 µs against 72 |
| The size of a batch | about 256 KiB: a batch is closed by the document that takes it past that |
| The form of an address | `syngodbs://host[:port][/database]` with TLS and `syngodb://` without, as SyngoDB Server 6 had them, and `syngodb+unix:///path/to/socket[?database=name]` (on Windows `syngodb+unix:///C:/path/to/socket`). `?insecure=true` takes whatever certificate the server shows. An address never carries a user or a password |
| The database of a request | named by each request, where the first draft named it once, in `Hello` |
