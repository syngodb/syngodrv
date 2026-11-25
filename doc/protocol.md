# The SyngoDB Server protocol, version 1

**A draft, on paper.** Written in Phase 1 so that the public package is shaped against its second
implementer before anything is frozen. Nothing speaks it yet: the driver and the server are
built to it in Phase 3, and what they teach changes this page before a version 1 is promised.
The messages are defined in [`wire/syngo.proto`](../wire/syngo.proto).

It is our own protocol. The measurements that decided it are in the server's plan: a read costs
10 µs through it and 46 µs through gRPC.

## In one page

- A client opens a connection, says hello, and logs in once. The connection is then a
  **session**.
- A session carries **one request at a time**: a request, then its answer. A driver that wants
  more at once keeps more connections (a pool).
- A **transaction lives on its session**: from `Begin` to `Commit`, everything the session sends
  is part of it.
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
- **Request numbers** are chosen by the client, each greater than the one before. An answer
  carries the number of its request.
- A frame that is not one of these, or that comes out of turn, ends the session.

## The session

1. The client sends `Hello`: the versions of the protocol it speaks, who it is (for the log), the
   database it wants, and a user and password unless its certificate is its identity.
2. The server answers `Welcome`: the version both will speak, the role of the session, and the
   limits the client must keep to (frame, message, idle time, open answers). Or it answers an
   error and closes.
3. Requests and answers follow, one at a time.

- **The role is read at every request.** A user whose role changes, or who is removed, loses
  what they lost at once, on sessions that are already open.
- **Keep alive.** A session silent for longer than `Welcome` says is closed. `Ping` is a
  request like any other, and is how a pool keeps an idle connection and learns that one died.
- **A failed login** is answered after a delay that grows, never by locking anyone out.

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

What belongs to the server as a whole (creating and dropping databases, users and identities,
backup and restore) comes with the phases that build it, as more requests of the same shape.

## Queries

A `Query` says what is asked and what kind of answer is wanted:

- **What is asked**: conditions (`Cond`, a tree of comparisons exactly as `syngo.Cond` describes
  it), or a text of SyngoQL with a value for each of its parameters. Then the order, the limit,
  the offset and the page token, each as a field of its own.
- **The answer wanted**: the documents, their identifiers, their number, one page, their
  removal, or the explanation of how the query would be answered.

**Pulled answers.** Documents and identifiers come in batches. A batch holds as many as fit in
about 256 KiB, and at least one. When there are more, the batch names a **cursor**:

- `Next` pulls the following batch, `CloseCursor` gives the answer up.
- The whole answer is read from one moment of the database, however long the client takes.
- A cursor belongs to its session. Outside a transaction a driver keeps the connection out of
  its pool until the answer ends. Inside a transaction the session goes on with other requests
  between two batches: that is what lets a program write inside its loop.
- The server bounds how many cursors a session holds and how long one may stay unread. A cursor
  it gave up answers `STALE`.

**Pages.** A page token is made by the server and signed by it: a token that was changed on its
way through a browser is refused, not obeyed.

## Transactions

- `Begin` puts the session in a transaction; `Commit` or `Rollback` ends it. A transaction can
  be begun read only: it is then never checked against others.
- `Update` is the driver's: it begins, runs the program's function, commits, and when the
  commit answers `CONFLICT` it runs the function again, as the embedded library does.
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
   answers it does the same inside a transaction, which is sure to finish.

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

## Versions

- `Hello` states a range of versions and `Welcome` picks the highest both know. A server that
  shares none answers an error that names what it speaks.
- Inside a version, messages only grow: a field that is added is ignored by whoever does not
  know it, and a request the server does not know is answered `UNSUPPORTED`.

## Left for Phase 3

| Question | How it is settled |
| --- | --- |
| The encoder for the messages (generated code, or written by hand against the wire format) | measured against the targets: 15 µs a read, 16 KiB an idle connection |
| The size of a batch | measured on the query of 200 documents |
| The form of the address a program gives the driver | with the names and ports, as the phase opens |
