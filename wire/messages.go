package wire

import (
	"encoding/binary"
	"slices"
	"unsafe"

	"github.com/syngodb/syngo"
)

// The messages, as Go values. Each has the fields syngo.proto gives it, an encoder that appends
// it to what is being sent, and a decoder that reads it from what was received.
//
// What a decoder hands out as bytes (a document, a value, a stamp) lies in the message that was
// received: it holds until the next message is read from the connection, and whoever keeps it
// copies it. Texts are strings of their own.

// Op says which request a Request is. The numbers are those of the fields of syngo.proto.
type Op uint8

const (
	OpPing        Op = 10
	OpGet         Op = 20
	OpExists      Op = 21
	OpCount       Op = 22
	OpPut         Op = 23
	OpPutBatch    Op = 24
	OpPatch       Op = 25
	OpModify      Op = 26
	OpDelete      Op = 27
	OpQuery       Op = 30
	OpNext        Op = 31
	OpCloseCursor Op = 32
	OpBegin       Op = 40
	OpCommit      Op = 41
	OpRollback    Op = 42

	OpEnsureIndex    Op = 50
	OpRebuildIndex   Op = 51
	OpDropIndex      Op = 52
	OpIndexes        Op = 53
	OpCollections    Op = 54
	OpDropCollection Op = 55
	OpStats          Op = 56

	OpEnsureDatabase Op = 60
	OpDropDatabase   Op = 61
	OpDatabases      Op = 62

	OpPutUser     Op = 70
	OpRemoveUser  Op = 71
	OpUsers       Op = 72
	OpSetPassword Op = 73

	OpPutIdentity    Op = 74
	OpRemoveIdentity Op = 75
	OpIdentities     Op = 76

	OpBackup    Op = 80
	OpExport    Op = 81
	OpRestore   Op = 82
	OpImport    Op = 83
	OpPart      Op = 84
	OpEnd       Op = 85
	OpCompact   Op = 86
	OpMeasure   Op = 87
	OpKept      Op = 88
	OpBringBack Op = 89
	OpDiscard   Op = 90
)

// Ops is every request there is, in the order of their numbers. Whoever must have something
// for each of them (a server its answer, a table of who may ask what its row) goes through
// this list, and a test of this package holds the list to the constants above.
func Ops() []Op {
	return []Op{OpPing, OpGet, OpExists, OpCount, OpPut, OpPutBatch, OpPatch, OpModify, OpDelete, OpQuery, OpNext, OpCloseCursor,
		OpBegin, OpCommit, OpRollback, OpEnsureIndex, OpRebuildIndex, OpDropIndex, OpIndexes, OpCollections, OpDropCollection, OpStats,
		OpEnsureDatabase, OpDropDatabase, OpDatabases, OpPutUser, OpRemoveUser, OpUsers, OpSetPassword,
		OpPutIdentity, OpRemoveIdentity, OpIdentities,
		OpBackup, OpExport, OpRestore, OpImport, OpPart, OpEnd, OpCompact, OpMeasure, OpKept, OpBringBack, OpDiscard}
}

// Request is one request of a session. Op says which, and the field of that name holds it.
type Request struct {
	// Room is the most the lists of the request may take once it is decoded, beside the bytes
	// of its message; zero is DefaultRoom. A request that describes more is not decoded: Decode
	// then returns false with Over set, which says "too large" and not "no message". Used is
	// what the lists of the request that was decoded last took.
	Room int
	Over bool
	Used int

	// Timeout is how long the client will wait, in milliseconds; zero for no limit of its own.
	Timeout uint32
	// Database is the database the request is for; empty for what belongs to the server as a
	// whole.
	Database string
	Op       Op

	Get      Get
	Exists   Exists
	Count    Count
	Put      Put
	PutBatch PutBatch
	Patch    Patch
	Modify   Modify
	Delete   Delete
	Query    Query
	// Cursor is the answer Next pulls from and CloseCursor gives up.
	Cursor uint64
	// ReadOnly is of Begin: a transaction that only reads. Again is of Begin too: the
	// transaction runs again what the last one of the session ran, which lost.
	ReadOnly    bool
	Again       bool
	EnsureIndex EnsureIndex
	// Named is the index of RebuildIndex and DropIndex.
	Named IndexRef
	// Indexes is the collection of Indexes.
	Indexes string
	// Name is the name of DropCollection, EnsureDatabase, DropDatabase, RemoveUser and
	// RemoveIdentity.
	Name        string
	PutUser     PutUser
	SetPassword SetPassword
	PutIdentity PutIdentity
	// Restore and Import begin an upload, which Part then sends and End ends.
	Restore Restore
	Import  Import
	// Part is the data of OpPart. It lies in the message it was decoded from.
	Part []byte
	// Abandon is of OpEnd: what was sent is given up.
	Abandon bool
	// Measure is the collection of OpMeasure.
	Measure string
	// KeptID is the kept directory of OpBringBack and OpDiscard.
	KeptID uint64
	// Details is of OpDatabases: each database with what it takes and since when it is.
	Details bool
}

// Restore begins the upload of a backup, which becomes the database of that name. Passphrase
// is the one the backup was made under, when that is not the server's own.
type Restore struct {
	Name       string
	Passphrase string
}

// Import begins the upload of an export. Without Replace its lines are added to the database
// of that name as it is; with it they become the database of that name, in the place of what
// it was. SkipRefused passes over the lines that are refused.
type Import struct {
	Name        string
	Replace     bool
	SkipRefused bool
}

// PutIdentity makes an identity or changes one. A role of zero keeps the role the identity
// has, and an empty fingerprint the fingerprint.
type PutIdentity struct {
	Name        string
	Role        uint8
	Fingerprint string
}

// PutUser makes a user or changes one. A role of zero keeps the role the user has, and an empty
// password the password.
type PutUser struct {
	Name     string
	Role     uint8
	Password string
}

// SetPassword gives the user of the session another password, for the one the user has.
type SetPassword struct {
	Old, New string
}

// Forget lets go of what a request holds that is a secret, once the request was served: the
// passwords of PutUser and SetPassword, and the passphrase of Restore. Whoever read the request
// from a connection wipes the message they lay in as well.
func (m *Request) Forget() {
	m.PutUser.Password, m.SetPassword, m.Restore.Passphrase = "", SetPassword{}, ""
}

// Secret says whether the request carries a password or a passphrase.
func (m *Request) Secret() bool {
	return m.Op == OpPutUser || m.Op == OpSetPassword || m.Op == OpRestore
}

type Get struct {
	Collection string
	ID         string
	// Stamp asks for the stamp of the document as well.
	Stamp bool
}

type Exists struct {
	Collection string
	ID         string
}

type Count struct {
	Collection string
}

// Put stores a document. Mode is WritePut, WriteInsert or WriteReplace.
type Put struct {
	Collection string
	Mode       uint8
	Document   []byte
	Options    Write
}

// PutBatch stores several documents, as Put with WritePut stores each.
type PutBatch struct {
	Collection string
	Documents  [][]byte
	Options    Write
}

type Patch struct {
	Collection string
	ID         string
	Patch      []byte
	Options    Write
}

// Modify gives the stored document the difference between Before and After, if it still has the
// stamp.
type Modify struct {
	Collection string
	ID         string
	Stamp      []byte
	Before     []byte
	After      []byte
	Options    Write
}

type Delete struct {
	Collection string
	ID         string
}

// Write is what the options of a write come to. Expiry is one of ExpiryUnstated, ExpiryNever,
// ExpiryIn and ExpiryAt, and Meta one of MetaUnstated, MetaNone and MetaSet.
type Write struct {
	Expiry uint8
	// ExpireIn is in nanoseconds, for ExpiryIn.
	ExpireIn int64
	// ExpireAt is for ExpiryAt.
	ExpireAt Instant
	Meta     uint8
	// MetaJSON is for MetaSet.
	MetaJSON []byte
}

// Instant is a time as seconds since 1970 and nanoseconds within the second.
type Instant struct {
	Seconds int64
	Nanos   int32
}

// Index is the definition of an index.
type Index struct {
	Name   string
	Fields []string
	Unique bool
}

type EnsureIndex struct {
	Collection string
	Index      Index
}

// IndexRef names an index of a collection.
type IndexRef struct {
	Collection string
	Name       string
}

// Answer is the kind of answer a query is asked for.
type Answer uint8

const (
	AnswerDocuments Answer = iota
	AnswerIDs
	AnswerCount
	AnswerPage
	AnswerDelete
	AnswerExplain
	AnswerFirst
)

// Query asks a collection a question and says what kind of answer is wanted.
type Query struct {
	Collection string
	// The question: Where when HasWhere, the text when HasText, every document otherwise.
	HasWhere bool
	Where    Cond
	HasText  bool
	Text     string
	Args     []Arg
	// HasOrder says an order was set, even to nothing: it then replaces the one the text stated.
	HasOrder bool
	Order    []Order
	// HasLimit says a limit was set, and Unlimited that the limit of the text is taken away.
	HasLimit  bool
	Limit     uint64
	Unlimited bool
	// HasOffset says an offset was set, even to zero: it then replaces the one the text stated.
	HasOffset bool
	Offset    uint64
	After     string
	Answer    Answer
	PageSize  uint32
}

// Arg is the value of one parameter of a query text: one value, or a list of them.
type Arg struct {
	Name   string
	Values [][]byte
	List   bool
}

type Order struct {
	Field      string
	Descending bool
	Relevance  bool
}

// Cond is a condition as the public package describes it: Op has the numbers of syngo.Op.
type Cond struct {
	Op       uint8
	Negated  bool
	Field    string
	Values   [][]byte
	Language string
	Conds    []Cond
}

// Append appends the request to b.
func (m *Request) Append(b []byte) []byte {
	b = putUint(b, 1, uint64(m.Timeout))
	b = putString(b, 2, m.Database)
	if m.Op == 0 {
		return b
	}
	var at int
	b, at = open(b, int(m.Op))
	switch m.Op {
	case OpGet:
		b = putString(b, 1, m.Get.Collection)
		b = putString(b, 2, m.Get.ID)
		b = putBool(b, 3, m.Get.Stamp)
	case OpExists:
		b = putString(b, 1, m.Exists.Collection)
		b = putString(b, 2, m.Exists.ID)
	case OpCount:
		b = putString(b, 1, m.Count.Collection)
	case OpPut:
		b = putString(b, 1, m.Put.Collection)
		b = putUint(b, 2, uint64(m.Put.Mode))
		b = putBytes(b, 3, m.Put.Document)
		b = m.Put.Options.put(b, 4)
	case OpPutBatch:
		b = putString(b, 1, m.PutBatch.Collection)
		for _, doc := range m.PutBatch.Documents {
			b = setBytes(b, 2, doc)
		}
		b = m.PutBatch.Options.put(b, 3)
	case OpPatch:
		b = putString(b, 1, m.Patch.Collection)
		b = putString(b, 2, m.Patch.ID)
		b = putBytes(b, 3, m.Patch.Patch)
		b = m.Patch.Options.put(b, 4)
	case OpModify:
		b = putString(b, 1, m.Modify.Collection)
		b = putString(b, 2, m.Modify.ID)
		b = putBytes(b, 3, m.Modify.Stamp)
		b = putBytes(b, 4, m.Modify.Before)
		b = putBytes(b, 5, m.Modify.After)
		b = m.Modify.Options.put(b, 6)
	case OpDelete:
		b = putString(b, 1, m.Delete.Collection)
		b = putString(b, 2, m.Delete.ID)
	case OpQuery:
		b = m.Query.append(b)
	case OpNext, OpCloseCursor:
		b = putUint(b, 1, m.Cursor)
	case OpBegin:
		b = putBool(b, 1, m.ReadOnly)
		b = putBool(b, 2, m.Again)
	case OpEnsureIndex:
		b = putString(b, 1, m.EnsureIndex.Collection)
		b = m.EnsureIndex.Index.put(b, 2)
	case OpRebuildIndex, OpDropIndex:
		b = putString(b, 1, m.Named.Collection)
		b = putString(b, 2, m.Named.Name)
	case OpIndexes:
		b = putString(b, 1, m.Indexes)
	case OpDropCollection, OpEnsureDatabase, OpDropDatabase, OpRemoveUser, OpRemoveIdentity:
		b = putString(b, 1, m.Name)
	case OpDatabases:
		b = putBool(b, 1, m.Details)
	case OpRestore:
		b = putString(b, 1, m.Restore.Name)
		b = putString(b, 2, m.Restore.Passphrase)
	case OpImport:
		b = putString(b, 1, m.Import.Name)
		b = putBool(b, 2, m.Import.Replace)
		b = putBool(b, 3, m.Import.SkipRefused)
	case OpPart:
		b = putBytes(b, 1, m.Part)
	case OpEnd:
		b = putBool(b, 1, m.Abandon)
	case OpMeasure:
		b = putString(b, 1, m.Measure)
	case OpBringBack, OpDiscard:
		b = putUint(b, 1, m.KeptID)
	case OpPutIdentity:
		b = putString(b, 1, m.PutIdentity.Name)
		b = putUint(b, 2, uint64(m.PutIdentity.Role))
		b = putString(b, 3, m.PutIdentity.Fingerprint)
	case OpPutUser:
		b = putString(b, 1, m.PutUser.Name)
		b = putUint(b, 2, uint64(m.PutUser.Role))
		b = putString(b, 3, m.PutUser.Password)
	case OpSetPassword:
		b = putString(b, 1, m.SetPassword.Old)
		b = putString(b, 2, m.SetPassword.New)
	}
	return shut(b, at)
}

// Decode reads the request from b. It reports whether b is a request; a request this package
// does not know has Op zero.
func (m *Request) Decode(b []byte) bool {
	m.Timeout, m.Database, m.Op = 0, "", 0
	bud := budget{room: m.Room}
	if bud.room <= 0 {
		bud.room = DefaultRoom
	}
	room := bud.room
	d := decoder{b: b, bud: &bud}
	for {
		field, how, ok := d.next()
		if !ok {
			m.Over, m.Used = bud.over, room-max(bud.room, 0)
			return !d.bad
		}
		switch {
		case field == 1:
			m.Timeout = d.u32(how)
		case field == 2:
			m.Database = d.text(how)
		case field >= 10 && field < 256 && how == asBytes:
			body, _ := d.inner(how)
			m.Op = Op(field)
			switch m.Op {
			case OpPing, OpCommit, OpRollback, OpCollections, OpStats, OpUsers, OpIdentities,
				OpBackup, OpExport, OpCompact, OpKept:
				body.rest()
			case OpDatabases:
				m.Details = false
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Details = body.bool(h)
					default:
						body.skip(h)
					}
				}
			case OpPutIdentity:
				m.PutIdentity = PutIdentity{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.PutIdentity.Name = body.text(h)
					case 2:
						m.PutIdentity.Role = body.small(h)
					case 3:
						m.PutIdentity.Fingerprint = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpGet:
				m.Get = Get{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Get.Collection = body.text(h)
					case 2:
						m.Get.ID = body.text(h)
					case 3:
						m.Get.Stamp = body.bool(h)
					default:
						body.skip(h)
					}
				}
			case OpExists:
				m.Exists = Exists{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Exists.Collection = body.text(h)
					case 2:
						m.Exists.ID = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpCount:
				m.Count = Count{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Count.Collection = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpPut:
				m.Put = Put{}
				m.Put.decode(&body)
			case OpPutBatch:
				m.PutBatch = PutBatch{Documents: m.PutBatch.Documents[:0]}
				m.PutBatch.decode(&body)
			case OpPatch:
				m.Patch = Patch{}
				m.Patch.decode(&body)
			case OpModify:
				m.Modify = Modify{}
				m.Modify.decode(&body)
			case OpDelete:
				m.Delete = Delete{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Delete.Collection = body.text(h)
					case 2:
						m.Delete.ID = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpQuery:
				m.Query.decode(&body)
			case OpBegin:
				m.ReadOnly, m.Again = false, false
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.ReadOnly = body.bool(h)
					case 2:
						m.Again = body.bool(h)
					default:
						body.skip(h)
					}
				}
			case OpEnsureIndex:
				m.EnsureIndex = EnsureIndex{Index: Index{Fields: m.EnsureIndex.Index.Fields[:0]}}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.EnsureIndex.Collection = body.text(h)
					case 2:
						in, ok := body.inner(h)
						if !ok {
							continue
						}
						m.EnsureIndex.Index = Index{Fields: m.EnsureIndex.Index.Fields[:0]}
						m.EnsureIndex.Index.decode(&in)
						body.bad = body.bad || in.bad
					default:
						body.skip(h)
					}
				}
			case OpRebuildIndex, OpDropIndex:
				m.Named = IndexRef{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Named.Collection = body.text(h)
					case 2:
						m.Named.Name = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpPutUser:
				m.PutUser = PutUser{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.PutUser.Name = body.text(h)
					case 2:
						m.PutUser.Role = body.small(h)
					case 3:
						m.PutUser.Password = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpSetPassword:
				m.SetPassword = SetPassword{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.SetPassword.Old = body.text(h)
					case 2:
						m.SetPassword.New = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpIndexes, OpDropCollection, OpEnsureDatabase, OpDropDatabase, OpRemoveUser, OpRemoveIdentity:
				// One text in field 1, which Indexes keeps apart from the others.
				var text string
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						text = body.text(h)
					default:
						body.skip(h)
					}
				}
				if m.Op == OpIndexes {
					m.Indexes = text
				} else {
					m.Name = text
				}
			case OpRestore:
				m.Restore = Restore{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Restore.Name = body.text(h)
					case 2:
						m.Restore.Passphrase = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpImport:
				m.Import = Import{}
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Import.Name = body.text(h)
					case 2:
						m.Import.Replace = body.bool(h)
					case 3:
						m.Import.SkipRefused = body.bool(h)
					default:
						body.skip(h)
					}
				}
			case OpPart:
				m.Part = nil
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Part = body.bytes(h)
					default:
						body.skip(h)
					}
				}
			case OpEnd:
				m.Abandon = false
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Abandon = body.bool(h)
					default:
						body.skip(h)
					}
				}
			case OpMeasure:
				m.Measure = ""
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Measure = body.text(h)
					default:
						body.skip(h)
					}
				}
			case OpBringBack, OpDiscard:
				m.KeptID = 0
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.KeptID = body.uint(h)
					default:
						body.skip(h)
					}
				}
			case OpNext, OpCloseCursor:
				m.Cursor = 0
				for {
					f, h, ok := body.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						m.Cursor = body.uint(h)
					default:
						body.skip(h)
					}
				}
			default:
				// A request this package does not know: the server answers UNSUPPORTED.
				m.Op = 0
			}
			d.bad = d.bad || body.bad
		default:
			d.skip(how)
		}
	}
}

func (m *Query) append(b []byte) []byte {
	b = putString(b, 1, m.Collection)
	var at int
	switch {
	case m.HasWhere:
		b, at = open(b, 2)
		b = shut(m.Where.append(b), at)
	case m.HasText:
		b, at = open(b, 3)
		b = putString(b, 1, m.Text)
		for i := range m.Args {
			// An entry of a map: the name, then the value.
			var entry, value int
			b, entry = open(b, 2)
			b = putString(b, 1, m.Args[i].Name)
			b, value = open(b, 2)
			for _, v := range m.Args[i].Values {
				b = setBytes(b, 1, v)
			}
			b = putBool(b, 2, m.Args[i].List)
			b = shut(shut(b, value), entry)
		}
		b = shut(b, at)
	}
	if m.HasOrder {
		b, at = open(b, 4)
		for i := range m.Order {
			var one int
			b, one = open(b, 1)
			b = putString(b, 1, m.Order[i].Field)
			b = putBool(b, 2, m.Order[i].Descending)
			b = putBool(b, 3, m.Order[i].Relevance)
			b = shut(b, one)
		}
		b = shut(b, at)
	}
	switch {
	case m.HasLimit:
		b = setUint(b, 5, m.Limit)
	case m.Unlimited:
		b = setBool(b, 6, true)
	}
	if m.HasOffset {
		b = setUint(b, 7, m.Offset)
	}
	b = putString(b, 8, m.After)
	b = putUint(b, 9, uint64(m.Answer))
	return putUint(b, 10, uint64(m.PageSize))
}

func (m *Query) decode(d *decoder) {
	*m = Query{Args: m.Args[:0], Order: m.Order[:0]}
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Collection = d.text(how)
		case 2:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.HasWhere, m.HasText = true, false
			m.Where = Cond{}
			m.Where.decode(&body, 0)
			d.bad = d.bad || body.bad
		case 3:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.HasText, m.HasWhere = true, false
			m.Text, m.Args = "", m.Args[:0]
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					m.Text = body.text(h)
				case 2:
					entry, ok := body.inner(h)
					if !ok {
						continue
					}
					var arg Arg
					for {
						f, h, ok := entry.next()
						if !ok {
							break
						}
						switch f {
						case 1:
							arg.Name = entry.text(h)
						case 2:
							value, ok := entry.inner(h)
							if !ok {
								continue
							}
							arg.Values, arg.List = nil, false
							for {
								f, h, ok := value.next()
								if !ok {
									break
								}
								switch f {
								case 1:
									if h != asBytes {
										value.skip(h)
										continue
									}
									if !value.take(unsafe.Sizeof([]byte(nil))) {
										continue
									}
									arg.Values = append(arg.Values, value.bytes(h))
								case 2:
									arg.List = value.bool(h)
								default:
									value.skip(h)
								}
							}
							entry.bad = entry.bad || value.bad
						default:
							entry.skip(h)
						}
					}
					body.bad = body.bad || entry.bad
					if body.take(unsafe.Sizeof(arg)) {
						m.Args = append(m.Args, arg)
					}
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		case 4:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.HasOrder = true
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 || h != asBytes {
					body.skip(h)
					continue
				}
				one := decoder{b: body.bytes(h), bud: body.bud}
				var order Order
				for {
					f, h, ok := one.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						order.Field = one.text(h)
					case 2:
						order.Descending = one.bool(h)
					case 3:
						order.Relevance = one.bool(h)
					default:
						one.skip(h)
					}
				}
				body.bad = body.bad || one.bad
				if body.take(unsafe.Sizeof(order)) {
					m.Order = append(m.Order, order)
				}
			}
			d.bad = d.bad || body.bad
		case 5:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Limit, m.HasLimit, m.Unlimited = d.uint(how), true, false
		case 6:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Unlimited, m.HasLimit, m.Limit = d.bool(how), false, 0
		case 7:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Offset, m.HasOffset = d.uint(how), true
		case 8:
			m.After = d.text(how)
		case 9:
			m.Answer = Answer(d.small(how))
		case 10:
			m.PageSize = d.u32(how)
		default:
			d.skip(how)
		}
	}
}

// CondDepth is how deep conditions may be combined in one another: as deep as the public
// package lets a program combine them, which is as deep as the text of a query may nest. More is
// no message: it keeps a peer from making the reader go as deep as it likes.
const CondDepth = syngo.MaxCondDepth

func (m *Cond) append(b []byte) []byte {
	b = putUint(b, 1, uint64(m.Op))
	b = putBool(b, 2, m.Negated)
	b = putString(b, 3, m.Field)
	for _, v := range m.Values {
		b = setBytes(b, 4, v)
	}
	b = putString(b, 5, m.Language)
	for i := range m.Conds {
		var at int
		b, at = open(b, 6)
		b = shut(m.Conds[i].append(b), at)
	}
	return b
}

func (m *Cond) decode(d *decoder, depth int) {
	if depth > CondDepth {
		d.bad = true
		return
	}
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Op = d.small(how)
		case 2:
			m.Negated = d.bool(how)
		case 3:
			m.Field = d.text(how)
		case 4:
			if how != asBytes {
				d.skip(how)
				continue
			}
			if !d.take(unsafe.Sizeof([]byte(nil))) {
				continue
			}
			m.Values = append(m.Values, d.bytes(how))
		case 5:
			m.Language = d.text(how)
		case 6:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			var cond Cond
			cond.decode(&body, depth+1)
			d.bad = d.bad || body.bad
			if d.take(unsafe.Sizeof(cond)) {
				m.Conds = append(m.Conds, cond)
			}
		default:
			d.skip(how)
		}
	}
}

// put writes the options of a write as the field, when they state something.
func (m *Write) put(b []byte, field int) []byte {
	if m.Expiry == 0 && m.Meta == 0 {
		return b
	}
	var at int
	b, at = open(b, field)
	switch m.Expiry {
	case ExpiryNever:
		b = setBool(b, 1, true)
	case ExpiryIn:
		b = setUint(b, 2, uint64(m.ExpireIn))
	case ExpiryAt:
		var in int
		b, in = open(b, 3)
		b = putUint(b, 1, uint64(m.ExpireAt.Seconds))
		b = putUint(b, 2, uint64(int64(m.ExpireAt.Nanos)))
		b = shut(b, in)
	}
	switch m.Meta {
	case MetaNone:
		b = setBool(b, 4, true)
	case MetaSet:
		b = setBytes(b, 5, m.MetaJSON)
	}
	return shut(b, at)
}

func (m *Write) decode(d *decoder) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Expiry, m.ExpireIn, m.ExpireAt = ExpiryUnstated, 0, Instant{}
			if d.bool(how) {
				m.Expiry = ExpiryNever
			}
		case 2:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Expiry, m.ExpireIn, m.ExpireAt = ExpiryIn, int64(d.uint(how)), Instant{}
		case 3:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Expiry, m.ExpireIn, m.ExpireAt = ExpiryAt, 0, Instant{}
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					m.ExpireAt.Seconds = int64(body.uint(h))
				case 2:
					m.ExpireAt.Nanos = body.i32(h)
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		case 4:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Meta, m.MetaJSON = MetaUnstated, nil
			if d.bool(how) {
				m.Meta = MetaNone
			}
		case 5:
			if how != asBytes {
				d.skip(how)
				continue
			}
			m.Meta, m.MetaJSON = MetaSet, d.bytes(how)
		default:
			d.skip(how)
		}
	}
}

// options reads the field of a message that holds the options of a write.
func (m *Write) options(d *decoder, how byte) {
	body, ok := d.inner(how)
	if !ok {
		return
	}
	*m = Write{}
	m.decode(&body)
	d.bad = d.bad || body.bad
}

func (m *Put) decode(d *decoder) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Collection = d.text(how)
		case 2:
			m.Mode = d.small(how)
		case 3:
			m.Document = d.bytes(how)
		case 4:
			m.Options.options(d, how)
		default:
			d.skip(how)
		}
	}
}

func (m *PutBatch) decode(d *decoder) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Collection = d.text(how)
		case 2:
			if how != asBytes {
				d.skip(how)
				continue
			}
			if !d.take(unsafe.Sizeof([]byte(nil))) {
				continue
			}
			m.Documents = append(m.Documents, d.bytes(how))
		case 3:
			m.Options.options(d, how)
		default:
			d.skip(how)
		}
	}
}

func (m *Patch) decode(d *decoder) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Collection = d.text(how)
		case 2:
			m.ID = d.text(how)
		case 3:
			m.Patch = d.bytes(how)
		case 4:
			m.Options.options(d, how)
		default:
			d.skip(how)
		}
	}
}

func (m *Modify) decode(d *decoder) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Collection = d.text(how)
		case 2:
			m.ID = d.text(how)
		case 3:
			m.Stamp = d.bytes(how)
		case 4:
			m.Before = d.bytes(how)
		case 5:
			m.After = d.bytes(how)
		case 6:
			m.Options.options(d, how)
		default:
			d.skip(how)
		}
	}
}

func (m *Index) append(b []byte) []byte {
	b = putString(b, 1, m.Name)
	for _, f := range m.Fields {
		b = setString(b, 2, f)
	}
	return putBool(b, 3, m.Unique)
}

// put writes the index as the field, which is written always.
func (m *Index) put(b []byte, field int) []byte {
	var at int
	b, at = open(b, field)
	return shut(m.append(b), at)
}

func (m *Index) decode(d *decoder) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Name = d.text(how)
		case 2:
			if how != asBytes {
				d.skip(how)
				continue
			}
			if !d.take(unsafe.Sizeof("")) {
				continue
			}
			m.Fields = append(m.Fields, d.text(how))
		case 3:
			m.Unique = d.bool(how)
		default:
			d.skip(how)
		}
	}
}

// Result says which answer a Response is. The numbers are those of the fields of syngo.proto.
type Result uint8

const (
	ResultError      Result = 1
	ResultDone       Result = 2
	ResultDocument   Result = 3
	ResultFound      Result = 4
	ResultNumber     Result = 5
	ResultStored     Result = 6
	ResultBatch      Result = 7
	ResultText       Result = 8
	ResultIndexes    Result = 9
	ResultNames      Result = 10
	ResultStats      Result = 11
	ResultUsers      Result = 12
	ResultIdentities Result = 13
	ResultStream     Result = 14
	ResultReport     Result = 15
	ResultKept       Result = 16
	ResultDatabases  Result = 17
)

// Response is the answer to one request. Result says which, and the field of that name holds it.
type Response struct {
	// Room is the most the lists of the answer may take once it is decoded, beside the bytes of
	// its message; zero is DefaultRoom, or sixteen times the message when that is more. An
	// answer that describes more is not decoded: Decode returns false with Over set.
	Room int
	Over bool

	// AliasIDs is set by a reader that copies the documents it keeps. The identifier of a
	// document that is read then lies in the message, as its text does, and holds as long: a
	// reader that reads hundreds of documents makes no string for each.
	AliasIDs bool

	Result   Result
	Error    Error
	Document Document
	Found    bool
	Number   int64
	Batch    Batch
	Text     string
	// Stored holds the identifiers of ResultStored.
	Stored  []string
	Indexes []Index
	// Names holds the names of ResultNames.
	Names []string
	Stats Stats
	// Users holds the users of ResultUsers.
	Users []User
	// Identities holds the identities of ResultIdentities.
	Identities []Identity
	// Stream is a part of a backup or of an export.
	Stream Stream
	// Report is what an import did.
	Report Report
	// Kept holds the kept directories of ResultKept.
	Kept []KeptEntry
	// Databases holds the databases of ResultDatabases.
	Databases []DatabaseEntry
}

// Stream is a part of a stream of bytes. When Cursor is not zero the stream goes on, and Next
// pulls its next part. Data lies in the message it was decoded from.
type Stream struct {
	Data   []byte
	Cursor uint64
}

// Report is what an import did. Stopped is the line the import stopped at, when it stopped at
// one it refused: its Line is zero otherwise.
type Report struct {
	Lines     int64
	Documents int64
	Indexes   uint32
	Refused   int64
	Refusals  []Refusal
	Stopped   Refusal
}

// Refusal is a line of an import that was refused, and why.
type Refusal struct {
	Line  int64
	Error Error
}

// KeptEntry is a directory that was a database's until the database was replaced, and is kept
// for a time. The times are nanoseconds since 1970.
type KeptEntry struct {
	ID      uint64
	Name    string
	Created int64
	At      int64
	Until   int64
	Bytes   int64
}

// DatabaseEntry is a database as a list with details shows one. Created is nanoseconds since
// 1970.
type DatabaseEntry struct {
	Name    string
	Created int64
	Bytes   int64
}

// Identity is an identity of the server as a list of them shows one: who, what it may do, and
// the fingerprint of the key of its certificate.
type Identity struct {
	Name        string
	Role        uint8
	Fingerprint string
}

// User is a user of the server as a list of them shows one: who, and what the user may do.
type User struct {
	Name string
	Role uint8
}

// Document is a document with what the database keeps beside it.
type Document struct {
	ID       string
	JSON     []byte
	Revision uint64
	Modified int64
	Expires  int64
	Meta     []byte
	Stamp    []byte
}

// Batch is a part of an answer. When Cursor is not zero the answer goes on.
type Batch struct {
	Documents []Document
	IDs       []string
	Cursor    uint64
	NextPage  string
}

// Error is why a request was not done.
type Error struct {
	Code    uint32
	Message string
	// Detail says which detail follows: zero for none, or one of the Detail constants.
	Detail      uint8
	Duplicate   Duplicate
	BadDocument BadDocument
	BadQuery    BadQuery
	// Attempts is of a conflict.
	Attempts uint32
	// WaitMS is of a wait.
	WaitMS uint32
}

// Duplicate is the detail of CodeDuplicate: the index that refused and the document that holds
// the value.
type Duplicate struct {
	Collection string
	Index      string
	Holder     string
}

// BadDocument is the detail of CodeInvalidDocument. Offset is -1 when the error is not about a
// place in the text.
type BadDocument struct {
	Reason string
	Offset int32
}

// BadQuery is the detail of CodeInvalidQuery.
type BadQuery struct {
	Reason string
	Line   uint32
	Column uint32
	Text   string
}

// Stats is what the server tells about itself and its collections.
type Stats struct {
	Collections  []CollectionStats
	DiskBytes    int64
	Cache        CacheStats
	Engine       EngineStats
	Commits      CommitStats
	Transactions TxStats
	Sorts        SortStats
}

type CollectionStats struct {
	Name      string
	Documents int64
	Indexes   []IndexStats
}

type IndexStats struct {
	Index      Index
	Building   bool
	Rebuilding bool
	Stale      bool
}

type CacheStats struct {
	Budget    int64
	Used      int64
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

type EngineStats struct {
	CacheBudget int64
	CacheUsed   int64
	CacheHits   int64
	CacheMisses int64
	Buffered    int64
}

type CommitStats struct {
	Remembered int64
	Bytes      int64
	Kept       int64
	KeptUsed   int64
	KeptBudget int64
	Expired    uint64
}

type TxStats struct {
	Open    int64
	Writing int64
	Limit   int64
	Refused uint64
}

type SortStats struct {
	InMemory uint64
	OnDisk   uint64
	Each     int64
	Budget   int64
	Used     int64
}

func (m *Error) append(b []byte) []byte {
	b = putUint(b, 1, uint64(m.Code))
	b = putString(b, 2, m.Message)
	var at int
	switch m.Detail {
	case DetailDuplicate:
		b, at = open(b, 3)
		b = putString(b, 1, m.Duplicate.Collection)
		b = putString(b, 2, m.Duplicate.Index)
		b = putString(b, 3, m.Duplicate.Holder)
		b = shut(b, at)
	case DetailDocument:
		b, at = open(b, 4)
		b = putString(b, 1, m.BadDocument.Reason)
		// A negative number is written as its sign extended 64 bit value.
		b = putUint(b, 2, uint64(int64(m.BadDocument.Offset)))
		b = shut(b, at)
	case DetailQuery:
		b, at = open(b, 5)
		b = putString(b, 1, m.BadQuery.Reason)
		b = putUint(b, 2, uint64(m.BadQuery.Line))
		b = putUint(b, 3, uint64(m.BadQuery.Column))
		b = putString(b, 4, m.BadQuery.Text)
		b = shut(b, at)
	case DetailConflict:
		b, at = open(b, 6)
		b = putUint(b, 1, uint64(m.Attempts))
		b = shut(b, at)
	case DetailWait:
		b, at = open(b, 7)
		b = putUint(b, 1, uint64(m.WaitMS))
		b = shut(b, at)
	}
	return b
}

func (m *Error) decode(d *decoder) {
	*m = Error{}
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Code = d.u32(how)
		case 2:
			m.Message = d.text(how)
		case 3, 4, 5, 6, 7:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			// One detail at a time: the last one read is the one that stays.
			m.Detail = uint8(field)
			m.Duplicate, m.BadDocument, m.BadQuery, m.Attempts, m.WaitMS = Duplicate{}, BadDocument{}, BadQuery{}, 0, 0
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch m.Detail {
				case DetailDuplicate:
					switch f {
					case 1:
						m.Duplicate.Collection = body.text(h)
					case 2:
						m.Duplicate.Index = body.text(h)
					case 3:
						m.Duplicate.Holder = body.text(h)
					default:
						body.skip(h)
					}
				case DetailDocument:
					switch f {
					case 1:
						m.BadDocument.Reason = body.text(h)
					case 2:
						m.BadDocument.Offset = body.i32(h)
					default:
						body.skip(h)
					}
				case DetailQuery:
					switch f {
					case 1:
						m.BadQuery.Reason = body.text(h)
					case 2:
						m.BadQuery.Line = body.u32(h)
					case 3:
						m.BadQuery.Column = body.u32(h)
					case 4:
						m.BadQuery.Text = body.text(h)
					default:
						body.skip(h)
					}
				case DetailConflict:
					if f == 1 {
						m.Attempts = body.u32(h)
					} else {
						body.skip(h)
					}
				case DetailWait:
					if f == 1 {
						m.WaitMS = body.u32(h)
					} else {
						body.skip(h)
					}
				}
			}
			d.bad = d.bad || body.bad
		default:
			d.skip(how)
		}
	}
}

// put writes the refusal as the field, written always: it is an element of a list, or the
// member that says the import stopped.
func (m *Refusal) put(b []byte, field int) []byte {
	var at, in int
	b, at = open(b, field)
	b = putUint(b, 1, uint64(m.Line))
	b, in = open(b, 2)
	b = shut(m.Error.append(b), in)
	return shut(b, at)
}

func (m *Refusal) decode(d *decoder) {
	*m = Refusal{}
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			m.Line = int64(d.uint(how))
		case 2:
			in, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Error.decode(&in)
			d.bad = d.bad || in.bad
		default:
			d.skip(how)
		}
	}
}

func (m *Stats) append(b []byte) []byte {
	for i := range m.Collections {
		c := &m.Collections[i]
		var at int
		b, at = open(b, 1)
		b = putString(b, 1, c.Name)
		b = putUint(b, 2, uint64(c.Documents))
		for j := range c.Indexes {
			var one int
			b, one = open(b, 3)
			b = c.Indexes[j].Index.put(b, 1)
			b = putBool(b, 2, c.Indexes[j].Building)
			b = putBool(b, 3, c.Indexes[j].Rebuilding)
			b = putBool(b, 4, c.Indexes[j].Stale)
			b = shut(b, one)
		}
		b = shut(b, at)
	}
	b = putUint(b, 2, uint64(m.DiskBytes))
	// The five parts that follow are written always, empty or not.
	var at int
	b, at = open(b, 3)
	c := &m.Cache
	b = putUint(b, 1, uint64(c.Budget))
	b = putUint(b, 2, uint64(c.Used))
	b = putUint(b, 3, c.Hits)
	b = putUint(b, 4, c.Misses)
	b = putUint(b, 5, c.Evictions)
	b = shut(b, at)
	b, at = open(b, 4)
	e := &m.Engine
	b = putUint(b, 1, uint64(e.CacheBudget))
	b = putUint(b, 2, uint64(e.CacheUsed))
	b = putUint(b, 3, uint64(e.CacheHits))
	b = putUint(b, 4, uint64(e.CacheMisses))
	b = putUint(b, 5, uint64(e.Buffered))
	b = shut(b, at)
	b, at = open(b, 5)
	k := &m.Commits
	b = putUint(b, 1, uint64(k.Remembered))
	b = putUint(b, 2, uint64(k.Bytes))
	b = putUint(b, 3, uint64(k.Kept))
	b = putUint(b, 4, uint64(k.KeptUsed))
	b = putUint(b, 5, uint64(k.KeptBudget))
	b = putUint(b, 6, k.Expired)
	b = shut(b, at)
	b, at = open(b, 6)
	t := &m.Transactions
	b = putUint(b, 1, uint64(t.Open))
	b = putUint(b, 2, uint64(t.Writing))
	b = putUint(b, 3, uint64(t.Limit))
	b = putUint(b, 4, t.Refused)
	b = shut(b, at)
	b, at = open(b, 7)
	s := &m.Sorts
	b = putUint(b, 1, s.InMemory)
	b = putUint(b, 2, s.OnDisk)
	b = putUint(b, 3, uint64(s.Each))
	b = putUint(b, 4, uint64(s.Budget))
	b = putUint(b, 5, uint64(s.Used))
	return shut(b, at)
}

func (m *Stats) decode(d *decoder) {
	*m = Stats{Collections: m.Collections[:0]}
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			if !d.take(unsafe.Sizeof(CollectionStats{})) {
				continue
			}
			m.Collections = grow(m.Collections)
			c := &m.Collections[len(m.Collections)-1]
			*c = CollectionStats{Indexes: c.Indexes[:0]}
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					c.Name = body.text(h)
				case 2:
					c.Documents = int64(body.uint(h))
				case 3:
					one, ok := body.inner(h)
					if !ok {
						continue
					}
					if !body.take(unsafe.Sizeof(IndexStats{})) {
						continue
					}
					c.Indexes = grow(c.Indexes)
					x := &c.Indexes[len(c.Indexes)-1]
					*x = IndexStats{Index: Index{Fields: x.Index.Fields[:0]}}
					for {
						f, h, ok := one.next()
						if !ok {
							break
						}
						switch f {
						case 1:
							in, ok := one.inner(h)
							if !ok {
								continue
							}
							x.Index = Index{Fields: x.Index.Fields[:0]}
							x.Index.decode(&in)
							one.bad = one.bad || in.bad
						case 2:
							x.Building = one.bool(h)
						case 3:
							x.Rebuilding = one.bool(h)
						case 4:
							x.Stale = one.bool(h)
						default:
							one.skip(h)
						}
					}
					body.bad = body.bad || one.bad
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		case 2:
			m.DiskBytes = int64(d.uint(how))
		case 3, 4, 5, 6, 7:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			v := body.numbers()
			d.bad = d.bad || body.bad
			switch field {
			case 3:
				m.Cache = CacheStats{int64(v[0]), int64(v[1]), v[2], v[3], v[4]}
			case 4:
				m.Engine = EngineStats{int64(v[0]), int64(v[1]), int64(v[2]), int64(v[3]), int64(v[4])}
			case 5:
				m.Commits = CommitStats{int64(v[0]), int64(v[1]), int64(v[2]), int64(v[3]), int64(v[4]), v[5]}
			case 6:
				m.Transactions = TxStats{int64(v[0]), int64(v[1]), int64(v[2]), v[3]}
			case 7:
				m.Sorts = SortStats{v[0], v[1], int64(v[2]), int64(v[3]), int64(v[4])}
			}
		default:
			d.skip(how)
		}
	}
}

// Hello is the first message of a connection, from the client.
type Hello struct {
	// ProtocolMin and ProtocolMax are the versions of the protocol the client speaks.
	ProtocolMin uint32
	ProtocolMax uint32
	Driver      string
	Program     string
	// HasPassword says the login is a password, which User and Password hold. Without it the
	// certificate of the client is its identity.
	HasPassword bool
	User        string
	Password    string
}

// Welcome is the answer to Hello.
type Welcome struct {
	Protocol      uint32
	ServerVersion string
	Role          uint8
	MaxFrame      uint32
	MaxMessage    uint64
	IdleSeconds   uint32
	MaxCursors    uint32
}

// Append appends the message to b.
func (m *Hello) Append(b []byte) []byte {
	b = putUint(b, 1, uint64(m.ProtocolMin))
	b = putUint(b, 2, uint64(m.ProtocolMax))
	b = putString(b, 3, m.Driver)
	b = putString(b, 4, m.Program)
	if m.HasPassword {
		var at int
		b, at = open(b, 5)
		b = putString(b, 1, m.User)
		b = putString(b, 2, m.Password)
		b = shut(b, at)
	}
	return b
}

// Decode reads the message from b and reports whether b is one.
func (m *Hello) Decode(b []byte) bool {
	*m = Hello{}
	d := decoder{b: b}
	for {
		field, how, ok := d.next()
		if !ok {
			return !d.bad
		}
		switch field {
		case 1:
			m.ProtocolMin = d.u32(how)
		case 2:
			m.ProtocolMax = d.u32(how)
		case 3:
			m.Driver = d.text(how)
		case 4:
			m.Program = d.text(how)
		case 5:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.HasPassword, m.User, m.Password = true, "", ""
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					m.User = body.text(h)
				case 2:
					m.Password = body.text(h)
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		default:
			d.skip(how)
		}
	}
}

// Append appends the message to b.
func (m *Welcome) Append(b []byte) []byte {
	b = putUint(b, 1, uint64(m.Protocol))
	b = putString(b, 2, m.ServerVersion)
	b = putUint(b, 3, uint64(m.Role))
	b = putUint(b, 4, uint64(m.MaxFrame))
	b = putUint(b, 5, m.MaxMessage)
	b = putUint(b, 6, uint64(m.IdleSeconds))
	return putUint(b, 7, uint64(m.MaxCursors))
}

// Decode reads the message from b and reports whether b is one.
func (m *Welcome) Decode(b []byte) bool {
	*m = Welcome{}
	d := decoder{b: b}
	for {
		field, how, ok := d.next()
		if !ok {
			return !d.bad
		}
		switch field {
		case 1:
			m.Protocol = d.u32(how)
		case 2:
			m.ServerVersion = d.text(how)
		case 3:
			m.Role = d.small(how)
		case 4:
			m.MaxFrame = d.u32(how)
		case 5:
			m.MaxMessage = d.uint(how)
		case 6:
			m.IdleSeconds = d.u32(how)
		case 7:
			m.MaxCursors = d.u32(how)
		default:
			d.skip(how)
		}
	}
}

func (m *Document) size() int {
	n := 0
	if m.ID != "" {
		n += stringSize(1, len(m.ID))
	}
	if len(m.JSON) != 0 {
		n += stringSize(2, len(m.JSON))
	}
	if m.Revision != 0 {
		n += uintSize(3, m.Revision)
	}
	if m.Modified != 0 {
		n += 9
	}
	if m.Expires != 0 {
		n += 9
	}
	if len(m.Meta) != 0 {
		n += stringSize(6, len(m.Meta))
	}
	if len(m.Stamp) != 0 {
		n += stringSize(7, len(m.Stamp))
	}
	return n
}

func (m *Document) append(b []byte) []byte {
	b = putString(b, 1, m.ID)
	b = putBytes(b, 2, m.JSON)
	b = putUint(b, 3, m.Revision)
	b = putFixed64(b, 4, uint64(m.Modified))
	b = putFixed64(b, 5, uint64(m.Expires))
	b = putBytes(b, 6, m.Meta)
	return putBytes(b, 7, m.Stamp)
}

func (m *Document) decode(d *decoder, alias bool) {
	for {
		field, how, ok := d.next()
		if !ok {
			return
		}
		switch field {
		case 1:
			if alias {
				m.ID = d.lying(how)
			} else {
				m.ID = d.text(how)
			}
		case 2:
			m.JSON = d.bytes(how)
		case 3:
			m.Revision = d.uint(how)
		case 4:
			m.Modified = int64(d.fixed64(how))
		case 5:
			m.Expires = int64(d.fixed64(how))
		case 6:
			m.Meta = d.bytes(how)
		case 7:
			m.Stamp = d.bytes(how)
		default:
			d.skip(how)
		}
	}
}

func (m *Batch) size() int {
	n := 0
	for i := range m.Documents {
		size := m.Documents[i].size()
		n += 1 + varintSize(uint64(size)) + size
	}
	for _, id := range m.IDs {
		n += stringSize(2, len(id))
	}
	if m.Cursor != 0 {
		n += uintSize(3, m.Cursor)
	}
	if m.NextPage != "" {
		n += stringSize(4, len(m.NextPage))
	}
	return n
}

// Append appends the response to b.
func (m *Response) Append(b []byte) []byte {
	switch m.Result {
	case ResultError:
		var at int
		b, at = open(b, 1)
		return shut(m.Error.append(b), at)
	case ResultDone:
		return append(appendTag(b, 2, asBytes), 0)
	case ResultDocument:
		// A document may be large: its size is worked out first, so that it is written where it
		// stays.
		size := m.Document.size()
		b = appendVarint(appendTag(slices.Grow(b, size+8), 3, asBytes), uint64(size))
		return m.Document.append(b)
	case ResultFound:
		return setBool(b, 4, m.Found)
	case ResultNumber:
		return setUint(b, 5, uint64(m.Number))
	case ResultStored:
		var at int
		b, at = open(b, 6)
		for _, id := range m.Stored {
			b = setString(b, 1, id)
		}
		return shut(b, at)
	case ResultIndexes:
		var at int
		b, at = open(b, 9)
		for i := range m.Indexes {
			b = m.Indexes[i].put(b, 1)
		}
		return shut(b, at)
	case ResultNames:
		var at int
		b, at = open(b, 10)
		for _, name := range m.Names {
			b = setString(b, 1, name)
		}
		return shut(b, at)
	case ResultStats:
		var at int
		b, at = open(b, 11)
		return shut(m.Stats.append(b), at)
	case ResultUsers:
		var at int
		b, at = open(b, 12)
		for i := range m.Users {
			var one int
			b, one = open(b, 1)
			b = putString(b, 1, m.Users[i].Name)
			b = putUint(b, 2, uint64(m.Users[i].Role))
			b = shut(b, one)
		}
		return shut(b, at)
	case ResultIdentities:
		var at int
		b, at = open(b, 13)
		for i := range m.Identities {
			var one int
			b, one = open(b, 1)
			b = putString(b, 1, m.Identities[i].Name)
			b = putUint(b, 2, uint64(m.Identities[i].Role))
			b = putString(b, 3, m.Identities[i].Fingerprint)
			b = shut(b, one)
		}
		return shut(b, at)
	case ResultStream:
		var at int
		b, at = open(b, 14)
		b = putBytes(b, 1, m.Stream.Data)
		b = putUint(b, 2, m.Stream.Cursor)
		return shut(b, at)
	case ResultReport:
		var at int
		b, at = open(b, 15)
		b = putUint(b, 1, uint64(m.Report.Lines))
		b = putUint(b, 2, uint64(m.Report.Documents))
		b = putUint(b, 3, uint64(m.Report.Indexes))
		b = putUint(b, 4, uint64(m.Report.Refused))
		for i := range m.Report.Refusals {
			b = m.Report.Refusals[i].put(b, 5)
		}
		if m.Report.Stopped.Line != 0 {
			b = m.Report.Stopped.put(b, 6)
		}
		return shut(b, at)
	case ResultKept:
		var at int
		b, at = open(b, 16)
		for i := range m.Kept {
			var one int
			b, one = open(b, 1)
			b = putUint(b, 1, m.Kept[i].ID)
			b = putString(b, 2, m.Kept[i].Name)
			b = putFixed64(b, 3, uint64(m.Kept[i].Created))
			b = putFixed64(b, 4, uint64(m.Kept[i].At))
			b = putFixed64(b, 5, uint64(m.Kept[i].Until))
			b = putUint(b, 6, uint64(m.Kept[i].Bytes))
			b = shut(b, one)
		}
		return shut(b, at)
	case ResultDatabases:
		var at int
		b, at = open(b, 17)
		for i := range m.Databases {
			var one int
			b, one = open(b, 1)
			b = putString(b, 1, m.Databases[i].Name)
			b = putFixed64(b, 2, uint64(m.Databases[i].Created))
			b = putUint(b, 3, uint64(m.Databases[i].Bytes))
			b = shut(b, one)
		}
		return shut(b, at)
	case ResultBatch:
		// And so is a batch.
		size := m.Batch.size()
		b = appendVarint(appendTag(slices.Grow(b, size+8), 7, asBytes), uint64(size))
		for i := range m.Batch.Documents {
			b = appendVarint(appendTag(b, 1, asBytes), uint64(m.Batch.Documents[i].size()))
			b = m.Batch.Documents[i].append(b)
		}
		for _, id := range m.Batch.IDs {
			b = setString(b, 2, id)
		}
		b = putUint(b, 3, m.Batch.Cursor)
		return putString(b, 4, m.Batch.NextPage)
	case ResultText:
		return setString(b, 8, m.Text)
	}
	return b
}

// A batch written a document at a time.
//
// A server finds the documents of an answer one by one, and what it is handed of each may be
// its to read only until it asks for the next. So it writes each into the answer as it finds
// it, instead of gathering them for Append: BeginBatch, AppendDocument for each, EndBatch. What
// comes of it is, byte for byte, what Append writes for a Response whose batch holds those
// documents and that cursor.

// batchRoom is what BeginBatch leaves free before the documents: the header of a frame, the tag
// of the batch in its Response, and the longest length a batch has.
const batchRoom = HeaderSize + 1 + binary.MaxVarintLen32

// BeginBatch returns buf emptied and ready for AppendDocument.
func BeginBatch(buf []byte) []byte {
	if cap(buf) < batchRoom {
		buf = make([]byte, batchRoom, 1<<10)
	}
	return buf[:batchRoom]
}

// AppendDocument appends a document to a batch begun with BeginBatch.
func AppendDocument(b []byte, d *Document) []byte {
	return d.append(appendVarint(appendTag(b, 1, asBytes), uint64(d.size())))
}

// EndBatch ends a batch begun with BeginBatch. cursor is what the rest of the answer is pulled
// with, or zero when the batch holds all there is. It returns the buffer, to begin the next
// answer in, and the answer as a message for Send, which lies in that buffer.
func EndBatch(b []byte, cursor uint64) (buf, msg []byte) {
	b = putUint(b, 3, cursor)
	size := uint64(len(b) - batchRoom)
	at := batchRoom - varintSize(size)
	binary.PutUvarint(b[at:batchRoom], size)
	at--
	b[at] = byte(ResultBatch)<<3 | asBytes
	return b, b[at-HeaderSize:]
}

// Decode reads the response from b. It reports whether b is a response; an answer this package
// does not know has Result zero.
func (m *Response) Decode(b []byte) bool {
	m.Result = 0
	bud := budget{room: m.Room}
	if bud.room <= 0 {
		bud.room = max(DefaultRoom, 16*len(b))
	}
	d := decoder{b: b, bud: &bud}
	for {
		field, how, ok := d.next()
		if !ok {
			m.Over = bud.over
			return !d.bad
		}
		if field > 255 {
			d.skip(how)
			continue
		}
		switch Result(field) {
		case ResultError:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result = ResultError
			m.Error.decode(&body)
			d.bad = d.bad || body.bad
		case ResultDone:
			if body, ok := d.inner(how); ok {
				m.Result = ResultDone
				body.rest()
				d.bad = d.bad || body.bad
			}
		case ResultDocument:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Document = ResultDocument, Document{}
			m.Document.decode(&body, m.AliasIDs)
			d.bad = d.bad || body.bad
		case ResultFound:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Result, m.Found = ResultFound, d.bool(how)
		case ResultNumber:
			if how != asVarint {
				d.skip(how)
				continue
			}
			m.Result, m.Number = ResultNumber, int64(d.uint(how))
		case ResultBatch:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result = ResultBatch
			m.Batch = Batch{Documents: m.Batch.Documents[:0], IDs: m.Batch.IDs[:0]}
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					one, ok := body.inner(h)
					if !ok {
						continue
					}
					if !body.take(unsafe.Sizeof(Document{})) {
						continue
					}
					m.Batch.Documents = append(m.Batch.Documents, Document{})
					m.Batch.Documents[len(m.Batch.Documents)-1].decode(&one, m.AliasIDs)
					body.bad = body.bad || one.bad
				case 2:
					if h != asBytes {
						body.skip(h)
						continue
					}
					if !body.take(unsafe.Sizeof("")) {
						continue
					}
					m.Batch.IDs = append(m.Batch.IDs, body.text(h))
				case 3:
					m.Batch.Cursor = body.uint(h)
				case 4:
					m.Batch.NextPage = body.text(h)
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		case ResultText:
			if how != asBytes {
				d.skip(how)
				continue
			}
			m.Result, m.Text = ResultText, d.text(how)
		case ResultStored, ResultNames:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			var list []string
			if Result(field) == ResultStored {
				list = m.Stored[:0]
			} else {
				list = m.Names[:0]
			}
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 || h != asBytes {
					body.skip(h)
					continue
				}
				if !body.take(unsafe.Sizeof("")) {
					continue
				}
				list = append(list, body.text(h))
			}
			m.Result = Result(field)
			if m.Result == ResultStored {
				m.Stored = list
			} else {
				m.Names = list
			}
			d.bad = d.bad || body.bad
		case ResultIndexes:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Indexes = ResultIndexes, m.Indexes[:0]
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 {
					body.skip(h)
					continue
				}
				one, ok := body.inner(h)
				if !ok {
					continue
				}
				if !body.take(unsafe.Sizeof(Index{})) {
					continue
				}
				m.Indexes = grow(m.Indexes)
				x := &m.Indexes[len(m.Indexes)-1]
				*x = Index{Fields: x.Fields[:0]}
				x.decode(&one)
				body.bad = body.bad || one.bad
			}
			d.bad = d.bad || body.bad
		case ResultStats:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result = ResultStats
			m.Stats.decode(&body)
			d.bad = d.bad || body.bad
		case ResultUsers:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Users = ResultUsers, m.Users[:0]
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 {
					body.skip(h)
					continue
				}
				one, ok := body.inner(h)
				if !ok {
					continue
				}
				if !body.take(unsafe.Sizeof(User{})) {
					continue
				}
				var u User
				for {
					f, h, ok := one.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						u.Name = one.text(h)
					case 2:
						u.Role = one.small(h)
					default:
						one.skip(h)
					}
				}
				m.Users = append(m.Users, u)
				body.bad = body.bad || one.bad
			}
			d.bad = d.bad || body.bad
		case ResultIdentities:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Identities = ResultIdentities, m.Identities[:0]
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 {
					body.skip(h)
					continue
				}
				one, ok := body.inner(h)
				if !ok {
					continue
				}
				if !body.take(unsafe.Sizeof(Identity{})) {
					continue
				}
				var i Identity
				for {
					f, h, ok := one.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						i.Name = one.text(h)
					case 2:
						i.Role = one.small(h)
					case 3:
						i.Fingerprint = one.text(h)
					default:
						one.skip(h)
					}
				}
				m.Identities = append(m.Identities, i)
				body.bad = body.bad || one.bad
			}
			d.bad = d.bad || body.bad
		case ResultStream:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Stream = ResultStream, Stream{}
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					m.Stream.Data = body.bytes(h)
				case 2:
					m.Stream.Cursor = body.uint(h)
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		case ResultReport:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result = ResultReport
			m.Report = Report{Refusals: m.Report.Refusals[:0]}
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				switch f {
				case 1:
					m.Report.Lines = int64(body.uint(h))
				case 2:
					m.Report.Documents = int64(body.uint(h))
				case 3:
					m.Report.Indexes = body.u32(h)
				case 4:
					m.Report.Refused = int64(body.uint(h))
				case 5:
					one, ok := body.inner(h)
					if !ok {
						continue
					}
					if !body.take(unsafe.Sizeof(Refusal{})) {
						continue
					}
					var r Refusal
					r.decode(&one)
					m.Report.Refusals = append(m.Report.Refusals, r)
					body.bad = body.bad || one.bad
				case 6:
					one, ok := body.inner(h)
					if !ok {
						continue
					}
					m.Report.Stopped.decode(&one)
					body.bad = body.bad || one.bad
				default:
					body.skip(h)
				}
			}
			d.bad = d.bad || body.bad
		case ResultKept:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Kept = ResultKept, m.Kept[:0]
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 {
					body.skip(h)
					continue
				}
				one, ok := body.inner(h)
				if !ok {
					continue
				}
				if !body.take(unsafe.Sizeof(KeptEntry{})) {
					continue
				}
				var k KeptEntry
				for {
					f, h, ok := one.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						k.ID = one.uint(h)
					case 2:
						k.Name = one.text(h)
					case 3:
						k.Created = int64(one.fixed64(h))
					case 4:
						k.At = int64(one.fixed64(h))
					case 5:
						k.Until = int64(one.fixed64(h))
					case 6:
						k.Bytes = int64(one.uint(h))
					default:
						one.skip(h)
					}
				}
				m.Kept = append(m.Kept, k)
				body.bad = body.bad || one.bad
			}
			d.bad = d.bad || body.bad
		case ResultDatabases:
			body, ok := d.inner(how)
			if !ok {
				continue
			}
			m.Result, m.Databases = ResultDatabases, m.Databases[:0]
			for {
				f, h, ok := body.next()
				if !ok {
					break
				}
				if f != 1 {
					body.skip(h)
					continue
				}
				one, ok := body.inner(h)
				if !ok {
					continue
				}
				if !body.take(unsafe.Sizeof(DatabaseEntry{})) {
					continue
				}
				var e DatabaseEntry
				for {
					f, h, ok := one.next()
					if !ok {
						break
					}
					switch f {
					case 1:
						e.Name = one.text(h)
					case 2:
						e.Created = int64(one.fixed64(h))
					case 3:
						e.Bytes = int64(one.uint(h))
					default:
						one.skip(h)
					}
				}
				m.Databases = append(m.Databases, e)
				body.bad = body.bad || one.bad
			}
			d.bad = d.bad || body.bad
		default:
			d.skip(how)
		}
	}
}
