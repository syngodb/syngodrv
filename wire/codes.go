package wire

// The numbers of the enumerations of syngo.proto.

// Code says why a request was not done. The first fifteen are the errors of the public package,
// one for one.
const (
	CodeNotFound        uint32 = 1
	CodeExists          uint32 = 2
	CodeDuplicate       uint32 = 3
	CodeConflict        uint32 = 4
	CodeReadOnly        uint32 = 5
	CodeTxDone          uint32 = 6
	CodeTxExpired       uint32 = 7
	CodeClosed          uint32 = 8
	CodeInvalidDocument uint32 = 9
	CodeInvalidQuery    uint32 = 10
	CodeInvalidIndex    uint32 = 11
	CodeInvalidName     uint32 = 12
	CodeFailed          uint32 = 13
	CodeTooLarge        uint32 = 14
	CodeCorrupt         uint32 = 15
)

// What only a server says.
const (
	CodeUnauthenticated uint32 = 30
	CodeForbidden       uint32 = 31
	CodeNoDatabase      uint32 = 32
	CodeStale           uint32 = 33
	CodeBusy            uint32 = 34
	CodeCancelled       uint32 = 35
	CodeUnsupported     uint32 = 36
	CodeProtocol        uint32 = 37
	CodeInternal        uint32 = 38
)

// The roles of a session.
const (
	RoleNone  uint8 = 0
	RoleRead  uint8 = 1
	RoleWrite uint8 = 2
	RoleAdmin uint8 = 3
)

// The modes of Put.
const (
	WritePut     uint8 = 0
	WriteInsert  uint8 = 1
	WriteReplace uint8 = 2
)

// The states of Write.Expiry.
const (
	ExpiryUnstated uint8 = 0
	ExpiryNever    uint8 = 1
	ExpiryIn       uint8 = 2
	ExpiryAt       uint8 = 3
)

// The states of Write.Meta.
const (
	MetaUnstated uint8 = 0
	MetaNone     uint8 = 1
	MetaSet      uint8 = 2
)

// The details of an Error, by the number of the field that holds them.
const (
	DetailNone      uint8 = 0
	DetailDuplicate uint8 = 3
	DetailDocument  uint8 = 4
	DetailQuery     uint8 = 5
	DetailConflict  uint8 = 6
	DetailWait      uint8 = 7
)
