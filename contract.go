package syngodrv

import "github.com/syngodb/syngo"

// The driver keeps the contracts of the public package, as the embedded library does. The
// compiler checks it on these four lines.
var (
	_ syngo.DB[*Collection, *Tx] = (*DB)(nil)
	_ syngo.Collection[*Query]   = (*Collection)(nil)
	_ syngo.Query[*Query]        = (*Query)(nil)
	_ syngo.Tx[*Collection]      = (*Tx)(nil)
)
