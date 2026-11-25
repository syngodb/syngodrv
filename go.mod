module github.com/syngodb/syngodrv

go 1.26

require github.com/syngodb/syngo v0.0.0

require github.com/goccy/go-json v0.12.0 // indirect

// Until the public package has its first tag (v1.0.0, at the end of Phase 3), it is the clone
// beside this one.
replace github.com/syngodb/syngo => ../syngo
