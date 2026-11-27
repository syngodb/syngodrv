package wire

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// Ops lists every request there is: each constant of the kind Op that this package declares,
// once, in the order of their numbers. The constants are read from the source itself, so that a
// request that is added and not listed fails here, and with it everything that goes through
// the list: the server's table of who may ask what.
func TestOpsListsEveryRequest(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "messages.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]int{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			kind, ok := value.Type.(*ast.Ident)
			if !ok || kind.Name != "Op" {
				continue
			}
			for i, name := range value.Names {
				number, err := strconv.Atoi(value.Values[i].(*ast.BasicLit).Value)
				if err != nil {
					t.Fatalf("%s: %v", name.Name, err)
				}
				declared[name.Name] = number
			}
		}
	}
	if len(declared) < 30 {
		t.Fatalf("%d requests were found in the source: the test no longer reads it as it is written", len(declared))
	}
	listed := map[int]bool{}
	last := 0
	for _, op := range Ops() {
		if int(op) <= last {
			t.Errorf("the request %d is listed after %d", op, last)
		}
		last = int(op)
		listed[int(op)] = true
	}
	for name, number := range declared {
		if !listed[number] {
			t.Errorf("%s (%d) is a request, and Ops does not list it", name, number)
		}
	}
	if len(listed) != len(declared) {
		t.Errorf("Ops lists %d requests, and %d are declared", len(listed), len(declared))
	}
	// Each is written and read back as itself: a request the decoder does not know reads as
	// none.
	for _, op := range Ops() {
		var read Request
		if !read.Decode((&Request{Op: op}).Append(nil)) || read.Op != op {
			t.Errorf("the request %d is written and read back as %d", op, read.Op)
		}
	}
}
