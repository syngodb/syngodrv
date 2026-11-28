package wire

import "testing"

// The answers of Phase 6 that are lists (the refusals of a report, what is kept, the databases
// with their details) are decoded inside the room they are given: an answer that describes
// more is refused, and little was taken for it. The requests of an upload are refused when a
// length lies, and a field that is repeated costs nothing and leaves the last one.
func TestAttack6BTheNewMessagesHoldToTheirBound(t *testing.T) {
	const room = 1 << 20
	for name, result := range map[string]Result{"a report": ResultReport, "what is kept": ResultKept, "the databases": ResultDatabases} {
		field := 1
		if result == ResultReport {
			field = 5
		}
		var body []byte
		for i := 0; i < 400_000; i++ {
			body = attackField(body, field, nil)
		}
		msg := attackField(nil, int(result), body)
		resp := &Response{Room: room}
		var ok bool
		allocated, _ := attackAllocated(func() any { ok = resp.Decode(msg); return resp })
		if ok || !resp.Over {
			t.Errorf("%s of 400,000 entries was decoded in a room of 1 MiB: decoded %v, over %v", name, ok, resp.Over)
		}
		if allocated > 8*room {
			t.Errorf("%s of 400,000 entries, in a room of 1 MiB: %d KiB were taken before it was refused", name, allocated>>10)
		}
	}
	// A report that is stopped a hundred thousand times, each time with another reason: the last
	// stays, and what was taken is no more than the message.
	var stops []byte
	for i := 0; i < 100_000; i++ {
		refusal := attackField(appendVarint(appendTag(nil, 1, asVarint), uint64(i+1)), 2, attackField(nil, 2, []byte("why")))
		stops = attackField(stops, 6, refusal)
	}
	msg := attackField(nil, int(ResultReport), stops)
	resp := &Response{Room: room}
	var ok bool
	allocated, _ := attackAllocated(func() any { ok = resp.Decode(msg); return resp })
	if !ok || resp.Report.Stopped.Line != 100_000 || resp.Report.Stopped.Error.Message != "why" {
		t.Errorf("a report that is stopped again and again: decoded %v, %+v", ok, resp.Report.Stopped)
	}
	if allocated > int64(2*len(msg)) {
		t.Errorf("a report of %d KiB that is stopped again and again took %d KiB", len(msg)>>10, allocated>>10)
	}
	// A length that lies.
	lying := func(op Op, field int) []byte {
		inner := append(appendVarint(appendTag(nil, field, asBytes), 1<<30), "short"...)
		return attackField(nil, int(op), inner)
	}
	for name, msg := range map[string][]byte{"a part": lying(OpPart, 1), "a restore": lying(OpRestore, 2), "an import": lying(OpImport, 1), "a measure": lying(OpMeasure, 1)} {
		req := &Request{}
		if req.Decode(msg) {
			t.Errorf("%s whose length lies was decoded: %+v", name, req)
		}
	}
	// A part that is given a hundred thousand times.
	var parts []byte
	for i := 0; i < 100_000; i++ {
		parts = attackField(parts, 1, []byte("abcdefgh"))
	}
	parts = attackField(parts, 1, []byte("the last"))
	msg = attackField(nil, int(OpPart), parts)
	req := &Request{}
	allocated, _ = attackAllocated(func() any { ok = req.Decode(msg); return req })
	if !ok || req.Op != OpPart || string(req.Part) != "the last" {
		t.Errorf("a part that is given again and again: decoded %v, %q", ok, req.Part)
	}
	if allocated > 64<<10 {
		t.Errorf("a part that is given again and again took %d KiB", allocated>>10)
	}
}
