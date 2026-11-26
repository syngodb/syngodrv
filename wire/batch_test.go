package wire

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// A batch written a document at a time is, byte for byte, the batch Append writes: whatever the
// number of documents, their sizes, and the cursor.
func TestABatchWrittenADocumentAtATimeIsTheBatchOfAppend(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	text := func(most int) []byte {
		b := make([]byte, rng.IntN(most+1))
		for i := range b {
			b[i] = byte('a' + rng.IntN(26))
		}
		return b
	}
	// The sizes on both sides of every length a batch can have: one byte, two, three, four.
	sizes := []int{0, 1, 2, 3, 120, 127, 128, 129, 16_000, 16_383, 16_384, 16_390, 300_000, 2_097_151, 2_097_152, 2_097_200, 3_000_000}
	var buf []byte
	for round, size := range sizes {
		for _, cursor := range []uint64{0, 1, 1 << 40} {
			var docs []Document
			for left := size; left > 0; {
				d := Document{ID: string(text(40)), JSON: text(min(left, 700)), Revision: rng.Uint64N(1000)}
				if rng.IntN(2) == 0 {
					d.Modified, d.Expires = rng.Int64(), rng.Int64N(3)
				}
				if rng.IntN(3) == 0 {
					d.Meta = text(30)
				}
				if rng.IntN(5) == 0 {
					d.Stamp = text(8)
				}
				docs = append(docs, d)
				left -= d.size() + 3
			}
			resp := Response{Result: ResultBatch, Batch: Batch{Documents: docs, Cursor: cursor}}
			want := resp.Append(Begin(nil))

			// The buffer of the answer before is used again, as a session does.
			buf = BeginBatch(buf)
			for i := range docs {
				buf = AppendDocument(buf, &docs[i])
			}
			var got []byte
			buf, got = EndBatch(buf, cursor)
			if !bytes.Equal(got[HeaderSize:], want[HeaderSize:]) {
				t.Fatalf("round %d, %d documents, cursor %d: %d bytes written a document at a time, %d by Append, and they differ", round, len(docs), cursor, len(got), len(want))
			}
			// And it is read as the same answer.
			var back Response
			if !back.Decode(got[HeaderSize:]) || back.Result != ResultBatch || len(back.Batch.Documents) != len(docs) || back.Batch.Cursor != cursor {
				t.Fatalf("round %d: what was written a document at a time reads as %d documents and cursor %d", round, len(back.Batch.Documents), back.Batch.Cursor)
			}
			for i := range docs {
				if back.Batch.Documents[i].ID != docs[i].ID || !bytes.Equal(back.Batch.Documents[i].JSON, docs[i].JSON) || back.Batch.Documents[i].Revision != docs[i].Revision {
					t.Fatalf("round %d: document %d reads as another", round, i)
				}
			}
			// It goes out as any message does.
			var sent bytes.Buffer
			if err := Send(&sent, MaxFrame, KindResponse, 9, got); err != nil {
				t.Fatal(err)
			}
			r := NewReader(&sent)
			r.Limits(MaxFrame, 8<<20)
			kind, number, payload, err := r.Next()
			if err != nil || kind != KindResponse || number != 9 || !bytes.Equal(payload, want[HeaderSize:]) {
				t.Fatalf("round %d: sent and received, the batch is kind %d, number %d, %d bytes, %v", round, kind, number, len(payload), err)
			}
		}
	}
}
