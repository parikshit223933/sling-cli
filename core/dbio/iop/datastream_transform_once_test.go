package iop

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// Regression tests for transforms being applied more than once to a row, which
// turned a PII column's hmac(md5(v)) into hmac(md5(hmac(md5(v)))) or deeper.
//
// Seen in production on 2026-08-20 as "row 1 of a stream hashes differently on
// about half of runs" (admin.history_trackers, Mongo -> file).
//
// Datastream.Start's push loop evaluates transforms IN PLACE on ds.it.Row inside
// schemaChgLoop, and re-enters that loop for the SAME row (goto schemaChgLoop)
// after a pause or a schema change. Every re-entry evaluated the transforms
// again on a row already holding transformed values. Idempotent transforms hid
// this; hash_md5, hmac_sha256 and hmac_sha256_json are not idempotent.
//
// Row 1 was the usual victim: column discovery during sampling queues schema
// changes that the first pushed row consumes, but only once the dataflow has
// attached (ds.df != nil), which is a race — hence "about half of runs". A
// later row that changes a column's type mid-stream, or that is in flight when
// the dataflow pauses the stream, was hit the same way.

const onceKey = "test-secret-key"

// payloadFor is the JSON blob a source-side SQL scrub emits: the PII value
// already md5'd, keyed in flight by hmac_sha256_json.
func payloadFor(email string) string { return `{"e":"` + g.MD5(email) + `"}` }

// hashedStream returns a stream configured the way the replication configs
// configure PII: `email` through hash_md5 -> hmac_sha256, and `payload` through
// hmac_sha256_json(e). onRead(i, it) runs as row i (0-based) is read, so a test
// can inject a pause, a schema change or a value at an exact row.
func hashedStream(t *testing.T, emails []any, onRead func(i int, it *Iterator)) *Datastream {
	t.Helper()

	stages, err := ParseStageTransforms([]any{
		map[string]any{"email": "hash_md5"},
		map[string]any{"email": "hmac_sha256"},
		map[string]any{"payload": "hmac_sha256_json(e)"},
	})
	if err != nil {
		t.Fatal(err)
	}

	cols := NewColumns(
		Column{Name: "id", Type: BigIntType, Sourced: true},
		Column{Name: "email", Type: StringType, Sourced: true},
		Column{Name: "payload", Type: StringType, Sourced: true},
		Column{Name: "note", Type: StringType, Sourced: true},
	)

	i := 0
	ds := NewDatastreamIt(context.Background(), cols, func(it *Iterator) bool {
		if i >= len(emails) {
			return false
		}
		var payload any
		if email, ok := emails[i].(string); ok {
			payload = payloadFor(email)
		}
		it.Row = []any{int64(i + 1), emails[i], payload, "x"}
		if onRead != nil {
			onRead(i, it)
		}
		i++
		return true
	})
	ds.Sp.Config.Transforms = NewTransform(stages, ds.Sp)
	return ds
}

// assertTransformedOnce collects the stream and checks every row carries
// exactly one application of each transform. NULLs must stay NULL.
func assertTransformedOnce(t *testing.T, ds *Datastream, emails []any) {
	t.Helper()

	// read ds.Rows() directly: Collect() clears ds.Buffer while the stream's
	// own Close() does too, an upstream race unrelated to these tests
	var rows [][]any
	for row := range ds.Rows() {
		rows = append(rows, row)
	}
	if err := ds.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(emails) {
		t.Fatalf("got %d rows, want %d", len(rows), len(emails))
	}

	for i, row := range rows {
		email, ok := emails[i].(string)
		if !ok {
			if row[1] != nil || row[2] != nil {
				t.Errorf("row %d: NULL source became email=%v payload=%v", i+1, row[1], row[2])
			}
			continue
		}

		once := expected(onceKey, g.MD5(email))
		if got := cast.ToString(row[1]); got != once {
			twice := expected(onceKey, g.MD5(once))
			t.Errorf("row %d email = %s, want hmac(md5(v)) = %s (double-applied would be %s)", i+1, got, once, twice)
		}

		wantPayload := `{"e":"` + expected(onceKey, g.MD5(email)) + `"}`
		if got := cast.ToString(row[2]); got != wantPayload {
			t.Errorf("row %d payload = %s, want %s", i+1, got, wantPayload)
		}
	}
}

// Baseline: nothing re-enters the push loop, every row is transformed once.
func TestTransformsAppliedOnce_Baseline(t *testing.T) {
	withKey(t, onceKey)
	emails := []any{"a@example.com", "a@example.com", nil, "b@example.com"}

	ds := hashedStream(t, emails, nil)
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	assertTransformedOnce(t, ds, emails)
}

// The production case. A column discovered while reading queues a schema
// change (what the JSON/Mongo reader does on meeting a new key), and the push
// loop consumes it while row 1 is in hand.
func TestTransformsAppliedOnce_SchemaChange(t *testing.T) {
	withKey(t, onceKey)
	emails := []any{"a@example.com", "a@example.com", nil, "b@example.com"}

	ds := hashedStream(t, emails, func(i int, it *Iterator) {
		if i == 0 {
			it.ds.AddColumns(Columns{{Name: "late_key", Type: StringType}}, false)
		}
	})
	// attached before row 1 is pushed: the half of production runs where
	// Dataflow.PushStreamChan wins the race
	ds.df = NewDataflow()

	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	assertTransformedOnce(t, ds, emails)
}

// The dataflow pausing the stream (it does so before every database write)
// while a row sits between its transforms and its push.
func TestTransformsAppliedOnce_Pause(t *testing.T) {
	withKey(t, onceKey)
	emails := []any{"a@example.com", "a@example.com", nil, "b@example.com"}

	var ds *Datastream
	taken := make(chan struct{})
	ds = hashedStream(t, emails, func(i int, it *Iterator) {
		if i != 0 {
			return
		}
		// Park a pauser on pauseChan before the push loop starts, so the loop's
		// own select takes the pause while row 1 is in hand. Datastream.Pause
		// needs Ready, which is only set after sampling, so drive the channels
		// directly the way TryPause does.
		go func() {
			ds.pauseChan <- struct{}{}
			ds.unpauseChan <- struct{}{}
			close(taken)
		}()
		time.Sleep(20 * time.Millisecond)
	})

	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	assertTransformedOnce(t, ds, emails)

	select {
	case <-taken:
	case <-time.After(5 * time.Second):
		t.Fatal("the pause was never taken; the test did not exercise the re-entry")
	}
}

// Several queued schema changes are consumed one per pass, so before the fix
// the row in hand was transformed once per change plus once: here, 4 times.
// Under flatten:0, every key a Mongo document introduces queues one.
func TestTransformsAppliedOnce_ManySchemaChanges(t *testing.T) {
	withKey(t, onceKey)
	emails := []any{"a@example.com", "b@example.com"}

	ds := hashedStream(t, emails, func(i int, it *Iterator) {
		if i == 0 {
			for _, name := range []string{"k1", "k2", "k3"} {
				it.ds.AddColumns(Columns{{Name: name, Type: StringType}}, false)
			}
		}
	})
	ds.df = NewDataflow()

	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	assertTransformedOnce(t, ds, emails)
}

// A pause taken inside Batch.Push sends the row back through it.Reprocess.
// That row is already transformed and must be pushed as-is, not again.
func TestTransformsAppliedOnce_ReprocessedRow(t *testing.T) {
	withKey(t, onceKey)
	emails := []any{"a@example.com", "a@example.com", "b@example.com"}

	ds := hashedStream(t, emails, nil)
	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}

	// Batch.Rows is unbuffered and nothing reads it yet, so the push loop
	// parks inside Batch.Push with row 1; the pause is taken there.
	time.Sleep(50 * time.Millisecond)
	ds.pauseChan <- struct{}{}
	ds.unpauseChan <- struct{}{}

	assertTransformedOnce(t, ds, emails)
}

// A row that is NOT the first: a value past the sample changes a column's type
// (a >4000-char string turns `note` into text), which queues a schema change
// from inside CastRow, after that row's transforms already ran.
//
// Under -race this test also reports an upstream race between ChangeColumn and
// processBwRows reading the column type. It predates this fix and is not in
// the transform path.
func TestTransformsAppliedOnce_TypeChangeMidStream(t *testing.T) {
	withKey(t, onceKey)
	orig := SampleSize
	SampleSize = 2
	t.Cleanup(func() { SampleSize = orig })

	emails := []any{"a@example.com", "b@example.com", "c@example.com", "a@example.com", nil, "d@example.com"}
	ds := hashedStream(t, emails, func(i int, it *Iterator) {
		if i == 3 {
			it.Row[3] = strings.Repeat("x", 4001)
		}
	})
	ds.df = NewDataflow()

	if err := ds.Start(); err != nil {
		t.Fatal(err)
	}
	assertTransformedOnce(t, ds, emails)
}
