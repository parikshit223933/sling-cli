package sling

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

// oidAt builds a hex ObjectId from unix seconds and a 16-hex-char tail
// (5 process bytes + 3 counter bytes).
func oidAt(ts uint32, tail string) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], ts)
	return hex.EncodeToString(b[:]) + tail
}

const zeroTail = "0000000000000000"

func TestIsObjectIDHex(t *testing.T) {
	cases := map[string]bool{
		"6a74cc45449c40650f96bd87":  true,
		"6A74CC45449C40650F96BD87":  true,
		"000000000000000000000000":  true,
		"6a74cc45449c40650f96bd8":   false, // 23 chars
		"6a74cc45449c40650f96bd877": false, // 25 chars
		"":                          false,
		"6a74cc45449c40650f96bd8g":  false, // non-hex
		"6a74cc45-49c40650f96bd87":  false,
		"zzzzzzzzzzzzzzzzzzzzzzzz":  false,
	}
	for in, want := range cases {
		assert.Equal(t, want, isObjectIDHex(in), in)
	}
}

func TestObjectIDLowerBound(t *testing.T) {
	const ts = uint32(1793367604)
	id := oidAt(ts, "449c40650f96bd87")

	cases := []struct {
		name string
		buf  time.Duration
		want string
	}{
		{"two hours", 2 * time.Hour, oidAt(ts-7200, zeroTail)},
		{"one second", time.Second, oidAt(ts-1, zeroTail)},
		{"sub-second rounds up", 1500 * time.Millisecond, oidAt(ts-2, zeroTail)},
		{"one nanosecond rounds up", time.Nanosecond, oidAt(ts-1, zeroTail)},
		{"fraction above whole minutes", 59*time.Minute + 59*time.Second + 500*time.Millisecond, oidAt(ts-3600, zeroTail)},
		{"exactly back to the epoch", time.Duration(ts) * time.Second, strings.Repeat("0", 24)},
		{"before the epoch clamps", 1_000_000 * time.Hour, strings.Repeat("0", 24)},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, objectIDLowerBound(id, c.buf), c.name)
	}

	// uppercase input decodes the same and comes back lowercase
	assert.Equal(t, oidAt(ts-7200, zeroTail), objectIDLowerBound(strings.ToUpper(id), 2*time.Hour))
	// largest representable timestamp
	assert.Equal(t, oidAt(0xffffffff-3600, zeroTail), objectIDLowerBound(oidAt(0xffffffff, "ffffffffffffffff"), time.Hour))
}

// TestObjectIDLowerBoundProperties checks, over random ids and buffers, that
// the bound is a lowercase ObjectId with a zero tail, never above the
// checkpoint, at exactly ceil(buf) seconds earlier (floored at the epoch), and
// that every id generated in the bound's second or later sorts above it — the
// guarantee the source's `_id $gt bound` filter relies on.
func TestObjectIDLowerBoundProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(20261030))
	for i := 0; i < 20000; i++ {
		ts := rng.Uint32()
		tail := make([]byte, 8)
		rng.Read(tail)
		id := oidAt(ts, hex.EncodeToString(tail))
		buf := time.Duration(rng.Int63n(int64(10*365*24*time.Hour))) + 1

		bound := objectIDLowerBound(id, buf)
		if !assert.True(t, isObjectIDHex(bound)) || !assert.Equal(t, strings.ToLower(bound), bound) {
			return
		}
		assert.Equal(t, zeroTail, bound[8:])
		assert.LessOrEqual(t, bound, id)

		secs := int64((buf + time.Second - 1) / time.Second)
		wantTS := int64(ts) - secs
		if wantTS < 0 {
			wantTS = 0
		}
		raw, _ := hex.DecodeString(bound)
		if !assert.Equal(t, uint32(wantTS), binary.BigEndian.Uint32(raw[:4]), "id=%s buf=%s", id, buf) {
			return
		}

		// smallest real id in the bound's second sorts above the bound
		assert.Greater(t, oidAt(uint32(wantTS), "0000000000000001"), bound)
	}
}

func TestApplyIncrementalBuffer(t *testing.T) {
	const ts = uint32(1793367604)
	id := oidAt(ts, "449c40650f96bd87")
	at := time.Date(2026, 10, 30, 13, 30, 0, 0, time.UTC)

	strCol := iop.Column{Name: "_id", Type: iop.StringType}
	textCol := iop.Column{Name: "_id", Type: iop.TextType}
	tsCol := iop.Column{Name: "updated_at", Type: iop.TimestampType}
	dateCol := iop.Column{Name: "day", Type: iop.DateType}
	intCol := iop.Column{Name: "id", Type: iop.BigIntType}
	decCol := iop.Column{Name: "amount", Type: iop.DecimalType}

	mongo, postgres := dbio.TypeDbMongoDB, dbio.TypeDbPostgres

	cases := []struct {
		name string
		src  dbio.Type
		col  iop.Column
		buf  string // "-" means no source options at all
		in   any
		want any
	}{
		// Mongo ObjectId keys (new)
		{"objectid 2h", mongo, strCol, "2h", id, oidAt(ts-7200, zeroTail)},
		{"objectid text column", mongo, textCol, "2h", id, oidAt(ts-7200, zeroTail)},
		{"objectid uppercase checkpoint", mongo, strCol, "2h", strings.ToUpper(id), oidAt(ts-7200, zeroTail)},
		{"objectid sub-second buffer rounds up", mongo, strCol, "1500ms", id, oidAt(ts-2, zeroTail)},
		{"objectid buffer older than epoch", mongo, strCol, "1000000h", id, strings.Repeat("0", 24)},
		{"objectid invalid duration", mongo, strCol, "abc", id, id},
		{"objectid integer-style buffer", mongo, strCol, "10000", id, id},
		{"objectid negative", mongo, strCol, "-2h", id, id},
		{"objectid zero", mongo, strCol, "0s", id, id},
		{"objectid-shaped text from postgres", postgres, strCol, "2h", id, id},
		{"non-objectid string on mongo", mongo, strCol, "2h", "doc-0001", "doc-0001"},
		{"23-char hex on mongo", mongo, strCol, "2h", id[:23], id[:23]},
		{"no source options", mongo, strCol, "-", id, id},
		{"empty buffer", mongo, strCol, "", id, id},
		{"empty target", mongo, strCol, "2h", nil, nil},

		// datetime / date keys (moved verbatim, must not change)
		{"datetime 1h", mongo, tsCol, "1h", at, at.Add(-time.Hour)},
		{"datetime 1h on postgres", postgres, tsCol, "1h", at, at.Add(-time.Hour)},
		{"datetime missing unit", postgres, tsCol, "10", at, at},
		{"datetime negative", postgres, tsCol, "-1h", at, at},
		{"datetime zero", postgres, tsCol, "0s", at, at},
		{"date 24h", postgres, dateCol, "24h", at, at.Add(-24 * time.Hour)},

		// integer keys (moved verbatim, must not change)
		{"integer 10", postgres, intCol, "10", 100, int64(90)},
		{"integer clamps at 0", postgres, intCol, "500", 100, int64(0)},
		{"integer on mongo", mongo, intCol, " 10 ", int64(100), int64(90)},
		{"integer duration buffer", postgres, intCol, "1h", 100, 100},
		{"integer negative", postgres, intCol, "-5", 100, 100},

		// other types are ignored
		{"decimal ignored", postgres, decCol, "10", 1.5, 1.5},
	}

	for _, c := range cases {
		cfg := &Config{}
		if c.buf != "-" {
			cfg.Source.Options = &SourceOptions{IncrementalBuffer: c.buf}
		}
		cfg.IncrementalVal = c.in
		applyIncrementalBuffer(cfg, c.col, c.src, c.col.Name)
		assert.Equal(t, c.want, cfg.IncrementalVal, c.name)
	}
}

// The buffer runs once per stream run; these show it costs well under a
// microsecond and a handful of small allocations.
func BenchmarkObjectIDLowerBound(b *testing.B) {
	id := oidAt(1793367604, "449c40650f96bd87")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = objectIDLowerBound(id, 2*time.Hour)
	}
}

func BenchmarkApplyIncrementalBufferObjectID(b *testing.B) {
	id := oidAt(1793367604, "449c40650f96bd87")
	col := iop.Column{Name: "_id", Type: iop.StringType}
	cfg := &Config{}
	cfg.Source.Options = &SourceOptions{IncrementalBuffer: "2h"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cfg.IncrementalVal = id
		applyIncrementalBuffer(cfg, col, dbio.TypeDbMongoDB, "_id")
	}
}
