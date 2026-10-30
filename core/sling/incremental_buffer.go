package sling

import (
	"encoding/binary"
	"encoding/hex"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// applyIncrementalBuffer widens the incremental lookback window to tolerate the
// snapshot-isolation + replica-lag race where a row updated during Sling's scan
// can be permanently skipped because a neighboring row with higher update_key
// advances the watermark past the stuck row's new value. It rewrites
// cfg.IncrementalVal (the target's max(update_key)) in place:
//   - datetime/date keys: subtract a duration, e.g. "1h"
//   - integer keys: subtract an id/row count, e.g. "10000"
//   - Mongo ObjectId keys (`update_key: _id`): subtract a duration from the
//     ObjectId's embedded creation timestamp, e.g. "2h"
//
// It only applies when the target already has data (cfg.IncrementalVal != nil),
// and it runs once per stream run, so its cost does not depend on row counts.
func applyIncrementalBuffer(cfg *Config, maxCol iop.Column, srcConnType dbio.Type, tgtUpdateKey string) {
	if cfg.Source.Options == nil || cfg.Source.Options.IncrementalBuffer == "" || cfg.IncrementalVal == nil {
		return
	}

	if maxCol.IsDatetime() || maxCol.Type.IsDate() {
		bufStr := cfg.Source.Options.IncrementalBuffer
		buf, bufErr := time.ParseDuration(bufStr)
		switch {
		case bufErr != nil:
			g.Warn("incremental_buffer %q is not a valid Go duration: %v (skipping buffer, full precision used)", bufStr, bufErr)
		case buf < 0:
			g.Warn("incremental_buffer %q is negative (skipping buffer)", bufStr)
		case buf == 0:
			// explicit no-op — do nothing
		default:
			t := cast.ToTime(cfg.IncrementalVal)
			if t.IsZero() {
				g.Warn("incremental_buffer: could not cast incremental value %v (type %T) to time.Time (skipping buffer)", cfg.IncrementalVal, cfg.IncrementalVal)
			} else {
				shifted := t.Add(-buf)
				g.Debug("incremental_buffer=%s applied: %s -> %s (update_key=%s)", bufStr, t.Format(time.RFC3339Nano), shifted.Format(time.RFC3339Nano), tgtUpdateKey)
				cfg.IncrementalVal = shifted
			}
		}
	} else if maxCol.Type.IsInteger() {
		// Integer (e.g. primary-key `id`) update keys have an analogous
		// out-of-order-commit race: a row whose transaction commits *after*
		// a higher id has already advanced the watermark is stranded
		// (the next scan filters `id > watermark` and never sees it).
		// Interpret incremental_buffer as a row/id count and subtract it
		// from the integer watermark so the next scan re-reads that safety
		// margin. The DELETE+INSERT merge is idempotent, so re-reading is a
		// no-op for the unchanged rows already in the target.
		bufStr := strings.TrimSpace(cfg.Source.Options.IncrementalBuffer)
		buf, bufErr := cast.ToInt64E(bufStr)
		switch {
		case bufErr != nil:
			g.Warn("incremental_buffer %q is not a valid integer for integer update_key %q (skipping buffer, full precision used)", bufStr, tgtUpdateKey)
		case buf < 0:
			g.Warn("incremental_buffer %q is negative (skipping buffer)", bufStr)
		case buf == 0:
			// explicit no-op — do nothing
		default:
			cur := cast.ToInt64(cfg.IncrementalVal)
			shifted := cur - buf
			if shifted < 0 {
				shifted = 0
			}
			g.Debug("incremental_buffer=%s applied (integer): %d -> %d (update_key=%s)", bufStr, cur, shifted, tgtUpdateKey)
			cfg.IncrementalVal = shifted
		}
	} else if oidHex, ok := objectIDCheckpoint(srcConnType, maxCol, cfg.IncrementalVal); ok {
		// Mongo `_id` keys. ObjectIds are generated client-side as
		// <4-byte unix seconds><5-byte per-process random><3-byte counter>,
		// so two app processes writing in the same second produce ids that
		// sort by their random bytes, not by commit order: a later insert can
		// land BELOW an `_id` that already advanced the watermark, and the next
		// `_id > watermark` scan never sees it. Rewind the watermark by the
		// buffer using the timestamp the ObjectId carries. The bound still
		// filters on `_id`, so the source keeps using its built-in `_id` index.
		bufStr := cfg.Source.Options.IncrementalBuffer
		buf, bufErr := time.ParseDuration(bufStr)
		switch {
		case bufErr != nil:
			g.Warn("incremental_buffer %q is not a valid Go duration: %v (skipping buffer, full precision used)", bufStr, bufErr)
		case buf < 0:
			g.Warn("incremental_buffer %q is negative (skipping buffer)", bufStr)
		case buf == 0:
			// explicit no-op — do nothing
		default:
			shifted := objectIDLowerBound(oidHex, buf)
			g.Debug("incremental_buffer=%s applied (ObjectId): %s -> %s (update_key=%s)", bufStr, oidHex, shifted, tgtUpdateKey)
			cfg.IncrementalVal = shifted
		}
	} else {
		g.Debug("incremental_buffer=%s ignored: update_key %q has unsupported type %q", cfg.Source.Options.IncrementalBuffer, tgtUpdateKey, maxCol.Type)
	}
}

// objectIDCheckpoint returns the target's max(update_key) as a lowercase hex
// ObjectId when it can be buffered as one. Only Mongo sources qualify: their
// reader turns a 24-hex checkpoint back into an ObjectID before filtering
// (MongoDBConn.coerceIncrementalFilterValue), so the rewound bound is compared
// as an ObjectId. For any other source a 24-hex string is just text.
func objectIDCheckpoint(srcConnType dbio.Type, maxCol iop.Column, val any) (string, bool) {
	if srcConnType != dbio.TypeDbMongoDB || !maxCol.Type.IsString() {
		return "", false
	}
	s := cast.ToString(val)
	if !isObjectIDHex(s) {
		return "", false
	}
	return strings.ToLower(s), true
}

// isObjectIDHex reports whether s is a 24-character hex string (12-byte ObjectId).
func isObjectIDHex(s string) bool {
	if len(s) != 24 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(('0' <= c && c <= '9') || ('a' <= c && c <= 'f') || ('A' <= c && c <= 'F')) {
			return false
		}
	}
	return true
}

// objectIDLowerBound rewinds the timestamp of a valid hex ObjectId by buf and
// returns the smallest ObjectId of that second (zero process and counter
// bytes), so a `$gt` filter keeps every document generated in that second or
// later. Sub-second buffers round up to a whole second, since ObjectId
// timestamps have one-second resolution and the window must never be narrower
// than configured. A buffer that reaches before the Unix epoch clamps to the
// all-zero ObjectId, i.e. a full re-read.
func objectIDLowerBound(oidHex string, buf time.Duration) string {
	raw, _ := hex.DecodeString(oidHex) // caller validated with isObjectIDHex

	secs := int64(buf / time.Second)
	if buf%time.Second != 0 {
		secs++
	}
	ts := int64(binary.BigEndian.Uint32(raw[:4])) - secs
	if ts < 0 {
		ts = 0
	}

	var bound [12]byte
	binary.BigEndian.PutUint32(bound[:4], uint32(ts))
	return hex.EncodeToString(bound[:])
}
