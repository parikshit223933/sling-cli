# incremental_buffer on Mongo ObjectId `_id` keys — integration tests

End-to-end proof for `source_options.incremental_buffer` on a Mongo
`update_key: _id`, and that nothing else changed. Every scenario runs the
**old** binary (fork `main`, the code in production) and the **new** binary
(this branch) against the same Mongo data, each into its own ClickHouse
database.

## Why this exists

The ops repo's docdb stream `sales_call_transcription_contents` moves from
`update_key: updated_at` to `update_key: _id`. The collection has no
`updated_at` index, so the old key meant a full collection scan every hour
(~2.6 M docs / ~12 GiB, ~10 min a run). `_id` uses the built-in index.

`_id > watermark` alone loses documents, though. ObjectIds are generated
client-side as `<4-byte unix seconds><5-byte per-process random><3-byte counter>`,
so two app processes writing in the same second produce ids that sort by their
random bytes, not by commit order. A document can land *below* an `_id` that
already advanced the watermark, and the next scan never sees it. In production
27% of documents arrive below an already-synced `_id` (never more than 1 s
back). Before this branch the fork ignored `incremental_buffer` on string keys
with only a debug log, so the buffer could not cover that.

## Setup

| Piece | Image | Why |
|---|---|---|
| source | `mongo:5.0` (5.0.32), WiredTiger cache 0.25 GB | DocumentDB 5.0 API |
| target | `clickhouse/clickhouse-server:26.2.5.45` | production version, `mutations_sync=2` like prod |
| runner | `golang:1.25-bookworm` | glibc 2.36; runs the linux/arm64 binaries |

The replication yaml mirrors `sling/sling-docdb.yaml`: the same `defaults`
(`flatten: 1`, `incremental_buffer: 1h`, `primary_key: [_id]`,
`add_new_columns`, `column_casing: snake`, `adjust_column_type`), the stream's
`select`, `SLING_THREADS: 1`, and a ClickHouse connection whose database is the
target's (as `CLICKHOUSE_MONGO` points at `/mongo`).

## Running

Binaries go in `$RUN_DIR/bin` (default `~/.cache/sling-oidbuf`; colima only
mounts `/Users`, so keep it there). From the repo root:

```bash
R=~/.cache/sling-oidbuf; mkdir -p $R/bin $R/gocache
build() { # build <worktree> <name> <version>
  docker run --rm -v "$1":/src -v $R/gocache:/root/.cache/go-build -v $R/bin:/out -w /src \
    golang:1.25-bookworm go build -buildvcs=false -mod=vendor \
    -ldflags "-X github.com/slingdata-io/sling-cli/core.Version=$3" -o /out/$2 ./cmd/sling/
}
build <main-worktree> sling-old dev-base
build "$PWD" sling-new dev-oidbuf

test-incremental-buffer-objectid/run.sh all     # up + matrix + soak + perf
test-incremental-buffer-objectid/run.sh down
```

A fresh worktree needs the vendored substrait test data the build embeds:
copy `vendor/github.com/substrait-io/substrait/tests/cases` from a checkout
that has it (it is git-ignored).

Results land in `$RUN_DIR/results.tsv` (`PASS`/`FAIL`/`INFO`, test, binary,
expected, actual) and sling logs in `$RUN_DIR/logs/`.

## What is tested

### Matrix (`run.sh matrix`) — deterministic, old vs new

The seed is 1,000 docs spread over 5 h, with the newest pinned to the highest
process bytes. Each case then changes the source and syncs both binaries with
`update_key: _id`, `incremental_buffer: 2h`.

| # | Case | old (`main`) | new (this branch) |
|---|---|---|---|
| 01 | first sync, empty target | full load, 0 missing | full load, 0 missing |
| 02 | **same-second id from a lower-sorting process, committed after the watermark passed it** | **lost** (log: buffer `ignored`, unsupported type `text`) | **synced** (log: `applied (ObjectId)`, watermark rewound exactly 7200 s) |
| 03 | late insert, id 90 min below the watermark | lost | synced |
| 04 | late insert, id 3 h below the watermark (outside 2h) | lost | lost — by design |
| 05 | 50 ordinary new docs | 50 synced | 50 synced |
| 06 | re-save 30 min below the watermark | stale | updated, still 1 copy |
| 07 | re-save 5 h below the watermark (outside 2h) | stale | stale — by design |
| 08 | rerun with no source change | — | count unchanged, 0 dups (window re-read and merged) |
| 09 | target truncated | — | full reload, 0 missing; heals 04 and 07 |
| 10 | buffer `0s` / `abc` / `-2h` / `10000` | — | no lookback; `abc`, `-2h`, `10000` warn |
| 10 | buffer `1500ms` / `1000000h` | — | synced; `1000000h` re-reads all 1,059 docs, 0 dups |
| 11 | string `_id` (`doc-000301`…) | new ids synced, buffer ignored | identical |
| 12 | integer `_id`, buffer `10` | identical to new (see limits) | identical to old |
| 13 | `update_key: updated_at` + 1h (today's prod config): row moved 30 min / 2 h below the watermark | caught / missed | caught / missed — identical |
| 14 | stream-level `source_options` with `flatten: 1` / without it | — | prod column shape / run fails `did not find update_key: _id` |

### Soak (`run.sh soak`) — concurrent writers, old vs new

Four writers, each with its own ObjectId process bytes, ran for 240 s with a
random 0–700 ms gap between building a doc's id and inserting it. Both
binaries synced back to back the whole time (118 runs each). 2,859 docs total.

| | missing | extra | duplicates |
|---|---|---|---|
| old (`main`) | **407 (14%)** | 0 | 0 |
| new | **0** | 0 | 0 |

### Performance (`run.sh perf`) — 150,000 docs × 5 KB (818 MiB), then 2,000 new

The Mongo profiler reading below covers the incremental `find` and every
`getMore` of its cursor.

| Run | Plan | Docs examined | Mongo time | Rows |
|---|---|---|---|---|
| old, `updated_at` + 1h (today) | COLLSCAN | 152,000 | 1,198 ms | 2,041 |
| new, `updated_at` + 1h (unchanged) | COLLSCAN | 152,000 | 1,226 ms | 2,041 |
| **new, `_id` + 2h** | **IXSCAN `{ _id: 1 }`** | **2,083** | **11 ms** | 2,083 |

All three targets end with all 152,000 docs, 0 extra, 0 duplicates. The scan
cost on `updated_at` grows with the collection; the `_id` read grows only with
the 2h window. In production that scan is ~2.6 M docs / ~12 GiB on DocumentDB
storage, which is why it takes ~10 min there.

The buffer itself costs ~1 µs once per stream run
(`BenchmarkApplyIncrementalBufferObjectID`; `objectIDLowerBound` is 66 ns).

## Known limits

- **By design:** a doc inserted with an id older than `watermark - buffer`, or
  re-saved after that point, is not re-read. Datetime buffers have the same
  rule.
- **Already existed:** integer `_id` keys on Mongo never match. The reader
  passes the checkpoint as a string (`{"$gt": "290"}`), and Mongo never orders
  strings after numbers. Case 12 shows old and new behave the same.
- **Already existed:** the reader coerces any 24-hex checkpoint into an
  ObjectID, so string-typed `_id`s that look like ObjectIds would not match
  either.
- **Not covered here:** DocumentDB itself. Mongo 5.0 stands in for its 5.0 API.
