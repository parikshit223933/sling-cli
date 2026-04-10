# incremental_buffer — integration tests

End-to-end tests for the `source_options.incremental_buffer` feature.

## What's being tested

The `incremental_buffer` config option widens the incremental sync lookback window to tolerate the snapshot-isolation + replica-lag race where a row updated during Sling's scan is permanently skipped because a neighboring row with higher `update_key` advances the watermark past the stuck row's new value.

The critical test (test 04 / test 05) **deterministically reproduces the race post-state** — then proves the buffer catches it.

## Prerequisites

- Docker + Docker Compose
- Go 1.24+ (for building sling-cli)
- `curl`, `bash`, `sed`
- Ports `13306`, `18123`, `19000` free on localhost

## Running

```bash
cd test-incremental-buffer
./test.sh
```

Flags:

- `--keep` — leave containers running after tests finish (for debugging)
- `--no-build` — reuse an existing `sling-test-bin` (skip `go build`)
- `--only <test_fn>` — run one test by function name, e.g. `--only test_05_fix_works_with_buffer`

## Test matrix

| # | Test | What it proves |
|---|---|---|
| 01 | `baseline_initial_sync` | Plain full load works |
| 02 | `incremental_new_rows` | Plain incremental captures new rows |
| 03 | `incremental_updates` | Updates with forward-moving `modified_on` propagate |
| 04 | `reproduce_race_without_buffer` | **Bug reproduction:** a row whose `modified_on` moves BELOW CH max is stranded |
| 05 | `fix_works_with_buffer` | **Fix verification:** same setup, 1h buffer, row is recovered |
| 06 | `buffer_boundary_inside` | Update at `(max - 30m)` is caught with buffer=1h |
| 07 | `buffer_boundary_outside` | Update at `(max - 2h)` is NOT caught with buffer=1h — confirms buffer semantics |
| 08 | `first_sync_with_buffer` | Empty target + buffer → full load works |
| 09 | `intkey_buffer_ignored` | Integer `update_key` → buffer is silently ignored |
| 10 | `zero_buffer_equals_no_buffer` | `buffer: 0s` is a no-op |
| 11 | `invalid_buffer_string` | `buffer: "not-a-duration"` logs a warning and falls back cleanly |

## File layout

```
test-incremental-buffer/
├── docker-compose.yml          # MySQL 8.0 (IST) + ClickHouse 24.8
├── init-mysql.sql              # Creates test_src.events and test_src.events_intkey
├── sling-home/env.yaml         # Sling connection config (MYSQL_TEST, CH_TEST)
├── replication-no-buffer.yaml
├── replication-with-buffer.yaml
├── replication-intkey.yaml
├── test.sh                     # Main harness
└── README.md
```

`sling-home/` is used as `SLING_HOME_DIR` so the test is isolated from your real `~/.sling/env.yaml`.

## Matching production TZ

MySQL is started with session timezone `Asia/Kolkata` and `--default-time-zone=+05:30` to mirror the LSQ production replica. This matters because the update key is `DATETIME` (no TZ conversion) — if the literal passed from CH were reinterpreted in a different session TZ, the test would silently mis-test the fix. Keep this setting.

## What the "race reproduction" actually does

The real race requires two concurrent processes fighting over a MySQL snapshot — not tractable to reproduce deterministically. But the race is only interesting because of its **effect**, which is:

> "Row R has `modified_on` in CH = `Vold`, but MySQL now has `modified_on` = `Vnew` where `Vold < Vnew < max(CH.modified_on)`."

The test creates this state directly by writing a lower `modified_on` to MySQL for one row, then appending a new row with a higher `modified_on`. Running a no-buffer incremental sync leaves the first row stranded — exactly like production. Adding `incremental_buffer: 1h` recovers it. The reproduction is equivalent in outcome to the actual race and is 100% deterministic.

## Expected output

```
[test] SUMMARY: 11 passed, 0 failed
```

Any test reporting `FAIL` means the patch is broken — do not deploy.

## Debugging failed runs

```bash
./test.sh --keep            # leave containers up
docker compose exec mysql mysql -uroot -prootpass test_src
curl 'http://localhost:18123/?database=test_tgt' --data-binary 'SELECT * FROM events'
```

Sling run output is indented under each test step, so you can scroll up to see the actual queries and errors.
