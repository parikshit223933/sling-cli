#!/usr/bin/env bash
# Integration test harness for the `incremental_buffer` SourceOptions feature.
#
# The harness:
#   1. Spins up MySQL 8 + ClickHouse via docker-compose
#   2. Builds a fresh sling-cli binary from the parent repo
#   3. Runs a sequence of tests that seed MySQL, trigger sling syncs,
#      and verify ClickHouse state
#
# The critical test deterministically simulates the snapshot-isolation race
# by directly setting MySQL `modified_on` values that are lower than
# ClickHouse's current max, which is EXACTLY the post-race state. Without
# the buffer, Sling cannot catch the update. With the buffer, it can.
#
# Usage:
#   ./test.sh                 # run everything
#   ./test.sh --keep          # don't tear down docker-compose on exit
#   ./test.sh --no-build      # skip sling-cli rebuild
#   ./test.sh --only <test>   # run only a specific test function name

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
SLING_BIN="$SCRIPT_DIR/sling-test-bin"
SLING_HOME="$SCRIPT_DIR/sling-home"

# Isolate all sling state inside the test dir.
export SLING_HOME_DIR="$SLING_HOME"

KEEP_CONTAINERS=0
SKIP_BUILD=0
ONLY_TEST=""
for arg in "$@"; do
  case "$arg" in
    --keep)     KEEP_CONTAINERS=1 ;;
    --no-build) SKIP_BUILD=1 ;;
    --only)     shift; ONLY_TEST="${2:-}" ;;
  esac
done
if [[ "${1:-}" == "--only" ]]; then
  ONLY_TEST="${2:-}"
fi

# ----------------------------------------------------------------------------
# ANSI color helpers
# ----------------------------------------------------------------------------
C_RESET=$'\033[0m'
C_RED=$'\033[31m'
C_GREEN=$'\033[32m'
C_YELLOW=$'\033[33m'
C_BLUE=$'\033[34m'
C_DIM=$'\033[2m'

log()  { echo "${C_BLUE}[test]${C_RESET} $*"; }
pass() { echo "${C_GREEN}  PASS${C_RESET} $*"; }
fail() { echo "${C_RED}  FAIL${C_RESET} $*"; FAILED_TESTS+=("$1"); }
warn() { echo "${C_YELLOW}[warn]${C_RESET} $*"; }
dim()  { echo "${C_DIM}$*${C_RESET}"; }

FAILED_TESTS=()
PASSED=0
FAILED=0

# ----------------------------------------------------------------------------
# Docker helpers
# ----------------------------------------------------------------------------
# Support both `docker compose` (v2 plugin) and `docker-compose` (v2 standalone).
if docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(docker-compose)
else
  echo "error: neither 'docker compose' nor 'docker-compose' is available" >&2
  exit 1
fi

compose() {
  (cd "$SCRIPT_DIR" && "${COMPOSE_CMD[@]}" "$@")
}

cleanup() {
  local ec=$?
  if [[ $KEEP_CONTAINERS -eq 0 ]]; then
    log "tearing down containers"
    compose down -v 2>/dev/null || true
  else
    warn "--keep set, containers left running"
  fi
  exit $ec
}
trap cleanup EXIT

bring_up_stack() {
  log "starting docker-compose stack"
  compose down -v 2>/dev/null || true
  compose up -d
  log "waiting for MySQL health"
  for i in $(seq 1 60); do
    if compose exec -T mysql mysqladmin ping -prootpass 2>/dev/null | grep -q alive; then
      break
    fi
    sleep 2
    if [[ $i -eq 60 ]]; then
      fail "mysql_health" "mysql never became healthy"
      return 1
    fi
  done
  log "waiting for ClickHouse health"
  for i in $(seq 1 60); do
    if curl -s http://localhost:18123/ping 2>/dev/null | grep -q Ok; then
      break
    fi
    sleep 2
    if [[ $i -eq 60 ]]; then
      fail "ch_health" "clickhouse never became healthy"
      return 1
    fi
  done
  # Make sure the target DB exists (CLICKHOUSE_DB env var creates it but be explicit)
  ch_exec "CREATE DATABASE IF NOT EXISTS test_tgt"
  log "stack ready"
}

# ----------------------------------------------------------------------------
# SQL execution helpers
# ----------------------------------------------------------------------------
mysql_exec() {
  compose exec -T mysql mysql -uroot -prootpass --default-character-set=utf8 -N -B test_src -e "$1" 2>/dev/null
}

ch_exec() {
  curl -sS "http://localhost:18123/?database=test_tgt" --data-binary "$1"
}

ch_query() {
  curl -sS "http://localhost:18123/?database=test_tgt&default_format=TabSeparated" --data-binary "$1"
}

# ----------------------------------------------------------------------------
# Build sling-cli
# ----------------------------------------------------------------------------
build_sling() {
  if [[ $SKIP_BUILD -eq 1 && -x "$SLING_BIN" ]]; then
    log "skipping build ($SLING_BIN already exists)"
    return
  fi
  log "building sling-cli binary at $SLING_BIN"
  (cd "$REPO_DIR" && go build -mod=vendor -o "$SLING_BIN" ./cmd/sling/)
  "$SLING_BIN" --version
}

# ----------------------------------------------------------------------------
# Sync runner
# ----------------------------------------------------------------------------
run_sling() {
  local replication_file="$1"
  log "sling run --replication $(basename "$replication_file")"
  "$SLING_BIN" run --replication "$SCRIPT_DIR/$replication_file" 2>&1 | sed 's/^/    /'
}

# ----------------------------------------------------------------------------
# Assertion helpers
# ----------------------------------------------------------------------------
assert_eq() {
  local expected="$1"
  local actual="$2"
  local label="$3"
  if [[ "$expected" == "$actual" ]]; then
    pass "$label (got '$actual')"
    PASSED=$((PASSED+1))
  else
    fail "$label" "expected '$expected', got '$actual'"
    FAILED=$((FAILED+1))
  fi
}

# ----------------------------------------------------------------------------
# Test state: wipe both sides so each test is independent
# ----------------------------------------------------------------------------
reset_data() {
  mysql_exec "TRUNCATE TABLE events; TRUNCATE TABLE events_intkey;" >/dev/null
  ch_exec "DROP TABLE IF EXISTS test_tgt.events" >/dev/null
  ch_exec "DROP TABLE IF EXISTS test_tgt.events_intkey" >/dev/null
}

# ----------------------------------------------------------------------------
# Test cases
# ----------------------------------------------------------------------------

test_01_baseline_initial_sync() {
  log "TEST 1: baseline initial sync (no buffer, empty target → full seed)"
  reset_data
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A', 'first',  '2024-01-01 10:00:00'),
      (2, 'B', 'second', '2024-01-01 10:00:05'),
      (3, 'C', 'third',  '2024-01-01 10:00:10'),
      (4, 'D', 'fourth', '2024-01-01 10:00:15'),
      (5, 'E', 'fifth',  '2024-01-01 10:00:20');
  " >/dev/null
  run_sling replication-no-buffer.yaml
  local count
  count=$(ch_query "SELECT count() FROM test_tgt.events")
  assert_eq "5" "$count" "test_01: row count after initial sync"
}

test_02_incremental_new_rows() {
  log "TEST 2: incremental picks up new rows (no buffer, happy path)"
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (6, 'F', 'sixth',   '2024-01-01 10:00:25'),
      (7, 'G', 'seventh', '2024-01-01 10:00:30');
  " >/dev/null
  run_sling replication-no-buffer.yaml
  local count
  count=$(ch_query "SELECT count() FROM test_tgt.events")
  assert_eq "7" "$count" "test_02: row count after incremental add"
}

test_03_incremental_updates() {
  log "TEST 3: incremental picks up updates with advancing modified_on"
  mysql_exec "
    UPDATE events SET stage='X', modified_on='2024-01-01 10:00:35'
      WHERE id=3;
  " >/dev/null
  run_sling replication-no-buffer.yaml
  local stage
  stage=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=3")
  assert_eq "X" "$stage" "test_03: updated stage reflected"
}

test_04_reproduce_race_without_buffer() {
  log "TEST 4: reproduce the race bug WITHOUT buffer (deterministic)"
  reset_data
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A',   'a', '2024-01-01 10:00:00'),
      (2, 'OLD', 'b', '2024-01-01 10:00:05'),
      (3, 'C',   'c', '2024-01-01 10:00:10');
  " >/dev/null
  run_sling replication-no-buffer.yaml

  # Sanity: before the race setup, row 2 should be 'OLD' in CH
  local pre
  pre=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=2")
  assert_eq "OLD" "$pre" "test_04: pre-race CH row 2 stage"

  # Now simulate the race post-state directly:
  #   - row 2 in MySQL gets new stage + a modified_on LOWER than CH's current max
  #   - row 4 arrives with modified_on HIGHER than row 2's update
  # A no-buffer incremental query `WHERE modified_on > 10:00:10` captures row 4
  # but NOT row 2's update, permanently stranding the stale value in CH.
  mysql_exec "
    UPDATE events SET stage='NEW', modified_on='2024-01-01 10:00:07' WHERE id=2;
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (4, 'D', 'd', '2024-01-01 10:00:15');
  " >/dev/null
  run_sling replication-no-buffer.yaml

  local count stage_2
  count=$(ch_query "SELECT count() FROM test_tgt.events")
  stage_2=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=2")
  # Expected BUG state: row 4 is synced, row 2 is still OLD.
  assert_eq "4" "$count" "test_04: row 4 was captured"
  assert_eq "OLD" "$stage_2" "test_04: row 2 STUCK at 'OLD' (reproduces bug)"
}

test_05_fix_works_with_buffer() {
  log "TEST 5: buffer=1h catches the stuck row (race fix verification)"
  reset_data
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A',   'a', '2024-01-01 10:00:00'),
      (2, 'OLD', 'b', '2024-01-01 10:00:05'),
      (3, 'C',   'c', '2024-01-01 10:00:10');
  " >/dev/null
  # Initial sync with buffer enabled.
  run_sling replication-with-buffer.yaml

  # Same race simulation as test 4.
  mysql_exec "
    UPDATE events SET stage='NEW', modified_on='2024-01-01 10:00:07' WHERE id=2;
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (4, 'D', 'd', '2024-01-01 10:00:15');
  " >/dev/null
  run_sling replication-with-buffer.yaml

  local stage_2 count
  count=$(ch_query "SELECT count() FROM test_tgt.events")
  stage_2=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=2")
  assert_eq "4" "$count" "test_05: row 4 was captured"
  assert_eq "NEW" "$stage_2" "test_05: row 2 UPDATED to 'NEW' (fix works)"
}

test_06_buffer_boundary_inside() {
  log "TEST 6: update exactly (max - 30min) is caught with buffer=1h"
  reset_data
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A', 'a', '2024-01-01 09:00:00'),
      (2, 'B', 'b', '2024-01-01 10:00:00');
  " >/dev/null
  run_sling replication-with-buffer.yaml

  # CH max = 10:00:00. Put row 1's update at 09:30:00 (inside 1h buffer).
  mysql_exec "UPDATE events SET stage='INSIDE', modified_on='2024-01-01 09:30:00' WHERE id=1;" >/dev/null
  run_sling replication-with-buffer.yaml

  local s
  s=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=1")
  assert_eq "INSIDE" "$s" "test_06: row inside buffer window captured"
}

test_07_buffer_boundary_outside() {
  log "TEST 7: update at (max - 2h) is NOT caught with buffer=1h (boundary)"
  reset_data
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A', 'a', '2024-01-01 08:00:00'),
      (2, 'B', 'b', '2024-01-01 11:00:00');
  " >/dev/null
  run_sling replication-with-buffer.yaml

  # CH max = 11:00:00. Put row 1's update at 09:00:00 — 2 hours behind max.
  mysql_exec "UPDATE events SET stage='OUTSIDE', modified_on='2024-01-01 09:00:00' WHERE id=1;" >/dev/null
  run_sling replication-with-buffer.yaml

  # With buffer=1h, watermark = 10:00:00, query = `WHERE mod > 10:00:00`.
  # 09:00:00 is below that, so the update is missed. This confirms buffer semantics.
  local s
  s=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=1")
  assert_eq "A" "$s" "test_07: row outside buffer window correctly missed"
}

test_08_first_sync_with_buffer() {
  log "TEST 8: first sync (empty target) with buffer does full load"
  reset_data
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A', 'a', '2020-01-01 00:00:00'),
      (2, 'B', 'b', '2024-01-01 00:00:00'),
      (3, 'C', 'c', '2025-01-01 00:00:00');
  " >/dev/null
  run_sling replication-with-buffer.yaml
  local c
  c=$(ch_query "SELECT count() FROM test_tgt.events")
  assert_eq "3" "$c" "test_08: full load via first-sync unaffected by buffer"
}

test_09_intkey_buffer_ignored() {
  log "TEST 9: integer update_key — buffer silently ignored"
  mysql_exec "INSERT INTO events_intkey (id, stage) VALUES (1,'A'),(2,'B'),(3,'C');" >/dev/null
  run_sling replication-intkey.yaml
  local c
  c=$(ch_query "SELECT count() FROM test_tgt.events_intkey")
  assert_eq "3" "$c" "test_09: int-key initial sync works"

  mysql_exec "INSERT INTO events_intkey (id, stage) VALUES (4,'D');" >/dev/null
  run_sling replication-intkey.yaml
  c=$(ch_query "SELECT count() FROM test_tgt.events_intkey")
  assert_eq "4" "$c" "test_09: int-key incremental works (no buffer interference)"
}

test_10_zero_buffer_equals_no_buffer() {
  log "TEST 10: buffer='0s' reproduces the bug like no-buffer"
  reset_data
  # Create a temporary replication file with zero buffer.
  cat > "$SCRIPT_DIR/replication-zero-buffer.yaml" <<'YAML'
source: MYSQL_TEST
target: CH_TEST
defaults:
  mode: incremental
  source_options:
    incremental_buffer: 0s
  target_options:
    add_new_columns: false
    column_casing: snake
streams:
  test_src.events:
    object: test_tgt.events
    primary_key: [id]
    update_key: modified_on
    select: [id, stage, note, modified_on, last_modified]
YAML
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A',   'a', '2024-01-01 10:00:00'),
      (2, 'OLD', 'b', '2024-01-01 10:00:05'),
      (3, 'C',   'c', '2024-01-01 10:00:10');
  " >/dev/null
  run_sling replication-zero-buffer.yaml
  mysql_exec "
    UPDATE events SET stage='NEW', modified_on='2024-01-01 10:00:07' WHERE id=2;
    INSERT INTO events (id, stage, note, modified_on) VALUES (4, 'D', 'd', '2024-01-01 10:00:15');
  " >/dev/null
  run_sling replication-zero-buffer.yaml
  local s
  s=$(ch_query "SELECT stage FROM test_tgt.events WHERE id=2")
  assert_eq "OLD" "$s" "test_10: zero buffer behaves like no buffer"
  rm -f "$SCRIPT_DIR/replication-zero-buffer.yaml"
}

test_11_invalid_buffer_string() {
  log "TEST 11: invalid buffer string logs warning and falls back"
  reset_data
  cat > "$SCRIPT_DIR/replication-bad-buffer.yaml" <<'YAML'
source: MYSQL_TEST
target: CH_TEST
defaults:
  mode: incremental
  source_options:
    incremental_buffer: "not-a-duration"
  target_options:
    add_new_columns: false
    column_casing: snake
streams:
  test_src.events:
    object: test_tgt.events
    primary_key: [id]
    update_key: modified_on
    select: [id, stage, note, modified_on, last_modified]
YAML
  mysql_exec "
    INSERT INTO events (id, stage, note, modified_on) VALUES
      (1, 'A', 'a', '2024-01-01 10:00:00'),
      (2, 'B', 'b', '2024-01-01 10:00:05');
  " >/dev/null
  # Should complete without crashing; the warning is printed and sync proceeds
  # like no buffer was set.
  run_sling replication-bad-buffer.yaml
  local c
  c=$(ch_query "SELECT count() FROM test_tgt.events")
  assert_eq "2" "$c" "test_11: invalid buffer gracefully falls back"
  rm -f "$SCRIPT_DIR/replication-bad-buffer.yaml"
}

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
main() {
  log "========================================================"
  log "sling-cli incremental_buffer integration tests"
  log "========================================================"
  build_sling
  bring_up_stack

  local tests=(
    test_01_baseline_initial_sync
    test_02_incremental_new_rows
    test_03_incremental_updates
    test_04_reproduce_race_without_buffer
    test_05_fix_works_with_buffer
    test_06_buffer_boundary_inside
    test_07_buffer_boundary_outside
    test_08_first_sync_with_buffer
    test_09_intkey_buffer_ignored
    test_10_zero_buffer_equals_no_buffer
    test_11_invalid_buffer_string
  )

  for t in "${tests[@]}"; do
    if [[ -n "$ONLY_TEST" && "$t" != "$ONLY_TEST" ]]; then
      continue
    fi
    echo
    "$t"
  done

  echo
  log "========================================================"
  log "SUMMARY: ${C_GREEN}$PASSED passed${C_RESET}, ${C_RED}$FAILED failed${C_RESET}"
  log "========================================================"
  if [[ $FAILED -ne 0 ]]; then
    exit 1
  fi
}

main
