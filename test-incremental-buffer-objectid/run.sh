#!/usr/bin/env bash
# End-to-end tests for `incremental_buffer` on Mongo ObjectId `_id` keys:
# MongoDB 5.0 (the DocumentDB 5.0 API) -> ClickHouse 26.2 (prod version),
# replication config shaped like sling/sling-docdb.yaml in the ops repo.
#
#   ./run.sh up       start the containers (idempotent)
#   ./run.sh matrix   deterministic correctness matrix, old vs new binary
#   ./run.sh soak     concurrent writers while syncing, old vs new binary
#   ./run.sh perf     COLLSCAN on updated_at vs _id range, at scale
#   ./run.sh all      up + matrix + soak + perf
#   ./run.sh down     remove the containers
#
# Binaries are linux/arm64 builds in $RUN_DIR/bin (see README.md).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="${RUN_DIR:-$HOME/.cache/sling-oidbuf}" # colima only mounts /Users
NET=oidbuf-net
MONGO=oidbuf-mongo
CH=oidbuf-ch
RUNNER=oidbuf-runner
MONGO_IMAGE="${MONGO_IMAGE:-mongo:5.0}"
CH_IMAGE="${CH_IMAGE:-clickhouse/clickhouse-server:26.2.5.45}"
RUNNER_IMAGE="${RUNNER_IMAGE:-golang:1.25-bookworm}"
OLD="${OLD_BIN:-sling-old}"
NEW="${NEW_BIN:-sling-new}"
COLL=sales_call_transcription_contents
RESULTS="$RUN_DIR/results.tsv"

mkdir -p "$RUN_DIR"/{bin,logs,gen,dump,slinghome,chconf}

# ---------------------------------------------------------------- plumbing
mjs() { docker exec "$MONGO" mongosh --quiet "mongodb://localhost:27017/admin" --eval "load('/harness/lib.js'); $1"; }
chq() { docker exec "$CH" clickhouse-client --password itest -q "$1"; }

# srun <bin> <label> [sling flags]: run $RUN_DIR/gen/<label>.yaml, log to logs/<label>.log
srun() {
  local bin=$1 label=$2
  shift 2
  docker exec -e SLING_HOME_DIR=/itest/slinghome "$RUNNER" \
    "/itest/bin/$bin" run "$@" --replication "/itest/gen/$label.yaml" >"$RUN_DIR/logs/$label.log" 2>&1
  local rc=$?
  [ $rc -ne 0 ] && echo "  ! $label exited $rc (see $RUN_DIR/logs/$label.log)"
  return $rc
}

# ch_conn <ch-db>: register CH_<DB> in env.yaml, pointing at that database (like
# prod's CLICKHOUSE_MONGO -> /mongo). Sling's ClickHouse bulk insert names the
# _tmp table without a database and HTTP requests don't keep USE, so the
# connection's database has to be the target's.
ch_conn() {
  local name
  name="CH_$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')"
  grep -q "^  $name:" "$RUN_DIR/slinghome/env.yaml" || cat >>"$RUN_DIR/slinghome/env.yaml" <<YAML
  $name:
    type: clickhouse
    http_url: 'http://default:itest@oidbuf-ch:8123/$1?mutations_sync=2'
YAML
  echo "$name"
}

# gen <label> <mongo-db> <coll> <ch-db> <update_key> <srcopts: full|bufonly|none> [buffer]
gen() {
  local label=$1 mdb=$2 coll=$3 tdb=$4 key=$5 mode=$6 buf=${7:-2h} so="" tconn
  tconn=$(ch_conn "$tdb")
  case $mode in
    full) so=$'    source_options:\n      flatten: 1\n      incremental_buffer: '"\"$buf\"" ;;
    bufonly) so=$'    source_options:\n      incremental_buffer: '"\"$buf\"" ;;
    none) so="" ;;
  esac
  cat >"$RUN_DIR/gen/$label.yaml" <<YAML
source: MONGO_IT
target: $tconn

env:
  SLING_THREADS: "1"

defaults:
  mode: incremental
  primary_key: [_id]
  update_key: updated_at
  source_options:
    flatten: 1
    incremental_buffer: 1h
  target_options:
    add_new_columns: true
    column_casing: snake
    adjust_column_type: true

streams:
  $mdb.$coll:
    object: $tdb.$coll
    update_key: $key
$so
    select:
      - _id
      - sales_call_transcription_id
      - transcription_data
      - created_at
      - updated_at
YAML
}

# record <test> <bin> <expected> <actual>
record() {
  local verdict=PASS
  [ "$3" == "$4" ] || verdict=FAIL
  printf '%s\t%s\t%s\t%s\t%s\n' "$verdict" "$1" "$2" "$3" "$4" | tee -a "$RESULTS"
}

info() { printf 'INFO\t%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }

has() { # has <ch-db> <id> -> present|absent
  local n
  n=$(chq "SELECT count() FROM $1.$COLL WHERE _id = '$2'")
  [ "$n" -ge 1 ] 2>/dev/null && echo present || echo absent
}
copies() { chq "SELECT count() FROM $1.$COLL WHERE _id = '$2'"; }
ch_updated_ms() { chq "SELECT toUnixTimestamp64Milli(updated_at) FROM $1.$COLL WHERE _id = '$2' LIMIT 1"; }

# sets <mongo-db> <coll> <ch-db> -> "missing=<n> extra=<n> dups=<n>"
sets() {
  local m="$RUN_DIR/dump/m-$3.txt" c="$RUN_DIR/dump/c-$3.txt"
  mjs "dumpIds('$1','$2')" | sort >"$m"
  chq "SELECT _id FROM $3.$2 ORDER BY _id" >"$c"
  echo "missing=$(comm -23 "$m" <(sort -u "$c") | wc -l | tr -d ' ') extra=$(comm -13 "$m" <(sort -u "$c") | wc -l | tr -d ' ') dups=$(($(wc -l <"$c") - $(sort -u "$c" | wc -l)))"
}

secs_of() { # seconds of the stream's read+write from a sling log
  grep -oE "inserted [0-9]+ rows into .* in [0-9]+ secs" "$1" | tail -1 | grep -oE "in [0-9]+ secs" | grep -oE "[0-9]+"
}
rows_of() { grep -oE "inserted [0-9]+ rows" "$1" | tail -1 | grep -oE "[0-9]+"; }

# ---------------------------------------------------------------- lifecycle
up() {
  cat >"$RUN_DIR/chconf/prod-like.xml" <<'XML'
<clickhouse>
  <profiles>
    <default>
      <!-- production sling write user waits for mutations (mutations_sync=2) -->
      <mutations_sync>2</mutations_sync>
    </default>
  </profiles>
</clickhouse>
XML
  cat >"$RUN_DIR/slinghome/env.yaml" <<'YAML'
connections:
  MONGO_IT:
    type: mongodb
    url: 'mongodb://oidbuf-mongo:27017/admin?directConnection=true&retryWrites=false'
  CH_IT:
    type: clickhouse
    http_url: 'http://default:itest@oidbuf-ch:8123/default?mutations_sync=2'
YAML
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
  docker inspect "$MONGO" >/dev/null 2>&1 || docker run -d --name "$MONGO" --network "$NET" --memory 1536m \
    -v "$RUN_DIR":/itest -v "$HERE":/harness:ro "$MONGO_IMAGE" --bind_ip_all --wiredTigerCacheSizeGB 0.25 >/dev/null
  docker inspect "$CH" >/dev/null 2>&1 || docker run -d --name "$CH" --network "$NET" --memory 2g \
    --ulimit nofile=262144:262144 -e CLICKHOUSE_PASSWORD=itest \
    -v "$RUN_DIR/chconf/prod-like.xml":/etc/clickhouse-server/users.d/prod-like.xml:ro "$CH_IMAGE" >/dev/null
  docker inspect "$RUNNER" >/dev/null 2>&1 || docker run -d --name "$RUNNER" --network "$NET" --memory 1536m \
    -v "$RUN_DIR":/itest "$RUNNER_IMAGE" sleep infinity >/dev/null
  for _ in $(seq 1 60); do
    mjs "db.runCommand({ping:1}).ok" >/dev/null 2>&1 && chq "SELECT 1" >/dev/null 2>&1 && break
    sleep 1
  done
  echo "mongo $(mjs 'db.version()') | clickhouse $(chq 'SELECT version()') | mutations_sync=$(chq "SELECT getSetting('mutations_sync')")"
  for b in "$OLD" "$NEW"; do
    echo "$b: $(docker exec "$RUNNER" "/itest/bin/$b" --version 2>&1 | tr '\n' ' ')"
  done
}

down() { docker rm -f "$MONGO" "$CH" "$RUNNER" >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; echo down; }

# ---------------------------------------------------------------- matrix
matrix() {
  local T0 SPAN=18000 TMAX
  T0=$(($(date +%s) / 60 * 60 - 3 * 86400))
  TMAX=$((T0 + SPAN))
  echo "== matrix (T0=$T0 TMAX=$TMAX)"

  mjs "db.getSiblingDB('admin').$COLL.drop()" >/dev/null
  for d in itest_old itest_new; do chq "DROP DATABASE IF EXISTS $d"; chq "CREATE DATABASE $d"; done
  gen m-old admin $COLL itest_old _id full 2h
  gen m-new admin $COLL itest_new _id full 2h

  # 01 first sync: empty target -> full load, no buffer involved
  mjs "seed('admin','$COLL',1000,$T0,$SPAN,2,1000000)" >/dev/null
  for b in old new; do
    local B; [ $b == old ] && B=$OLD || B=$NEW
    srun "$B" m-$b -d
    record 01_first_sync_full_load $b "missing=0 extra=0 dups=0" "$(sets admin $COLL itest_$b)"
  done

  # 02 THE race: same-second id from a lower-sorting process lands after the
  # watermark moved past it. Old binary ignores the buffer for _id -> lost.
  local X1
  X1=$(mjs "insertAt('admin','$COLL',$TMAX,'0000000001',7,2000001,'inversion')")
  srun "$OLD" m-old -d; srun "$NEW" m-new -d
  cp "$RUN_DIR/logs/m-old.log" "$RUN_DIR/logs/02-old.log"; cp "$RUN_DIR/logs/m-new.log" "$RUN_DIR/logs/02-new.log"
  record 02_same_second_inversion old absent "$(has itest_old "$X1")"
  record 02_same_second_inversion new present "$(has itest_new "$X1")"
  record 02_old_log_buffer_ignored old 1 "$(grep -c 'incremental_buffer=2h ignored: update_key "_id" has unsupported type' "$RUN_DIR/logs/02-old.log")"
  record 02_new_log_buffer_applied new 1 "$(grep -c 'incremental_buffer=2h applied (ObjectId)' "$RUN_DIR/logs/02-new.log")"

  # 03 late insert inside the buffer (id generated 90 min before the watermark)
  local X2
  X2=$(mjs "insertAt('admin','$COLL',$((TMAX - 5400)),'abcdefabcd',3,2000002,'late90m')")
  srun "$OLD" m-old -d; srun "$NEW" m-new -d
  record 03_late_insert_90m_inside_2h old absent "$(has itest_old "$X2")"
  record 03_late_insert_90m_inside_2h new present "$(has itest_new "$X2")"

  # 04 late insert outside the buffer (3h) -> missed by design, both binaries
  local X3
  X3=$(mjs "insertAt('admin','$COLL',$((TMAX - 10800)),'abcdefabcd',4,2000003,'late3h')")
  srun "$OLD" m-old -d; srun "$NEW" m-new -d
  record 04_late_insert_3h_outside_2h old absent "$(has itest_old "$X3")"
  record 04_late_insert_3h_outside_2h new absent "$(has itest_new "$X3")"

  # 05 ordinary new inserts after the watermark
  mjs "for (let i=1;i<=50;i++) insertAt('admin','$COLL',$TMAX+i*12,'5a5a5a5a5a',i,2000100+i,'new'+i)" >/dev/null
  srun "$OLD" m-old -d; srun "$NEW" m-new -d
  for b in old new; do
    record 05_new_inserts_captured $b 50 "$(chq "SELECT count() FROM itest_$b.$COLL WHERE sales_call_transcription_id BETWEEN 2000101 AND 2000150")"
  done
  local NMAX=$((TMAX + 600))

  # 06 re-save inside the buffer (doc ~30 min below the watermark)
  local D1 D1MS
  D1=$(mjs "idAtOrAfter('admin','$COLL',$((NMAX - 1800)))")
  D1MS=$(mjs "resave('admin','$COLL','$D1','rev2')")
  srun "$OLD" m-old -d; srun "$NEW" m-new -d
  record 06_resave_30m_inside_2h old stale "$([ "$(ch_updated_ms itest_old "$D1")" == "$D1MS" ] && echo updated || echo stale)"
  record 06_resave_30m_inside_2h new updated "$([ "$(ch_updated_ms itest_new "$D1")" == "$D1MS" ] && echo updated || echo stale)"
  record 06_resave_no_duplicate new 1 "$(copies itest_new "$D1")"

  # 07 re-save outside the buffer (doc ~4h50m below the watermark) -> missed by design
  local D2 D2MS
  D2=$(mjs "idAtOrAfter('admin','$COLL',$((T0 + 3600)))")
  D2MS=$(mjs "resave('admin','$COLL','$D2','rev3')")
  srun "$OLD" m-old -d; srun "$NEW" m-new -d
  for b in old new; do
    record 07_resave_5h_outside_2h $b stale "$([ "$(ch_updated_ms itest_$b "$D2")" == "$D2MS" ] && echo updated || echo stale)"
  done

  # 08 idempotent: re-running re-reads the 2h window and changes nothing
  local before after
  before=$(chq "SELECT count() FROM itest_new.$COLL")
  srun "$NEW" m-new -d; srun "$NEW" m-new -d
  after=$(chq "SELECT count() FROM itest_new.$COLL")
  record 08_rerun_idempotent new "$before dups=0" "$after dups=$(chq "SELECT count() - uniqExact(_id) FROM itest_new.$COLL")"
  record 08_rerun_rows_reread new yes "$([ "$(rows_of "$RUN_DIR/logs/m-new.log")" -gt 0 ] && echo yes || echo no)"

  # 09 emptied target -> next run is a full reload that heals 04 and 07
  chq "TRUNCATE TABLE itest_new.$COLL"
  srun "$NEW" m-new -d
  record 09_truncated_target_full_reload new "missing=0 extra=0 dups=0" "$(sets admin $COLL itest_new)"
  record 09_full_reload_heals_resave new updated "$([ "$(ch_updated_ms itest_new "$D2")" == "$D2MS" ] && echo updated || echo stale)"

  # 10 buffer values (new binary, fresh target each): inversion doc per case
  local v name exp ctr=100
  for v in "0s:absent" "abc:absent" "-2h:absent" "10000:absent" "1500ms:present" "1000000h:present"; do
    name=${v%%:*}; exp=${v##*:}; ctr=$((ctr + 1))
    local tdb="itest_buf_$(printf '%s' "$name" | tr -c 'a-z0-9' '_')"
    chq "DROP DATABASE IF EXISTS $tdb"; chq "CREATE DATABASE $tdb"
    gen "b-$ctr" admin $COLL "$tdb" _id full "$name"
    srun "$NEW" "b-$ctr" -d
    local mx inv
    mx=$(mjs "tsOf(maxId('admin','$COLL'))")
    inv=$(mjs "insertAt('admin','$COLL',$mx,'0000000002',$ctr,2100000+$ctr,'inv-$name')")
    srun "$NEW" "b-$ctr" -d
    record "10_buffer_${name}_same_second_inversion" new "$exp" "$(has "$tdb" "$inv")"
    case $name in
      abc) record 10_buffer_abc_warns new 1 "$(grep -c 'is not a valid Go duration' "$RUN_DIR/logs/b-$ctr.log")" ;;
      -2h) record 10_buffer_negative_warns new 1 "$(grep -c 'is negative' "$RUN_DIR/logs/b-$ctr.log")" ;;
      10000) record 10_buffer_count_on_objectid_warns new 1 "$(grep -c 'is not a valid Go duration' "$RUN_DIR/logs/b-$ctr.log")" ;;
      1000000h)
        record 10_buffer_huge_rereads_everything new "$(mjs "db.getSiblingDB('admin').$COLL.countDocuments()")" "$(rows_of "$RUN_DIR/logs/b-$ctr.log")"
        record 10_buffer_huge_no_dups new 0 "$(chq "SELECT count() - uniqExact(_id) FROM $tdb.$COLL")"
        ;;
    esac
  done

  # 11 string _id (not ObjectIds): buffer must stay ignored, old == new
  mjs "const c=db.getSiblingDB('admin').str_ids; c.drop(); for (let i=1;i<=300;i++){ if(i==150) continue; const at=new Date(($T0+i)*1000); c.insertOne(mkdoc('doc-'+String(i).padStart(6,'0'),i,at,at,1,'s'+i)); }" >/dev/null
  for b in old new; do
    chq "DROP DATABASE IF EXISTS itest_str_$b"; chq "CREATE DATABASE itest_str_$b"
    gen "s-$b" admin str_ids "itest_str_$b" _id full 2h
    local B; [ $b == old ] && B=$OLD || B=$NEW
    srun "$B" "s-$b" -d
  done
  mjs "const c=db.getSiblingDB('admin').str_ids; for (let i of [150,301,302,303]){ const at=new Date(($T0+i)*1000); c.insertOne(mkdoc('doc-'+String(i).padStart(6,'0'),i,at,at,1,'s'+i)); }" >/dev/null
  srun "$OLD" s-old -d; srun "$NEW" s-new -d
  for b in old new; do
    local ids
    ids=$(chq "SELECT arrayStringConcat(arraySort(groupArray(_id)), ',') FROM itest_str_$b.str_ids WHERE _id IN ('doc-000150','doc-000301','doc-000302','doc-000303')")
    record 11_string_ids_unchanged_behaviour $b "doc-000301,doc-000302,doc-000303" "$ids"
  done
  record 11_string_ids_buffer_ignored_new new 1 "$(grep -c 'incremental_buffer=2h ignored' "$RUN_DIR/logs/s-new.log")"

  # 12 integer _id: integer buffer branch unchanged, old == new
  mjs "const c=db.getSiblingDB('admin').int_ids; c.drop(); for (let i=1;i<=300;i++){ if(i==296) continue; const at=new Date(($T0+i)*1000); c.insertOne(mkdoc(NumberLong(String(i)),i,at,at,1,'i'+i)); }" >/dev/null
  for b in old new; do
    chq "DROP DATABASE IF EXISTS itest_int_$b"; chq "CREATE DATABASE itest_int_$b"
    gen "i-$b" admin int_ids "itest_int_$b" _id full 10
    local B; [ $b == old ] && B=$OLD || B=$NEW
    srun "$B" "i-$b" -d
  done
  mjs "const c=db.getSiblingDB('admin').int_ids; for (let i of [296,301,302]){ const at=new Date(($T0+i)*1000); c.insertOne(mkdoc(NumberLong(String(i)),i,at,at,1,'i'+i)); }" >/dev/null
  srun "$OLD" i-old -d; srun "$NEW" i-new -d
  local io inw
  io=$(chq "SELECT arrayStringConcat(arrayMap(x -> toString(x), arraySort(groupArray(_id))), ',') FROM itest_int_old.int_ids WHERE _id IN (296,301,302)")
  inw=$(chq "SELECT arrayStringConcat(arrayMap(x -> toString(x), arraySort(groupArray(_id))), ',') FROM itest_int_new.int_ids WHERE _id IN (296,301,302)")
  record 12_integer_ids_old_equals_new new "[$io]" "[$inw]"
  info 12_integer_ids_observed "ids of 296,301,302 present after the increment" "old=[$io] new=[$inw]"

  # 13 datetime key (today's prod config: updated_at + inherited 1h): unchanged
  for b in old new; do
    chq "DROP DATABASE IF EXISTS itest_dt_$b"; chq "CREATE DATABASE itest_dt_$b"
    gen "d-$b" admin $COLL "itest_dt_$b" updated_at none
    local B; [ $b == old ] && B=$OLD || B=$NEW
    srun "$B" "d-$b" -d
  done
  local DMAX D3 D4
  DMAX=$(mjs "db.getSiblingDB('admin').$COLL.find().sort({updated_at:-1}).limit(1).next().updated_at.getTime()")
  D3=$(mjs "idAtOrAfter('admin','$COLL',$((T0 + 7200)))")
  D4=$(mjs "idAtOrAfter('admin','$COLL',$((T0 + 10800)))")
  # the snapshot-race post-state: updated_at moves to just BELOW the watermark
  mjs "db.getSiblingDB('admin').$COLL.updateOne({_id:ObjectId('$D3')},{\$set:{updated_at:new Date($DMAX-1800000)}}); db.getSiblingDB('admin').$COLL.updateOne({_id:ObjectId('$D4')},{\$set:{updated_at:new Date($DMAX-7200000)}})" >/dev/null
  srun "$OLD" d-old -d; srun "$NEW" d-new -d
  for b in old new; do
    record 13_datetime_key_30m_below_watermark $b caught "$([ "$(ch_updated_ms itest_dt_$b "$D3")" == "$((DMAX - 1800000))" ] && echo caught || echo missed)"
    record 13_datetime_key_2h_below_watermark $b missed "$([ "$(ch_updated_ms itest_dt_$b "$D4")" == "$((DMAX - 7200000))" ] && echo caught || echo missed)"
  done

  # 14 config landmine: a stream-level source_options block does NOT inherit
  # the defaults' flatten (SourceOptions.SetDefaults has no Flatten line), so
  # the stream must repeat flatten: 1 next to its incremental_buffer.
  for m in full bufonly; do
    chq "DROP DATABASE IF EXISTS itest_flat_$m"; chq "CREATE DATABASE itest_flat_$m"
    gen "f-$m" admin $COLL "itest_flat_$m" _id "$m" 2h
    srun "$NEW" "f-$m" -d
    record "14_columns_with_${m}_stream_source_options" new \
      "$([ $m == full ] && echo "_id,created_at,sales_call_transcription_id,transcription_data,updated_at" || echo "not-the-prod-shape")" \
      "$(cols="$(chq "SELECT arrayStringConcat(arraySort(groupArray(name)), ',') FROM system.columns WHERE database = 'itest_flat_$m' AND table = '$COLL'")"; [ "$cols" == "_id,created_at,sales_call_transcription_id,transcription_data,updated_at" ] && echo "$cols" || echo "not-the-prod-shape")"
  done
}

# ---------------------------------------------------------------- soak
soak() {
  local SECS=${SOAK_SECS:-240} RATE=${SOAK_RATE:-8} WRITERS=${SOAK_WRITERS:-4}
  echo "== soak: $WRITERS writers x $RATE docs/s for ${SECS}s, syncing old and new back to back"
  mjs "db.getSiblingDB('admin').soak_scs.drop()" >/dev/null
  for b in old new; do chq "DROP DATABASE IF EXISTS itest_soak_$b"; chq "CREATE DATABASE itest_soak_$b"; done
  gen k-old admin soak_scs itest_soak_old _id full 2h
  gen k-new admin soak_scs itest_soak_new _id full 2h
  mjs "seed('admin','soak_scs',200,Math.floor(Date.now()/1000)-3600,3500,1,3000000)" >/dev/null
  srun "$OLD" k-old; srun "$NEW" k-new

  local pids=() w
  for w in $(seq 1 "$WRITERS"); do
    mjs "soakWriter('admin','soak_scs','$(printf '%02x' $((w * 37)))$(openssl rand -hex 4)',$SECS,$RATE,700,$((4000000 + w * 100000)))" >/dev/null &
    pids+=($!)
  done
  local runs=0
  while kill -0 "${pids[@]}" 2>/dev/null; do
    srun "$OLD" k-old; srun "$NEW" k-new
    runs=$((runs + 1))
  done
  wait
  for _ in 1 2; do srun "$OLD" k-old; srun "$NEW" k-new; done
  echo "  sync runs per binary while writing: $runs, total docs: $(mjs "db.getSiblingDB('admin').soak_scs.countDocuments()")"
  info soak_concurrent_writers old "$(sets admin soak_scs itest_soak_old)"
  record soak_concurrent_writers new "missing=0 extra=0 dups=0" "$(sets admin soak_scs itest_soak_new)"
}

# ---------------------------------------------------------------- perf
perf() {
  local N=${PERF_DOCS:-150000} KB=${PERF_KB:-5} NEWDOCS=${PERF_NEW:-2000}
  local now days=150
  now=$(date +%s)
  echo "== perf: $N docs x ${KB}KB over ${days} days, then $NEWDOCS new docs"
  mjs "db.getSiblingDB('perfdb').dropDatabase()" >/dev/null
  chq "DROP DATABASE IF EXISTS perf"; chq "CREATE DATABASE perf"
  local s=$(date +%s)
  mjs "seed('perfdb','$COLL',$N,$((now - days * 86400)),$((days * 86400 - 7200)),$KB,5000000)" >/dev/null
  echo "  seeded in $(($(date +%s) - s))s; collection $(mjs "Math.round(db.getSiblingDB('perfdb').$COLL.stats().size/1048576)") MiB"

  gen p-cur perfdb $COLL perf updated_at none
  # smaller insert batches for this one bulk load only, so the 1.5 GB runner
  # holds it; the measured runs below keep the production default (100000)
  perl -0pi -e 's/(    adjust_column_type: true\n)/$1    batch_limit: 10000\n/' "$RUN_DIR/gen/p-cur.yaml"
  s=$(date +%s)
  srun "$NEW" p-cur || { echo "  initial load failed; perf aborted"; return 1; }
  echo "  initial full load: $(rows_of "$RUN_DIR/logs/p-cur.log") rows in $(($(date +%s) - s))s"
  chq "RENAME TABLE perf.$COLL TO perf.base"
  for t in cur_old cur_new id_new; do
    chq "CREATE TABLE perf.$t AS perf.base"; chq "ALTER TABLE perf.$t ATTACH PARTITION ID 'all' FROM perf.base"
  done

  mjs "for (let i=0;i<$NEWDOCS;i++){ insertAt('perfdb','$COLL',$((now - 7200)) + Math.floor(i*7200/$NEWDOCS),'7a7a7a7a7a',i,6000000+i,'p'+i); }" >/dev/null

  mjs "const d=db.getSiblingDB('perfdb'); d.setProfilingLevel(0); d.system.profile.drop(); d.setProfilingLevel(2)" >/dev/null
  run_perf() { # run_perf <label> <bin> <ch-table> <update_key> <srcopts>
    gen "$1" perfdb $COLL perf "$4" "$5" 2h
    sed -i.bak "s/object: perf.$COLL/object: perf.$3/" "$RUN_DIR/gen/$1.yaml"
    mjs "db.getSiblingDB('perfdb').system.profile.drop()" >/dev/null 2>&1
    local t0=$(date +%s.%N 2>/dev/null || date +%s)
    srun "$2" "$1"
    local q
    q=$(mjs "profileOf('perfdb','$COLL')")
    printf '  %-34s rows=%-6s stream_secs=%-4s %s\n' "$1" "$(rows_of "$RUN_DIR/logs/$1.log")" "$(secs_of "$RUN_DIR/logs/$1.log")" "$q" | tee -a "$RUN_DIR/perf.txt"
  }
  : >"$RUN_DIR/perf.txt"
  run_perf perf-old-updated_at-today "$OLD" cur_old updated_at none
  run_perf perf-new-updated_at-today "$NEW" cur_new updated_at none
  run_perf perf-new-_id-2h "$NEW" id_new _id full
  mjs "db.getSiblingDB('perfdb').setProfilingLevel(0)" >/dev/null
  mjs "dumpIds('perfdb','$COLL')" | sort >"$RUN_DIR/dump/m-perfdb.txt"
  for t in cur_old cur_new id_new; do
    record "perf_${t}_has_all_docs" "$([ $t == cur_old ] && echo old || echo new)" "missing=0 extra=0 dups=0" "$(sets_tbl perfdb perf "$t")"
  done
}

sets_tbl() { # like sets, for a renamed CH table (Mongo ids dumped by the caller)
  local m="$RUN_DIR/dump/m-$1.txt" c="$RUN_DIR/dump/c-$2-$3.txt"
  chq "SELECT _id FROM $2.$3 ORDER BY _id" >"$c"
  echo "missing=$(comm -23 "$m" <(sort -u "$c") | wc -l | tr -d ' ') extra=$(comm -13 "$m" <(sort -u "$c") | wc -l | tr -d ' ') dups=$(($(wc -l <"$c") - $(sort -u "$c" | wc -l)))"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  matrix) : >"$RESULTS"; matrix ;;
  soak) soak ;;
  perf) perf ;;
  all) up && : >"$RESULTS" && matrix && soak && perf ;;
  *) sed -n '2,14p' "$0"; exit 1 ;;
esac
echo "results: $(grep -c ^PASS "$RESULTS" 2>/dev/null) PASS, $(grep -c ^FAIL "$RESULTS" 2>/dev/null) FAIL ($RESULTS)"
