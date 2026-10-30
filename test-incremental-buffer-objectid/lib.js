// mongosh helpers for the ObjectId incremental_buffer harness.
// Loaded with load('/harness/lib.js') inside the Mongo container.

// ObjectId with an explicit timestamp, 10-hex "process" part and counter, so a
// test controls exactly how ids sort: <4B unix seconds><5B process><3B counter>.
function oidAt(tsSec, proc10, counter) {
  const ts = Math.floor(tsSec).toString(16).padStart(8, '0');
  const cnt = (counter & 0xffffff).toString(16).padStart(6, '0');
  return ObjectId(ts + proc10 + cnt);
}

function tsOf(oid) {
  return parseInt(oid.toHexString().substring(0, 8), 16);
}

// Words drawn from a small vocabulary keep the payload realistic text that
// compresses about as well as a transcript does, not a run of one character.
const WORDS = ('hello yes no course fees placement batch mentor doubt class ' +
  'recording schedule payment emi refund demo python java data science ' +
  'interview offer salary package support thanks okay sure right well').split(' ');

function turns(kb, tag, seed) {
  const out = [];
  let size = 0;
  let i = 0;
  let x = (seed * 2654435761) >>> 0;
  while (size < kb * 1024) {
    let text = tag + ' ';
    for (let w = 0; w < 24; w++) {
      x = (x * 1103515245 + 12345) >>> 0;
      text += WORDS[x % WORDS.length] + ' ';
    }
    out.push({ speaker: i % 2 ? 'agent' : 'customer', text: text, start: i * 7.5, end: i * 7.5 + 7 });
    size += text.length + 48;
    i++;
  }
  return out;
}

function mkdoc(id, sctId, created, updated, kb, tag) {
  return {
    _id: id,
    sales_call_transcription_id: NumberLong(String(sctId)),
    transcription_data: turns(kb, tag, sctId),
    created_at: created,
    updated_at: updated,
  };
}

// n docs evenly spaced over [t0, t0+span) seconds, round-robin over 4 app
// processes, payload kb each; the very last doc is pinned to t0+span with the
// highest process bytes so a later same-second insert can sort below it.
function seed(dbName, coll, n, t0, span, kb, sctBase) {
  const c = db.getSiblingDB(dbName)[coll];
  const procs = ['1a2b3c4d5e', '2b3c4d5e6f', '3c4d5e6f70', '4d5e6f7081'];
  let batch = [];
  for (let i = 0; i < n; i++) {
    const last = i === n - 1;
    const ts = last ? t0 + span : t0 + Math.floor((i * span) / n);
    const id = last ? oidAt(ts, 'ffffffffff', 1) : oidAt(ts, procs[i % 4], i);
    const at = new Date(ts * 1000);
    batch.push(mkdoc(id, sctBase + i, at, at, kb, 'seed' + i));
    if (batch.length === 1000) {
      c.insertMany(batch, { ordered: false });
      batch = [];
    }
  }
  if (batch.length) c.insertMany(batch, { ordered: false });
  return c.countDocuments();
}

// Insert one doc whose ObjectId says it was generated at tsSec by proc10.
function insertAt(dbName, coll, tsSec, proc10, counter, sctId, tag) {
  const at = new Date(tsSec * 1000);
  const id = oidAt(tsSec, proc10, counter);
  db.getSiblingDB(dbName)[coll].insertOne(mkdoc(id, sctId, at, at, 2, tag));
  return id.toHexString();
}

// Re-save a doc the way NinjasTool does (new transcription_data + updated_at).
function resave(dbName, coll, idHex, tag) {
  const now = new Date();
  db.getSiblingDB(dbName)[coll].updateOne(
    { _id: ObjectId(idHex) },
    { $set: { transcription_data: turns(2, tag, 7), updated_at: now } }
  );
  return now.getTime();
}

function maxId(dbName, coll) {
  return db.getSiblingDB(dbName)[coll].find({}, { _id: 1 }).sort({ _id: -1 }).limit(1).next()._id;
}

// First doc whose ObjectId time is at least tsSec (used to pick update targets).
function idAtOrAfter(dbName, coll, tsSec) {
  return db.getSiblingDB(dbName)[coll]
    .find({ _id: { $gte: oidAt(tsSec, '0000000000', 0) } }, { _id: 1 })
    .sort({ _id: 1 }).limit(1).next()._id.toHexString();
}

function dumpIds(dbName, coll) {
  db.getSiblingDB(dbName)[coll].find({}, { _id: 1 }).forEach(function (d) {
    print(d._id && d._id.toHexString ? d._id.toHexString() : String(d._id));
  });
}

function dumpUpdated(dbName, coll) {
  db.getSiblingDB(dbName)[coll].find({}, { _id: 1, updated_at: 1 }).forEach(function (d) {
    print(d._id.toHexString() + '\t' + d.updated_at.getTime());
  });
}

// Concurrent writer for the soak test: generates ids in real time under its
// own process bytes, waits a random 0..maxDelayMs (the gap between Mongoid
// building the doc and save! committing it), then inserts.
function soakWriter(dbName, coll, proc10, seconds, perSec, maxDelayMs, sctBase) {
  const c = db.getSiblingDB(dbName)[coll];
  const end = Date.now() + seconds * 1000;
  let n = 0;
  while (Date.now() < end) {
    const genMs = Date.now();
    const id = oidAt(Math.floor(genMs / 1000), proc10, n);
    sleep(Math.floor(Math.random() * maxDelayMs));
    const at = new Date();
    c.insertOne(mkdoc(id, sctBase + n, at, at, 1, 'soak-' + proc10 + '-' + n));
    n++;
    const wait = Math.floor(1000 / perSec) - (Date.now() - genMs);
    if (wait > 0) sleep(wait);
  }
  return n;
}


// Server-side cost of the last incremental find (non-empty filter) on a
// collection, summed over the find and every getMore of its cursor. Needs
// profiling level 2 on that database.
function profileOf(dbName, coll) {
  const ops = db.getSiblingDB(dbName).system.profile.find({ ns: dbName + '.' + coll }).sort({ ts: 1 }).toArray();
  const finds = ops.filter(o => o.op === 'query' && o.command && o.command.filter && Object.keys(o.command.filter).length);
  if (!finds.length) return 'no-incremental-find';
  const f = finds[finds.length - 1];
  const more = ops.filter(o => o.op === 'getmore' && f.cursorid && o.cursorid && o.cursorid.toString() === f.cursorid.toString());
  const all = [f].concat(more);
  const sum = k => all.reduce((a, o) => a + (o[k] || 0), 0);
  return [f.planSummary, 'docsExamined=' + sum('docsExamined'), 'keysExamined=' + sum('keysExamined'),
    'nreturned=' + sum('nreturned'), 'mongo_ms=' + sum('millis'), 'getMores=' + more.length].join(' ');
}
