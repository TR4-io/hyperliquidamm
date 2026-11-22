'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');
const crypto = require('node:crypto');
const M = require('./btc-quote-model.cjs');
const { replay } = require('./btc-account-replay.cjs');
const {
  fitDevelopment,
  replayWindows,
  confirmedFunding,
  score,
  freeze,
  verifiedFees,
  verifyLock,
  readyThrough,
} = require('./annual.cjs');

function replayFixture() {
  const boundary = Date.parse('2026-01-02Z'),
    start = boundary - 2 * M.minute,
    end = boundary + 4 * M.minute;
  const raw = { books: [], trades: [] };
  for (let t = start; t < end; t += 5000) {
    const mid = 10000 + Math.floor((t - start) / M.minute);
    raw.books.push({
      time: t,
      levels: [[{ px: mid - 1, sz: 1 }], [{ px: mid + 1, sz: 1 }]],
    });
  }
  const times = [start + 61000, boundary + 100, boundary + 61500, boundary + 122000];
  times.forEach((time, i) =>
    raw.trades.push({
      time,
      tid: i,
      side: i % 2 ? 'B' : 'A',
      px: i % 2 ? 10020 : 9980,
      sz: 1,
    }),
  );
  const funding = [{ time: boundary + 9, rate: 0.01, oraclePx: 10000 }];
  const tape = M.prepare({ ...raw, funding });
  const cases = { touch: ['touch', {}], cash: ['cash', {}] };
  // First fill takes place before midnight, then the old ask fills inside cancel delay.
  const ref = replay(tape, start, end, 'touch');
  assert.equal(ref.daily[0].position, 0.001);
  assert.ok(ref.fundingPnl < 0);
  for (const chunk of [M.minute, 60100, 75500, M.hour]) {
    const outputs = replayWindows(
      (lo, hi) => ({
        books: tape.books.filter((r) => lo <= r.time && r.time < hi),
        trades: tape.trades.filter((r) => lo <= r.time && r.time < hi),
      }),
      funding,
      start,
      end,
      null,
      cases,
      chunk,
    );
    assert.deepEqual(outputs.touch, ref);
    assert.deepEqual(outputs.cash, replay(tape, start, end, 'cash'));
  }
  // Each chunk independently passes the edge-distance check at midnight; the seam spans 15 s.
  const absent = raw.books.filter((b) => b.time !== boundary && b.time !== boundary - 5000);
  assert.throws(
    () =>
      replayWindows(
        (lo, hi) => ({
          ...tape,
          books: M.prepare({ books: absent, trades: [] }).books.filter(
            (r) => lo <= r.time && r.time < hi,
          ),
        }),
        funding,
        start,
        end,
        null,
        cases,
        M.minute,
      ),
    /Gap/,
  );
}
function fitFixture(tmp) {
  const source = path.join(tmp, 'first-week');
  fs.mkdirSync(source);
  const start = M.training[0],
    end = start + 2 * M.day;
  const sha = (b) => crypto.createHash('sha256').update(b).digest('hex');
  const refBooks = [],
    refTrades = [];
  for (let t = start; t < end; t += M.hour) {
    const books = [],
      trades = [],
      d = new Date(t),
      stamp = d.toISOString().slice(0, 10).replaceAll('-', '');
    for (let x = t; x < t + M.hour; x += 10000) {
      const mid = 10000 + (((x - t) / 10000) % 20);
      books.push({
        time: x,
        levels: [[{ px: mid - 1, sz: 1 }], [{ px: mid + 1, sz: 1 }]],
      });
      if (x % M.minute === 0)
        trades.push({
          time: x + 1000,
          tid: String(x),
          side: 'A',
          px: 9980,
          sz: 1,
        });
    }
    refBooks.push(...books);
    refTrades.push(...trades);
    const compressed = zlib.gzipSync(JSON.stringify({ books, trades }));
    const p = path.join(source, String(t));
    fs.writeFileSync(p + '.json.gz', compressed);
    const clock = {
      rows: books.length,
      first: t,
      last: t + M.hour - 10000,
      max_gap_ms: 10000,
    };
    fs.writeFileSync(
      p + '.manifest.json',
      JSON.stringify({
        hour: t,
        books: clock,
        venue_blocks: clock,
        sources: [
          [
            'hyperliquid-archive',
            'market_data/' + stamp + '/' + d.getUTCHours() + '/l2Book/BTC.lz4',
          ],
          [
            'hl-mainnet-node-data',
            'node_fills_by_block/hourly/' + stamp + '/' + d.getUTCHours() + '.lz4',
          ],
        ].map(([bucket, key]) => ({
          bucket,
          key,
          bytes: 9,
          sha256: '0'.repeat(64),
        })),
        unique_takers: trades.length,
        packet_sha256: sha(compressed),
        packet_bytes: compressed.length,
      }),
    );
  }
  const work = path.join(tmp, 'check');
  const prefix = fitDevelopment(tmp, work, start + M.day, end);
  assert.ok(prefix.end > prefix.decisionEnd);
  assert.equal(prefix.complete, false);
  const full = fitDevelopment(tmp, work, end, end);
  assert.deepEqual(full, fitDevelopment(tmp, work, end, end));
  const fit = M.fit(M.prepare({ books: refBooks, trades: refTrades }), start, end);
  assert.equal(full.excludedDecisions, fit.excludedDecisions);
  assert.deepEqual(Object.keys(full.groups).sort(), Object.keys(fit.groups).sort());
  for (const [key, g] of Object.entries(full.groups)) {
    assert.equal(g.n, fit.groups[key].n);
    assert.equal(g.fills, fit.groups[key].fills);
    assert.ok(Math.abs(g.sum - fit.groups[key].sum) < 1e-6);
  }
  const changed = path.join(source, String(start) + '.manifest.json');
  const original = JSON.parse(fs.readFileSync(changed));
  const v = original.venue_blocks;
  v.max_gap_ms = 50000;
  v.gaps_gt_10s = [[start + M.minute, start + M.minute + 50000]];
  fs.writeFileSync(changed, JSON.stringify(original));
  const { readPackets } = require('./first-week.cjs');
  assert.deepEqual(
    readPackets(source, start + M.hour).interval(start, start + 2 * M.minute).venueGaps,
    v.gaps_gt_10s,
  );
  v.gaps_gt_10s = [];
  fs.writeFileSync(changed, JSON.stringify(original));
  assert.throws(() => readPackets(source, start + M.hour));
  assert.throws(() => fitDevelopment(tmp, work, end, end), /Changed checkpoint source/);
}
function freezeFixture(tmp) {
  const base = {
    maker: 1.5,
    taker: 4.5,
    verifiedStudyPeriod: true,
    officialUrl: 'https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees',
    sourceSha256: 'f'.repeat(64),
  };
  const [from, middle] = M.training,
    to = Date.parse('2026-12-30Z');
  const periods = [
    { ...base, from, to: middle },
    { ...base, from: middle, to },
  ];
  verifiedFees({ periods });
  assert.throws(() => verifiedFees({ periods: [periods[0]] }), /Incomplete/);
  assert.throws(
    () => verifiedFees({ periods: [periods[0], { ...periods[1], from: middle + 1 }] }),
    /Gap/,
  );
  assert.throws(
    () => verifiedFees({ periods: [periods[0], { ...periods[1], maker: 0 }] }),
    /differ/,
  );
  const file = path.join(tmp, 'development-lock.json');
  const lock = {
    version: 1,
    specification: { horizon: 30 },
    code: { x: 'y' },
    modelSha256: 'model',
    validationSha256: 'val',
  };
  fs.writeFileSync(file, JSON.stringify(lock));
  const seal = {
    ...lock,
    developmentLockSha256: crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex'),
  };
  verifyLock(seal, file);
  assert.throws(
    () => verifyLock({ ...seal, modelSha256: 'replaced' }, file),
    /Changed locked field/,
  );
  fs.appendFileSync(file, ' ');
  assert.throws(() => verifyLock(seal, file), /Changed development lock/);
}
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'btc-year-fixture-'));
try {
  replayFixture();
  fitFixture(tmp);
  assert.equal(readyThrough(tmp), M.training[0] + M.day);
  freezeFixture(tmp);
  const p = path.join(tmp, 'funding');
  fs.writeFileSync(
    p,
    JSON.stringify([
      {
        time: M.training[0],
        rate: 0,
        markPx: 1,
        oraclePx: 1,
        priceEvidence: 'minute-sampled context proxy',
      },
    ]),
  );
  assert.throws(() => confirmedFunding(p, M.training[0], M.training[0] + M.hour), /Unverified/);
  // Simulate a pre-completion clock without accessing any historical test-period packet.
  const real = Date.now;
  try {
    Date.now = () => M.finalStart;
    assert.throws(() => score(tmp, 'unused', 'unused', 'unused', 'unused'), /sealed/);
    assert.throws(() => freeze(tmp, 'unused', 'unused', 'unused', 'unused', 'unused'), /future/);
  } finally {
    Date.now = real;
  }
  console.log('Annual fit, resume, continuous replay, funding and seal fixtures passed');
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}
