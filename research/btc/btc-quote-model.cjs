'use strict';
// Replacement implementation. All times are exchange milliseconds, intervals [start,end).
const fs = require('node:fs');
const zlib = require('node:zlib');
const assert = require('node:assert/strict');

const minute = 60000,
  hour = 60 * minute,
  day = 24 * hour;
const training = [Date.parse('2025-12-30Z'), Date.parse('2026-08-27Z')];
const finalStart = Date.parse('2026-10-26Z');
const defaults = Object.freeze({
  delay: 500,
  life: minute,
  horizon: 30000,
  gap: 10000,
  clip: 0.001,
  limit: 0.01,
  maker: 1.5,
  taker: 4.5,
  sizeFactor: 1,
  extraThrough: 0,
  depths: [1.5, 3, 6],
  minOpp: 100,
  minFills: 10,
  priorOpp: 100,
  priorFills: 10,
  riskBps: 1,
  capital: 10000,
  halt: 0.1,
});

function indexAfter(rows, t) {
  let lo = 0,
    hi = rows.length;
  while (lo < hi) {
    const m = (lo + hi) >>> 1;
    if (rows[m].time <= t) lo = m + 1;
    else hi = m;
  }
  return lo;
}
function numeric(x) {
  if (
    !['number', 'string'].includes(typeof x) ||
    String(x).trim() === '' ||
    !Number.isFinite(Number(x))
  )
    throw Error('Expected finite number');
  return Number(x);
}
function timestamp(x) {
  const t = numeric(x);
  if (t < 0 || !Number.isSafeInteger(t)) throw Error('Invalid time');
  return t;
}
function positive(x) {
  const v = numeric(x);
  if (v <= 0) throw Error('Non-positive price or size');
  return v;
}
function prepare(raw) {
  const books = raw.books
    .map((b) => {
      if (b.levels.length !== 2) throw Error('Invalid book');
      const levels = b.levels.map((side, s) => {
        if (!side.length) throw Error('Empty book');
        const result = side.map((l) => ({ px: positive(l.px), sz: positive(l.sz) }));
        for (let i = 1; i < result.length; i++)
          if ((result[i - 1].px - result[i].px) * (s ? -1 : 1) <= 0) throw Error('Unordered book');
        return result;
      });
      const [bid, ask] = levels.map((side) => side[0].px);
      if (bid >= ask) throw Error('Crossed book');
      const [bv, av] = levels.map((side) => side.slice(0, 5).reduce((sum, l) => sum + l.sz, 0));
      return {
        time: timestamp(b.time),
        levels,
        bid,
        ask,
        mid: (bid + ask) / 2,
        imbalance: (bv - av) / (bv + av),
      };
    })
    .sort((a, b) => a.time - b.time);
  if (!books.length || books.some((b, i) => i && b.time === books[i - 1].time))
    throw Error('Empty or duplicate book time');
  const seen = new Map();
  for (const r of raw.trades) {
    if (!((typeof r.tid === 'string' && r.tid.length) || Number.isSafeInteger(r.tid)))
      throw Error('Invalid trade id');
    const t = {
      tid: String(r.tid),
      time: timestamp(r.time),
      px: positive(r.px),
      sz: positive(r.sz),
      side: r.side,
    };
    if (!['A', 'B'].includes(t.side)) throw Error('Invalid aggressor side');
    const prev = seen.get(t.tid);
    if (prev && ['time', 'px', 'sz', 'side'].some((k) => prev[k] !== t[k]))
      throw Error('Conflicting trade ID');
    seen.set(t.tid, t);
  }
  return {
    books,
    trades: [...seen.values()].sort((a, b) => a.time - b.time),
    funding: raw.funding || [],
  };
}
function readTape(path) {
  const bytes = fs.readFileSync(path);
  return prepare(JSON.parse(path.endsWith('.gz') ? zlib.gunzipSync(bytes) : bytes));
}
function asof(books, t, gap) {
  const b = books[indexAfter(books, t) - 1];
  return b && t - b.time <= gap ? b : null;
}
function features(books, t, c) {
  const start = indexAfter(books, t - minute) - 1,
    end = indexAfter(books, t) - 1;
  if (start < 0 || t - minute - books[start].time > c.gap || t - books[end].time > c.gap)
    return null;
  let variance = 0;
  for (let i = start + 1; i <= end; i++) {
    if (books[i].time - books[i - 1].time > c.gap) return null;
    variance += Math.log(books[i].mid / books[i - 1].mid) ** 2;
  }
  return {
    book: books[end],
    ret: 1e4 * Math.log(books[end].mid / books[start].mid),
    vol: 1e4 * Math.sqrt(variance),
    imb: books[end].imbalance,
  };
}
// BTC szDecimals=5: at most one decimal for perp prices, five significant figures,
// and integer prices always permitted. Choose the finest valid grid.
function quotePrice(mid, side, depth) {
  const raw = mid * (1 - (side * depth) / 10000);
  const tick = Math.min(1, Math.max(0.1, 10 ** (Math.floor(Math.log10(raw)) - 4)));
  return Number(((side === 1 ? Math.floor(raw / tick) : Math.ceil(raw / tick)) * tick).toFixed(1));
}
function postOnly(book, side, px) {
  return book && (side === 1 ? px < book.ask : px > book.bid);
}
function qualifies(trade, side, px, c) {
  return (
    trade.side === (side === 1 ? 'A' : 'B') &&
    trade.sz >= c.clip * c.sizeFactor &&
    side * (px - trade.px) > (px * c.extraThrough) / 1e4
  );
}
function markout(book, side, px) {
  return (1e4 * side * (book.mid - px)) / px;
}
function label(tape, t, side, px, c) {
  const arrival = t + c.delay,
    cancel = arrival + c.life;
  if (postOnly(asof(tape.books, arrival, c.gap), side, px)) {
    for (
      let i = indexAfter(tape.trades, arrival);
      i < tape.trades.length && tape.trades[i].time < cancel;
      i++
    ) {
      const tr = tape.trades[i];
      if (qualifies(tr, side, px, c)) {
        const b = asof(tape.books, tr.time + c.horizon, c.gap);
        if (!b) throw Error('Missing markout book');
        return markout(b, side, px);
      }
    }
  }
  return null;
}
function keys(side, depth, f) {
  // Fixed feature bins; direction-adjusted return and imbalance.
  const bucket = (v, cut) => (v < -cut ? -1 : v > cut ? 1 : 0);
  const parent = side + '/' + depth;
  const coarse = parent + '/' + bucket(side * f.imb, 0.25);
  return [parent, coarse, coarse + '/' + bucket(side * f.ret, 2) + '/' + (f.vol > 4 ? 1 : 0)];
}
function verifyBooks(books, start, end, c) {
  let prior = start;
  for (let i = indexAfter(books, start - 1); i < books.length && books[i].time < end; i++) {
    if (books[i].time - prior > c.gap) throw Error('Gap in book coverage at ' + prior);
    prior = books[i].time;
  }
  if (end - prior > c.gap) throw Error('Incomplete book interval ending ' + end);
}
function hasForwardBookCoverage(books, t, end, gap) {
  let i = indexAfter(books, t);
  let prior = books[i - 1]?.time;
  if (prior == null || t - prior > gap) return false;
  for (; i < books.length && books[i].time <= end; i++) {
    if (books[i].time - prior > gap) return false;
    prior = books[i].time;
  }
  return end - prior <= gap;
}
function fit(tape, start, end, c = defaults, decisionEnd = end, allowBookGaps = false) {
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start < training[0] ||
    end > training[1] ||
    start >= end
  )
    throw Error('Fit outside training interval');
  if (!allowBookGaps) verifyBooks(tape.books, start, end, c);
  const groups = {};
  let excludedDecisions = 0;
  for (
    let t = Math.ceil((start + minute) / minute) * minute;
    t < decisionEnd && t + c.delay + c.life + c.horizon < end;
    t += minute
  ) {
    const f = features(tape.books, t, c);
    const labelEnd = t + c.delay + c.life + c.horizon;
    const missingTrades = tape.venueGaps?.some(([lo, hi]) => lo < labelEnd && hi > t);
    if (
      !f ||
      missingTrades ||
      (allowBookGaps &&
        !hasForwardBookCoverage(tape.books, t, t + c.delay + c.life + c.horizon, c.gap))
    ) {
      excludedDecisions++;
      continue;
    }
    for (const side of [1, -1])
      for (const depth of c.depths) {
        const mo = label(tape, t, side, quotePrice(f.book.mid, side, depth), c);
        for (const key of keys(side, depth, f)) {
          const g = (groups[key] ||= { n: 0, fills: 0, sum: 0 });
          g.n++;
          if (mo !== null) {
            g.fills++;
            g.sum += mo;
          }
        }
      }
  }
  return { version: 1, start, end, config: c, groups, excludedDecisions };
}
function estimate(model, side, depth, f, c) {
  let p, mu;
  for (const key of keys(side, depth, f)) {
    const g = model.groups[key];
    if (!g) break;
    if (p === undefined) {
      if (g.n < c.minOpp || g.fills < c.minFills) return null;
      p = g.fills / g.n;
      mu = g.sum / g.fills;
    } else {
      p = (g.fills + c.priorOpp * p) / (g.n + c.priorOpp);
      mu = (g.sum + c.priorFills * mu) / (g.fills + c.priorFills);
    }
  }
  return p === undefined ? null : { p, mu };
}
function choose(model, side, f, inventory, c) {
  let best = null;
  if (Math.abs(inventory + side * c.clip) > c.limit + 1e-12) return null;
  const risk =
    (c.riskBps * ((inventory + side * c.clip) ** 2 - inventory ** 2)) / (c.limit * c.clip);
  for (const depth of c.depths) {
    const e = estimate(model, side, depth, f, c);
    if (!e) continue;
    const score = e.p * (e.mu - c.maker - risk);
    if (score > 0 && (!best || score > best.score))
      best = { depth, score, px: quotePrice(f.book.mid, side, depth) };
  }
  return best;
}
function selfTest() {
  const c = defaults,
    start = training[0],
    t = start + minute;
  const books = Array.from({ length: 61 }, (_, i) => ({
    time: start + i * 5000,
    levels: [[{ px: 9999, sz: 1 }], [{ px: 10001, sz: 1 }]],
  }));
  const px = quotePrice(10000, 1, 3);
  const trade = (tid, ms, price, sz, side) => ({ tid, time: t + ms, px: price, sz, side });
  const a = trade(1, 500, px - 1, 1, 'A'),
    b = trade(2, 501, px, 1, 'A'),
    d = trade(3, 502, px - 1, 0.0001, 'A');
  const valid = trade(5, 1100, px - 1, 0.001, 'A');
  const tape = prepare({ books, trades: [a, b, d, trade(4, 600, px - 1, 1, 'B'), valid, valid] });
  assert.equal(tape.trades.length, 5);
  assert.throws(() => prepare({ books, trades: [valid, { ...valid, sz: 1 }] }), /Conflicting/);
  assert.equal(label(tape, t, 1, px, c), (1e4 * (10000 - px)) / px);
  assert.equal(label({ ...tape, trades: tape.trades.slice(0, 4) }, t, 1, px, c), null);
  const model = fit(tape, start, start + 300000, c);
  assert.deepEqual([model.groups['1/3'].n, model.groups['1/3'].fills], [3, 1]);
  const missing = {
    ...tape,
    books: tape.books.filter((b) => b.time < t + 10000 || b.time > t + 35000),
  };
  assert.throws(() => fit(missing, start, start + 300000, c), /Gap/);
  const observed = fit(missing, start, start + 300000, c, start + 300000, true);
  assert.equal(observed.excludedDecisions, 2);
  assert.deepEqual([observed.groups['1/3'].n, observed.groups['1/3'].fills], [1, 0]);
  assert.equal(choose(model, 1, features(tape.books, t, c), 0, c), null);
  assert.throws(() => fit(tape, finalStart, finalStart + day), /outside training/);
  assert.throws(() => verifyBooks(tape.books.slice(4), start, start + 300000, c), /Gap/);
  assert.equal(quotePrice(111111, 1, 0), 111111);
  assert.equal(quotePrice(11000.42, -1, 0), 11001);
  console.log('quote-model replacement self-test passed');
}
module.exports = {
  minute,
  hour,
  day,
  training,
  finalStart,
  defaults,
  positive,
  timestamp,
  numeric,
  prepare,
  readTape,
  asof,
  features,
  indexAfter,
  quotePrice,
  postOnly,
  qualifies,
  markout,
  verifyBooks,
  fit,
  choose,
};
if (require.main === module) {
  const [cmd, file, start, end] = process.argv.slice(2);
  if (cmd === 'self-test') selfTest();
  else if (cmd === 'fit')
    console.log(JSON.stringify(fit(readTape(file), Date.parse(start), Date.parse(end))));
  else throw Error('Use self-test or fit TAPE START_ISO END_ISO');
}
