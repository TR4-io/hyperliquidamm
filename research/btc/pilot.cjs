'use strict';
// Single development regression, enabled only for the supplied six-hour October 25 artifact.
const fs = require('node:fs'),
  crypto = require('node:crypto'),
  assert = require('node:assert/strict');
const M = require('./btc-quote-model.cjs'),
  { replay } = require('./btc-account-replay.cjs');
const input = process.argv[2],
  from = Date.parse('2026-10-25T00:00:00Z'),
  to = from + 6 * M.hour;
const bytes = fs.readFileSync(input);
assert.equal(bytes.length, 1294606, 'Unexpected pilot byte length');
const tape = M.readTape(input),
  b = tape.books;
assert.equal(b.length, 4024);
assert.equal(tape.trades.length, 31998);
assert.ok(b[0].time >= from && b.at(-1).time < to);
const inside = tape.trades.filter((t) => t.time >= b[0].time && t.time <= b.at(-1).time);
assert.equal(inside.length, 31986);
const gaps = b
  .slice(1)
  .map((row, i) => row.time - b[i].time)
  .sort((a, b) => a - b);
const median = gaps[Math.floor(gaps.length / 2)],
  max = gaps.at(-1);
assert.equal(median, 5401);
assert.equal(max, 7155);
const out = replay(tape, b[0].time, b.at(-1).time + 1, 'touch', null, {
  allowMissingFunding: true,
});
const { fills, ...summary } = out;
console.log(
  JSON.stringify(
    {
      sha256: crypto.createHash('sha256').update(bytes).digest('hex'),
      snapshots: b.length,
      uniqueTrades: tape.trades.length,
      insideBooks: inside.length,
      medianGapMs: median,
      maxGapMs: max,
      from: b[0].time,
      last: b.at(-1).time,
      previousTouchUserReported: { buys: 144, sells: 137, net: -7.8307, maxDD: 0.000853 },
      replacementTouch: summary,
    },
    null,
    2,
  ),
);
