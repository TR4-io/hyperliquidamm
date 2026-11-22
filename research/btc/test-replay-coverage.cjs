'use strict';

const assert = require('node:assert/strict');
const { accountWindows, replay } = require('./btc-account-replay.cjs');
const M = require('./btc-quote-model.cjs');
const start = M.training[0],
  end = start + 5 * M.minute;
const makeBook = (time) => ({
  time: start + time,
  levels: [[{ px: 9999, sz: 1 }], [{ px: 10001, sz: 1 }]],
});
const raw = {
  books: Array.from({ length: 60 }, (_, i) => makeBook(i * 5000)),
  trades: [],
  funding: [{ time: start + 3, rate: 0.001, oraclePx: 10000 }],
};
const guarded = { coverageMode: 'flatAcrossGaps' };
function withGap(left, right, fills = []) {
  return M.prepare({
    ...raw,
    trades: fills,
    books: raw.books.filter((b) => b.time <= start + left || b.time >= start + right),
  });
}
const touched = [{ tid: '1', time: start + 60600, px: 9998, sz: 0.001, side: 'A' }];
function run(tape, policy, size = end - start) {
  const gen = accountWindows(start, end, policy, null, guarded, size);
  let state = gen.next();
  while (!state.done) {
    const { readStart, readEnd } = state.value;
    state = gen.next({
      venueGaps: [],
      ...tape,
      books: tape.books.filter((b) => b.time >= readStart && b.time < readEnd),
      trades: tape.trades.filter((t) => t.time >= readStart && t.time < readEnd),
    });
  }
  return state.value;
}
function unknown(thunk, check) {
  assert.throws(thunk, (e) => e.code === 'UNOBSERVABLE_ACCOUNT' && check(e.detail));
}
const flatTape = withGap(80000, 105000);
assert.throws(() => replay(flatTape, start, end, 'cash'), /Gap/);
const cash = run(flatTape, 'cash');
assert.equal(cash.net, 0);
assert.deepEqual(cash.diagnosticCoverage, {
  missingIntervalMs: 24999,
  missingMarkouts: 0,
  observationsComplete: false,
});
assert.deepEqual(run(flatTape, 'cash', 90000), cash);
assert.deepEqual(run(flatTape, 'touch15', 90000), run(flatTape, 'touch15'));
const safe = run(flatTape, 'touch15');
assert.equal(safe.net, 0);
assert.ok(safe.submitted > 0);
unknown(
  () => run(flatTape, 'touch'),
  (e) => e.position === 0 && e.liveSides.length === 2,
);
unknown(
  () => run(withGap(80000, 105000, touched), 'touch15'),
  (e) => e.position === 0.001 && !e.liveSides.length,
);
unknown(
  () => run(withGap(60000, 85000), 'touch'),
  (e) => e.time === start + 60001 && !e.liveSides.length && e.pendingSides.length === 2,
);
const venueHole = { ...M.prepare(raw), venueGaps: [[start + 59000, start + 64000]] };
// A future outage cannot suppress the first actual quote. A quote submitted into this gap censors the run.
unknown(
  () => run(venueHole, 'touch'),
  (e) => e.time === start + 60000 && e.pendingSides.length === 2,
);
const overlap = { ...flatTape, venueGaps: [[start + 90000, start + 115000]] };
assert.equal(run(overlap, 'cash').diagnosticCoverage.missingIntervalMs, 34999);
const complete = M.prepare({ ...raw, trades: touched });
const { diagnosticCoverage, ...same } = run(complete, 'touch');
assert.equal(diagnosticCoverage.observationsComplete, true);
assert.deepEqual(same, replay(complete, start, end, 'touch'));
const shallow = structuredClone(raw);
shallow.trades = touched;
shallow.books.find((b) => b.time === start + 65000).levels[0][0].sz = 0.0002;
assert.throws(
  () => run(M.prepare(shallow), 'touch'),
  (e) => e.code === 'UNOBSERVABLE_EXIT' && e.detail.position === 0.001,
);
console.log('Continuous gap-accounting fixtures passed');
