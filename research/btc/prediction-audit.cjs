'use strict';
// Training-week diagnostic. Score every covered candidate before any quote selection.
const fs = require('node:fs');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const M = require('./btc-quote-model.cjs');
const { readPackets } = require('./first-week.cjs');

function totals() {
  return { count: 0, fills: 0, unsupported: 0, filledMarkout: 0, predictions: {} };
}

function record(g, observed, forecasts) {
  g.count++;
  const filled = observed !== null;
  if (filled) {
    g.fills++;
    g.filledMarkout += observed;
  }
  if (!forecasts) {
    g.unsupported++;
    return;
  }
  for (const [name, { p, mu }] of Object.entries(forecasts)) {
    const s = (g.predictions[name] ||= {
      n: 0,
      fills: 0,
      probabilities: 0,
      brier: 0,
      squaredError: 0,
      absoluteError: 0,
      bias: 0,
    });
    s.n++;
    s.probabilities += p;
    s.brier += (p - Number(filled)) ** 2;
    if (filled) {
      s.fills++;
      const error = mu - observed;
      s.squaredError += error ** 2;
      s.absoluteError += Math.abs(error);
      s.bias += error;
    }
  }
}

function summary(g, fee) {
  const predictions = {};
  for (const [name, s] of Object.entries(g.predictions)) {
    predictions[name] = {
      scored: s.n,
      scoredFills: s.fills,
      meanPredictedFillProbability: s.probabilities / s.n,
      actualScoredFillProbability: s.fills / s.n,
      brier: s.brier / s.n,
      markoutRmseBps: s.fills ? Math.sqrt(s.squaredError / s.fills) : null,
      markoutMaeBps: s.fills ? s.absoluteError / s.fills : null,
      markoutForecastMinusActualBps: s.fills ? s.bias / s.fills : null,
    };
  }
  return {
    opportunities: g.count,
    fills: g.fills,
    unsupported: g.unsupported,
    grossMarkoutBps: g.fills ? g.filledMarkout / g.fills : null,
    afterMakerMarkoutBps: g.fills ? g.filledMarkout / g.fills - fee : null,
    predictions,
  };
}

function audit(model, interval, start, end) {
  const c = model.config;
  assert.ok(model.end <= start && start < end && end <= M.training[1], 'Training-only evaluation');
  assert.ok(start % M.hour === 0 && end % M.hour === 0);
  const pooled = totals(),
    groups = {},
    days = {};
  const exclusions = { feature: 0, futureBooks: 0, venueFills: 0 };
  for (let hour = start; hour < end; hour += M.hour) {
    // Each UTC minute belongs to exactly one hour. Context can span source objects.
    const from = Math.max(start, hour - M.minute - c.gap);
    const to = Math.min(end, hour + M.hour + c.delay + c.life + c.horizon + 1);
    const tape = interval(from, to);
    for (let t = Math.max(hour, start + M.minute); t < hour + M.hour; t += M.minute) {
      const labelEnd = t + c.delay + c.life + c.horizon;
      if (labelEnd >= end) continue;
      const f = M.features(tape.books, t, c);
      if (!f) {
        exclusions.feature++;
        continue;
      }
      if (tape.venueGaps?.some(([lo, hi]) => lo < labelEnd && hi > t)) {
        exclusions.venueFills++;
        continue;
      }
      if (!M.hasForwardBookCoverage(tape.books, t, labelEnd, c.gap)) {
        exclusions.futureBooks++;
        continue;
      }
      const date = new Date(t).toISOString().slice(0, 10);
      const daily = (days[date] ||= totals());
      for (const side of [1, -1]) {
        for (const depth of c.depths) {
          const key = side + '/' + depth;
          const bucket = (groups[key] ||= totals());
          const fitted = M.estimate(model, side, depth, f, c);
          const parent = model.groups[key];
          const forecasts = fitted && {
            fitted,
            sideDepth: { p: parent.fills / parent.n, mu: parent.sum / parent.fills },
          };
          const px = M.quotePrice(f.book.mid, side, depth);
          const observed = M.label(tape, t, side, px, c);
          for (const g of [pooled, daily, bucket]) record(g, observed, forecasts);
        }
      }
    }
  }
  const summaries = (groups) =>
    Object.fromEntries(Object.entries(groups).map(([key, g]) => [key, summary(g, c.maker)]));
  return {
    start,
    end,
    source: 'Covered UTC decision minutes in the held-back training interval',
    excludedMinutes: exclusions,
    pooled: summary(pooled, c.maker),
    bySideDepth: summaries(groups),
    byUtcDay: summaries(days),
  };
}

function selfTest() {
  const c = M.defaults;
  const f = { imb: 0.6, ret: -3, vol: 6, book: { mid: 60000 } };
  const groups = {
    '1/3': { n: 1000, fills: 200, sum: 500 },
    '1/3/1': { n: 100, fills: 30, sum: 120 },
    '1/3/1/-1/1': { n: 1, fills: 1, sum: 8 },
  };
  const model = { groups };
  const e = M.estimate(model, 1, 3, f, c);
  assert.equal(e.p, (1 + 100 * (50 / 200)) / 101);
  assert.equal(e.mu, (8 + 10 * (145 / 40)) / 11);
  const buy = M.choose(model, 1, f, 0, c);
  assert.equal(buy.depth, 3);
  assert.ok(Math.abs(buy.score - e.p * (e.mu - 1.5 - 0.1)) < 1e-12);
  assert.equal(M.choose(model, 1, f, c.limit, c), null);
  assert.ok(M.choose(model, 1, f, -0.005, c).score > buy.score);
  assert.equal(M.choose(model, 1, f, 0, { ...c, maker: 5 }), null);
  assert.equal(M.estimate(model, -1, 3, f, c), null);
  const g = totals();
  for (const observed of [2, null]) record(g, observed, { f: { p: 0.5, mu: 1 } });
  const s = summary(g, 1.5);
  assert.equal(s.predictions.f.brier, 0.25);
  assert.equal(s.predictions.f.markoutRmseBps, 1);
  assert.equal(s.predictions.f.markoutForecastMinusActualBps, -1);
  assert.equal(s.afterMakerMarkoutBps, 0.5);

  // Altering snapshots and trades after t cannot alter a decision at t.
  const t = M.training[0] + M.minute;
  const makeTape = (future) =>
    M.prepare({
      books: Array.from({ length: 33 }, (_, i) => {
        const time = M.training[0] + 5000 * i;
        const mid = time <= t ? 60000 : future;
        return { time, levels: [[{ px: mid - 1, sz: 2 }], [{ px: mid + 1, sz: 1 }]] };
      }),
      trades: [{ tid: 1, side: 'A', px: future - 100, sz: 1, time: t + 1500 }],
    });
  assert.deepEqual(
    M.features(makeTape(60000).books, t, c),
    M.features(makeTape(62000).books, t, c),
  );
  assert.notEqual(
    M.label(makeTape(60000), t, 1, 59980, c),
    M.label(makeTape(62000), t, 1, 59980, c),
  );
  const from = M.training[0] + M.hour,
    to = from + M.hour;
  const tape = M.prepare({
    books: Array.from({ length: 720 }, (_, i) => ({
      time: from + i * 5000,
      levels: [[{ px: 59999, sz: 1 }], [{ px: 60001, sz: 1 }]],
    })),
    trades: [
      { tid: 1, time: from + M.minute + 501, px: 59900, sz: 1, side: 'A' },
      { tid: 2, time: to - 100, px: 59900, sz: 1, side: 'A' },
    ],
  });
  const fixed = { end: from, config: c, groups };
  const result = audit(fixed, () => tape, from, to);
  assert.deepEqual(result.excludedMinutes, { feature: 0, futureBooks: 0, venueFills: 0 });
  assert.equal(result.pooled.opportunities, 58 * 6);
  assert.equal(result.pooled.fills, 3);
  assert.equal(result.pooled.unsupported, 58 * 5);
  tape.venueGaps = [[from + 61000, from + 62000]];
  const incomplete = audit(fixed, () => tape, from, to);
  assert.equal(incomplete.excludedMinutes.venueFills, 1);
  assert.equal(incomplete.pooled.opportunities, 57 * 6);
  assert.equal(incomplete.pooled.fills, 0);
  const impossible = () => {
    throw Error('Should not read out-of-bounds interval');
  };
  assert.throws(() => audit({ end: t, config: c }, impossible, t - 1, t + M.hour), /Training-only/);
  console.log('prediction audit self-test passed');
}

module.exports = { audit };

if (require.main === module) {
  const [folder, modelPath, resultPath] = process.argv.slice(2);
  if (folder === 'self-test') selfTest();
  else {
    if (!folder || !modelPath || !resultPath) throw Error('Use FOLDER MODEL RESULT or self-test');
    const modelBytes = fs.readFileSync(modelPath);
    const model = JSON.parse(modelBytes);
    const hash = (b) => crypto.createHash('sha256').update(b).digest('hex');
    const start = M.training[0] + 5 * M.day;
    const end = start + 2 * M.day;
    assert.equal(model.version, 1);
    assert.equal(model.start, M.training[0]);
    assert.equal(model.end, start);
    assert.deepEqual(model.config, M.defaults);
    assert.equal(
      model.modelCodeSha256,
      hash(fs.readFileSync(require.resolve('./btc-quote-model.cjs'))),
    );
    const packets = readPackets(folder, end);
    assert.equal(model.trainingManifestSha256, packets.trainingManifestSha256);
    const result = audit(model, packets.interval, start, end);
    result.modelSha256 = hash(modelBytes);
    fs.writeFileSync(resultPath, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  }
}
