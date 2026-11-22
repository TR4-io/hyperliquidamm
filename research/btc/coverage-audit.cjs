'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const zlib = require('node:zlib');
const assert = require('node:assert/strict');
const M = require('./btc-quote-model.cjs');

const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex');
const policySource = fs.readFileSync(require.resolve('./btc-quote-model.cjs'), 'utf8');
const originalPolicyHash = '57bbdd04ce04f32f5e6f27efa597222df4e6a389269f85b4b9c414b42f8426e7';
// A change to the bins, scorer or model representation requires a new proof.
// Ignore export-only changes so the prediction auditor can use the same model.
const reviewedCoreHash = 'a503b244b462b21585dd6d8d4fab54863c2b885a1e2742366c2ef6ef0d680a60';

function certifyFlat(model) {
  assert.equal(sha(policySource.split('module.exports = {')[0]), reviewedCoreHash);
  assert.equal(model.version, 1);
  assert.deepEqual(model.config, M.defaults);
  assert.ok(model.start >= M.training[0] && model.start < model.end && model.end <= M.training[1]);
  assert.ok([originalPolicyHash, sha(policySource)].includes(model.modelCodeSha256));
  for (const g of Object.values(model.groups)) {
    assert.ok(Number.isSafeInteger(g.n) && g.n > 0);
    assert.ok(Number.isSafeInteger(g.fills) && g.fills >= 0 && g.fills <= g.n);
    assert.ok(Number.isFinite(g.sum) && (g.fills || g.sum === 0));
  }
  const activeStates = [];
  let states = 0;
  // keys() reads only 3 imbalance bins, 3 return bins and 2 volatility bins.
  // Midpoint affects the rounded limit price only after a positive score is found.
  for (const side of [1, -1]) {
    for (const depth of M.defaults.depths) assert.ok(model.groups[side + '/' + depth]);
    for (const imb of [-0.5, 0, 0.5]) {
      for (const ret of [-3, 0, 3]) {
        for (const vol of [2, 5]) {
          const f = { imb, ret, vol, book: { mid: 100000 } };
          const action = M.choose(model, side, f, 0, model.config);
          states++;
          if (action)
            activeStates.push({ side, imb, ret, vol, depth: action.depth, score: action.score });
        }
      }
    }
  }
  // If no first order is reachable at zero inventory, no subsequent fill,
  // funding charge, halt or position is reachable for this fixed policy.
  return { states, activeStates, cashEquivalentFromFlat: activeStates.length === 0 };
}

function summarizeBookClock(times, start, end, limit) {
  assert.ok(start % M.day === 0 && end % M.day === 0 && start < end && limit > 0);
  const days = [];
  for (let t = start; t < end; t += M.day) {
    days.push({ date: new Date(t).toISOString().slice(0, 10), staleMs: 0, snapshots: 0 });
  }
  let prior = null;
  let longIntervals = 0;
  let maxBetweenBooksMs = 0;
  let staleAtStartMs = 0;
  function addStale(a, b) {
    a = Math.max(a, start);
    b = Math.min(b, end);
    while (a < b) {
      const index = Math.floor((a - start) / M.day);
      const next = Math.min(start + (index + 1) * M.day, b);
      days[index].staleMs += next - a;
      a = next;
    }
  }
  for (const t of times) {
    assert.ok(Number.isSafeInteger(t));
    if (prior !== null && t < prior) throw Error('Unsorted book clock');
    if (t < start) {
      prior = t;
      continue;
    }
    if (t >= end) break;
    if (t === prior) continue;
    days[Math.floor((t - start) / M.day)].snapshots++;
    if (prior === null) {
      staleAtStartMs = t - start;
      addStale(start, t);
    } else {
      maxBetweenBooksMs = Math.max(maxBetweenBooksMs, t - prior);
      if (t - prior > limit) {
        longIntervals++;
        addStale(prior + limit, t);
      }
    }
    prior = t;
  }
  if (prior === null) staleAtStartMs = end - start;
  addStale(prior === null ? start : prior + limit, end);
  return {
    days,
    longIntervals,
    maxBetweenBooksMs,
    staleAtStartMs,
    staleMs: days.reduce((n, x) => n + x.staleMs, 0),
  };
}

function inspectWeek(folder) {
  const start = M.training[0],
    end = start + 7 * M.day;
  const times = [];
  const suspectVenueHours = [];
  let previousVenueEnd;
  const manifestDigest = crypto.createHash('sha256');
  for (let h = start; h < end; h += M.hour) {
    const manifestBytes = fs.readFileSync(path.join(folder, h + '.manifest.json'));
    const m = JSON.parse(manifestBytes);
    const bytes = fs.readFileSync(path.join(folder, h + '.json.gz'));
    assert.equal(m.hour, h);
    assert.equal(bytes.length, m.packet_bytes);
    assert.equal(sha(bytes), m.packet_sha256);
    manifestDigest.update(sha(manifestBytes));
    const { books } = JSON.parse(zlib.gunzipSync(bytes));
    assert.equal(books.length, m.books.rows);
    assert.equal(books[0].time, m.books.first);
    assert.equal(books.at(-1).time, m.books.last);
    for (const b of books) times.push(b.time);
    const venue = m.venue_blocks;
    if (
      venue.max_gap_ms > M.defaults.gap ||
      Math.abs(venue.first - h) > M.defaults.gap ||
      Math.abs(venue.last - h - M.hour) > M.defaults.gap ||
      (previousVenueEnd !== undefined && venue.first - previousVenueEnd > M.defaults.gap)
    ) {
      suspectVenueHours.push({ hourStart: h, ...venue });
    }
    previousVenueEnd = venue.last;
  }
  times.sort((a, b) => a - b);
  return {
    start,
    end,
    freshnessLimitMs: M.defaults.gap,
    hourlyManifestSha256: manifestDigest.digest('hex'),
    ...summarizeBookClock(times, start, end, M.defaults.gap),
    suspectVenueHours,
  };
}

function observedWeek(folder, modelFile) {
  const { readPackets, checkFunding } = require('./first-week.cjs');
  const { accountWindows } = require('./btc-account-replay.cjs');
  const bytes = fs.readFileSync(modelFile);
  const model = JSON.parse(bytes);
  const certificate = certifyFlat(model);
  const start = M.training[0] + 5 * M.day,
    end = start + 2 * M.day;
  assert.equal(model.start, M.training[0]);
  assert.equal(model.end, start);
  const packets = readPackets(folder, end);
  assert.equal(model.trainingManifestSha256, packets.trainingManifestSha256);
  const funding = checkFunding(folder, true);
  const results = {},
    runs = new Map();
  for (const policy of ['model', 'touch', 'touch15', 'wide3', 'cash']) {
    const account = accountWindows(
      start,
      end,
      policy,
      model,
      { coverageMode: 'flatAcrossGaps' },
      M.hour,
    );
    runs.set(policy, { account, next: account.next() });
  }
  while (runs.size) {
    const window = runs.values().next().value.next.value;
    const tape = packets.interval(Math.max(start, window.readStart), Math.min(end, window.readEnd));
    tape.funding = funding.filter((row) => row.time >= window.start && row.time < window.end);
    for (const [policy, run] of runs) {
      assert.deepEqual(run.next.value, window);
      try {
        run.next = run.account.next(tape);
      } catch (e) {
        if (!['UNOBSERVABLE_ACCOUNT', 'UNOBSERVABLE_EXIT'].includes(e.code)) throw e;
        results[policy] = { status: 'unobservable', reason: e.code, ...e.detail };
        runs.delete(policy);
        continue;
      }
      if (run.next.done) {
        results[policy] = { status: 'completed', ...run.next.value };
        runs.delete(policy);
      }
    }
  }
  return {
    start,
    end,
    modelSha256: sha(bytes),
    certificate,
    eligibleForPromotion: false,
    fundingPrices: 'minute-sampled context proxy',
    policyInput: 'observed feed including outages; decisions use past information only',
    results,
  };
}

function selfTest() {
  const day = M.training[0],
    b = day + M.day,
    end = b + M.day;
  const clock = summarizeBookClock([b - 12000, b + 1000, b + 1000, b + 11000], day, end, 10000);
  assert.equal(clock.longIntervals, 1);
  assert.equal(clock.maxBetweenBooksMs, 13000);
  assert.equal(clock.staleAtStartMs, M.day - 12000);
  assert.equal(clock.days[0].staleMs, M.day - 10000);
  assert.equal(clock.days[1].staleMs, M.day - 20000);
  assert.equal(clock.days[1].snapshots, 2);
  assert.equal(summarizeBookClock([], day, b, 10000).staleMs, M.day);
  assert.equal(summarizeBookClock([day - 1000, day + 1000], day, b, 10000).staleMs, M.day - 11000);
  const model = {
    start: day,
    end: b,
    version: 1,
    config: M.defaults,
    modelCodeSha256: originalPolicyHash,
    groups: {},
  };
  for (const side of [1, -1]) {
    for (const depth of M.defaults.depths) {
      model.groups[side + '/' + depth] = { n: 100, fills: 30, sum: -300 };
    }
  }
  assert.deepEqual(certifyFlat(model), {
    states: 36,
    activeStates: [],
    cashEquivalentFromFlat: true,
  });
  model.groups['-1/6/-1/1/1'] = { n: 100, fills: 100, sum: 100000 };
  // The hierarchy uses the coarse group before the leaf.
  model.groups['-1/6/-1'] = { n: 100, fills: 30, sum: -300 };
  const active = certifyFlat(model);
  assert.equal(active.cashEquivalentFromFlat, false);
  assert.equal(active.activeStates.length, 1);
  assert.deepEqual([active.activeStates[0].side, active.activeStates[0].depth], [-1, 6]);
  console.log('Coverage and cash-equivalence fixtures passed');
}

module.exports = { certifyFlat, summarizeBookClock, inspectWeek };

if (require.main === module) {
  const [mode, file, modelFile] = process.argv.slice(2);
  if (mode === 'self-test') selfTest();
  else if (mode === 'books') console.log(JSON.stringify(inspectWeek(file), null, 2));
  else if (mode === 'replay') console.log(JSON.stringify(observedWeek(file, modelFile), null, 2));
  else if (mode === 'flat') {
    const bytes = fs.readFileSync(file);
    console.log(
      JSON.stringify({ modelSha256: sha(bytes), ...certifyFlat(JSON.parse(bytes)) }, null, 2),
    );
  } else throw Error('Use self-test, books FOLDER, flat MODEL or replay FOLDER MODEL');
}
