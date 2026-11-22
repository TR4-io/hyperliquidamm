'use strict';
const fs = require('node:fs'),
  path = require('node:path'),
  crypto = require('node:crypto');
const zlib = require('node:zlib'),
  assert = require('node:assert/strict');
const M = require('./btc-quote-model.cjs'),
  { replay } = require('./btc-account-replay.cjs');
const start = M.training[0],
  cut = start + 5 * M.day,
  end = start + 7 * M.day;
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex');
const json = (file) => JSON.parse(fs.readFileSync(file));
function checkFunding(folder, useContext = false) {
  const rates = fs.readFileSync(path.join(folder, 'fundingHistory.raw.json'));
  const m = json(path.join(folder, 'fundingHistory.manifest.json'));
  assert.equal(m.source, 'https://api.hyperliquid.xyz/info');
  assert.deepEqual(m.request, {
    type: 'fundingHistory',
    coin: 'BTC',
    startTime: start,
    endTime: end - 1,
  });
  assert.equal(m.sha256, sha(rates));
  const rows = JSON.parse(rates),
    prices = json(path.join(folder, useContext ? 'context-prices.json' : 'settlement-prices.json'));
  assert.equal(rows.length, 168);
  assert.equal(prices.length, 168);
  return rows.map((r, i) => {
    assert.equal(r.coin, 'BTC');
    assert.equal(Math.floor((r.time - start) / M.hour), i);
    const p = prices[i];
    if (
      p.time !== r.time ||
      !p.sourceKey ||
      !/^[a-f0-9]{64}$/.test(p.sourceSha256) ||
      (useContext
        ? p.priceEvidence !== 'minute-sampled context proxy' ||
          p.sourceBucket !== 'hyperliquid-archive' ||
          p.sourceField !== 'oracle_px' ||
          p.sampleTime !== Math.floor(r.time / M.hour) * M.hour
        : !['oraclePx', 'oraclePrice'].includes(p.sourceField))
    )
      throw Error('Unverified settlement-price join at ' + r.time);
    return {
      time: r.time,
      rate: M.numeric(r.fundingRate),
      oraclePx: M.positive(p.oraclePx),
      markPx: M.positive(p.markPx),
    };
  });
}
function readPackets(folder, sourceEnd) {
  const manifests = new Map(),
    cache = new Map();
  const trainingManifest = crypto.createHash('sha256');
  let prev;
  for (let t = start; t < sourceEnd; t += M.hour) {
    const rawManifest = fs.readFileSync(path.join(folder, t + '.manifest.json'));
    if (t < cut) trainingManifest.update(sha(rawManifest));
    const m = JSON.parse(rawManifest),
      date = new Date(t);
    const stamp = date.toISOString().slice(0, 10).replaceAll('-', ''),
      h = date.getUTCHours();
    assert.deepEqual(
      m.sources.map((r) => [r.bucket, r.key]),
      [
        ['hyperliquid-archive', 'market_data/' + stamp + '/' + h + '/l2Book/BTC.lz4'],
        ['hl-mainnet-node-data', 'node_fills_by_block/hourly/' + stamp + '/' + h + '.lz4'],
      ],
    );
    for (const r of m.sources) assert.ok(r.bytes > 0 && /^[0-9a-f]{64}$/.test(r.sha256));
    assert.equal(m.hour, t);
    const book = m.books;
    assert.ok(book.rows > 0 && book.first <= book.last && book.max_gap_ms >= 0);
    for (const k of ['venue_blocks']) {
      const r = m[k];
      if (
        !(
          r.rows > 0 &&
          r.max_gap_ms >= 0 &&
          r.max_gap_ms <= M.defaults.gap &&
          Math.abs(r.first - t) <= M.defaults.gap &&
          Math.abs(r.last - t - M.hour) <= M.defaults.gap
        )
      )
        throw Error('Insufficient ' + k + ' coverage: ' + t);
      if (prev && Math.abs(r.first - prev[k].last) > M.defaults.gap) throw Error('Hour seam: ' + t);
    }
    manifests.set(t, m);
    prev = m;
  }
  function packet(t) {
    if (!cache.has(t)) {
      const bytes = fs.readFileSync(path.join(folder, t + '.json.gz'));
      const m = manifests.get(t);
      assert.equal(bytes.length, m.packet_bytes);
      assert.equal(sha(bytes), m.packet_sha256);
      const raw = JSON.parse(zlib.gunzipSync(bytes));
      assert.equal(raw.books.length, m.books.rows);
      assert.equal(raw.trades.length, m.unique_takers);
      cache.set(t, raw);
      if (cache.size > 4) cache.delete(cache.keys().next().value);
    }
    return cache.get(t);
  }
  function interval(a, b) {
    // Include adjacent source objects: file partition time can differ from exchange time.
    const books = new Map(),
      trades = [];
    for (
      let t = Math.max(start, Math.floor(a / M.hour) * M.hour - M.hour);
      t <= Math.min(sourceEnd - M.hour, Math.floor(b / M.hour) * M.hour + M.hour);
      t += M.hour
    ) {
      const raw = packet(t);
      for (const book of raw.books)
        if (book.time >= a && book.time < b) {
          const old = books.get(book.time);
          if (old) assert.deepEqual(old, book);
          else books.set(book.time, book);
        }
      for (const trade of raw.trades) if (trade.time >= a && trade.time < b) trades.push(trade);
    }
    return M.prepare({ books: [...books.values()], trades });
  }
  return { interval, trainingManifestSha256: trainingManifest.digest('hex') };
}
function train(interval) {
  const model = {
    version: 1,
    start,
    end: cut,
    config: M.defaults,
    groups: {},
    excludedDecisions: 0,
  };
  for (let t = start; t < cut; t += M.hour) {
    const a = Math.max(start, t - M.minute),
      b = Math.min(cut, t + M.hour + 2 * M.minute);
    const part = M.fit(
      interval(Math.max(start, a - M.defaults.gap), b),
      a,
      b,
      M.defaults,
      t + M.hour,
      true,
    );
    model.excludedDecisions += part.excludedDecisions;
    for (const [key, g] of Object.entries(part.groups)) {
      const sum = (model.groups[key] ||= { n: 0, fills: 0, sum: 0 });
      sum.n += g.n;
      sum.fills += g.fills;
      sum.sum += g.sum;
    }
  }
  return model;
}
function fitWeek(folder) {
  const packets = readPackets(folder, cut);
  return {
    ...train(packets.interval),
    trainingManifestSha256: packets.trainingManifestSha256,
    modelCodeSha256: sha(fs.readFileSync(require.resolve('./btc-quote-model.cjs'))),
  };
}
function longestBookRun(books, start, end, gap) {
  let best = [start, start],
    runStart = Math.max(start, books[0].time - gap),
    prior = books[0].time;
  for (const b of books.slice(1)) {
    if (b.time - prior > gap) {
      if (prior + 1 - runStart > best[1] - best[0]) best = [runStart, prior + 1];
      runStart = b.time;
    }
    prior = b.time;
  }
  const last = end - prior <= gap ? end : prior + 1;
  return last - runStart > best[1] - best[0] ? [runStart, last] : best;
}
function study(folder, model = fitWeek(folder), developmentDiagnostic = false) {
  assert.equal(model.version, 1);
  assert.equal(model.start, start);
  assert.equal(model.end, cut);
  assert.deepEqual(model.config, M.defaults);
  assert.equal(
    model.modelCodeSha256,
    sha(fs.readFileSync(require.resolve('./btc-quote-model.cjs'))),
  );
  const packets = readPackets(folder, end);
  assert.equal(
    model.trainingManifestSha256,
    packets.trainingManifestSha256,
    'Training manifest changed',
  );
  const funding = checkFunding(folder, developmentDiagnostic);
  const tape = packets.interval(cut, end);
  tape.funding = funding;
  const [from, to] = developmentDiagnostic
    ? longestBookRun(tape.books, cut, end, M.defaults.gap)
    : [cut, end];
  if (to - from < 2 * M.minute) throw Error('No usable chronological replay interval');
  const coverage = {
    requestedStart: cut,
    requestedEnd: end,
    replayStart: from,
    replayEnd: to,
    fullBookWindow: from === cut && to === end,
    fundingPrices: developmentDiagnostic
      ? 'minute-sampled context proxy'
      : 'verified settlement oracle',
    eligibleForPromotion: false,
  };
  const results = {};
  for (const policy of ['model', 'touch', 'touch15', 'wide3', 'cash'])
    results[policy] = replay(tape, from, to, policy, model);
  // Sensitivities keep the fitted model fixed. Change only execution and scoring costs.
  for (const [name, opts] of Object.entries({
    doubleSize: { sizeFactor: 2 },
    through1bp: { extraThrough: 1 },
    delay1500: { delay: 1500 },
    feesPlus1bp: { maker: 2.5, taker: 5.5 },
  }))
    results[name] = replay(tape, from, to, 'model', model, opts);
  return { model, coverage, results };
}
module.exports = { study, train, fitWeek };
if (require.main === module) {
  const args = process.argv.slice(2);
  let result;
  if (args[0] === 'fit' && args.length === 3) result = fitWeek(args[1]);
  else if (args[0] === 'replay' && args.length === 4) result = study(args[1], json(args[2]));
  else if (args[0] === 'diagnose' && args.length === 4)
    result = study(args[1], json(args[2]), true);
  else if (args.length === 2) result = study(args[0]);
  else
    throw Error(
      'Use FOLDER RESULT, fit FOLDER MODEL, replay FOLDER MODEL RESULT, or diagnose FOLDER MODEL RESULT',
    );
  fs.writeFileSync(args.at(-1), JSON.stringify(result) + '\n', { flag: 'wx' });
  console.log(
    args[0] === 'fit'
      ? 'First five training days fitted'
      : 'First training-week chronological replay complete',
  );
}
