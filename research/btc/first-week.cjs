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
function checkFunding(folder) {
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
    prices = json(path.join(folder, 'settlement-prices.json'));
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
      !['oraclePx', 'oraclePrice'].includes(p.sourceField)
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
function readPackets(folder) {
  const manifests = new Map(),
    cache = new Map();
  let prev;
  for (let t = start; t < end; t += M.hour) {
    const m = json(path.join(folder, t + '.manifest.json')),
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
    for (const k of ['books', 'venue_blocks']) {
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
  return function interval(a, b) {
    // Include adjacent source objects: file partition time can differ from exchange time.
    const books = new Map(),
      trades = [];
    for (
      let t = Math.max(start, Math.floor(a / M.hour) * M.hour - M.hour);
      t <= Math.min(end - M.hour, Math.floor(b / M.hour) * M.hour + M.hour);
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
  };
}
function train(interval) {
  const model = { version: 1, start, end: cut, config: M.defaults, groups: {} };
  for (let t = start; t < cut; t += M.hour) {
    const a = Math.max(start, t - M.minute),
      b = Math.min(cut, t + M.hour + 2 * M.minute);
    const part = M.fit(
      interval(Math.max(start, a - M.defaults.gap), b),
      a,
      b,
      M.defaults,
      t + M.hour,
    );
    for (const [key, g] of Object.entries(part.groups)) {
      const sum = (model.groups[key] ||= { n: 0, fills: 0, sum: 0 });
      sum.n += g.n;
      sum.fills += g.fills;
      sum.sum += g.sum;
    }
  }
  return model;
}
function study(folder) {
  const interval = readPackets(folder),
    funding = checkFunding(folder);
  const model = train(interval),
    tape = interval(cut, end);
  tape.funding = funding;
  const results = {};
  for (const policy of ['model', 'touch', 'touch15', 'wide3', 'cash'])
    results[policy] = replay(tape, cut, end, policy, model);
  // Sensitivities keep the fitted model fixed. Change only execution and scoring costs.
  for (const [name, opts] of Object.entries({
    doubleSize: { sizeFactor: 2 },
    through1bp: { extraThrough: 1 },
    delay1500: { delay: 1500 },
    feesPlus1bp: { maker: 2.5, taker: 5.5 },
  }))
    results[name] = replay(tape, cut, end, 'model', model, opts);
  return { model, results };
}
module.exports = { study, train };
if (require.main === module) {
  const [dir, out] = process.argv.slice(2);
  const result = study(dir);
  fs.writeFileSync(out, JSON.stringify(result) + '\n', { flag: 'wx' });
  console.log('First training-week chronological replay complete');
}
