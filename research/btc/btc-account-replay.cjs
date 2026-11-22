'use strict';
// Replacement account simulator. No exchange client or signing code.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const M = require('./btc-quote-model.cjs');
const { accountGaps, assertFlat } = require('./replay-coverage.cjs');
const sides = [1, -1];
const isoDay = (t) => new Date(t).toISOString().slice(0, 10);

function closeValue(book, position, c) {
  if (!position) return { gross: 0, fee: 0 };
  let left = Math.abs(position),
    notional = 0;
  for (const { px, sz } of book.levels[position > 0 ? 0 : 1]) {
    const amount = Math.min(left, sz);
    left -= amount;
    notional += amount * px;
    if (left < 1e-12) break;
  }
  if (left > 1e-12) {
    const error = Error('Insufficient displayed exit liquidity');
    error.code = 'UNOBSERVABLE_EXIT';
    error.detail = { bookTime: book.time, position, missingSize: left };
    throw error;
  }
  return { gross: Math.sign(position) * notional, fee: (notional * c.taker) / 1e4 };
}
function fundingRows(rows, start, end, required) {
  const hours = new Set(),
    available = [];
  for (const r of rows) {
    const time = M.timestamp(r.time);
    if (time < start || time >= end) continue;
    const h = Math.floor(time / M.hour);
    if (hours.has(h)) throw Error('Duplicate funding hour');
    hours.add(h);
    const rate = M.numeric(r.rate ?? r.fundingRate);
    const oraclePx = r.oraclePx == null ? null : M.positive(r.oraclePx);
    available.push({ time, rate, oraclePx });
  }
  const missing = [];
  for (let t = Math.ceil(start / M.hour) * M.hour; t < end; t += M.hour) {
    if (!hours.has(t / M.hour)) missing.push(t);
  }
  const unpriced = available.filter((r) => r.oraclePx === null).length;
  if (required && (missing.length || unpriced))
    throw Error(
      'Incomplete funding coverage: ' +
        missing.length +
        ' missing rates, ' +
        unpriced +
        ' missing oracle prices',
    );
  return {
    events: available.filter((r) => r.oraclePx !== null).sort((a, b) => a.time - b.time),
    missingHours: missing,
    unpriced,
    complete: !missing.length && !unpriced,
  };
}
// Each window supplies bounded lookback for features and lookahead for reporting.
// The generator retains the account, peak, working orders, and pending cancellations.
function* accountWindows(start, end, policy, model, options, windowSize, frozenFinal = false) {
  const c = { ...M.defaults, ...options };
  for (const k of ['delay', 'life', 'extraThrough', 'maker', 'taker', 'sizeFactor']) {
    if (!Number.isFinite(c[k]) || c[k] < 0) throw Error('Invalid execution assumption ' + k);
  }
  if (c.delay >= M.minute || c.life <= 0 || c.life > M.minute || c.sizeFactor < 1)
    throw Error('Unsupported execution timing or size');
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(end) ||
    start >= end ||
    !(Number.isSafeInteger(windowSize) && windowSize > 0) ||
    (end > M.finalStart &&
      !(
        frozenFinal &&
        start === M.finalStart &&
        end === Date.parse('2026-12-30Z') &&
        Date.now() >= end
      ))
  )
    throw Error('Only complete development intervals are enabled');
  if (!['model', 'touch', 'touch15', 'wide3', 'cash'].includes(policy))
    throw Error('Unknown policy');
  if (
    policy === 'model' &&
    (!model || model.end > start || model.start < M.training[0] || model.end > M.training[1])
  )
    throw Error('A strictly prior training model is required');
  const flatAcrossGaps = c.coverageMode === 'flatAcrossGaps';
  if (c.coverageMode !== undefined && !flatAcrossGaps && c.coverageMode !== 'strict')
    throw Error('Unknown replay coverage mode');
  if (flatAcrossGaps && end > M.finalStart) throw Error('Gap diagnostic is development only');
  let missingIntervalMs = 0,
    missingMarkouts = 0;
  let book = null,
    books;
  const missingHours = [];
  let unpriced = 0;
  let pos = 0,
    cash = c.capital,
    peak = c.capital,
    maxDD = 0,
    haltTime = null,
    peakAbsInventory = 0;
  let makerFees = 0,
    takerFees = 0,
    fundingPnl = 0,
    inventoryPnl = 0,
    executionPnl = 0;
  let buys = 0,
    sells = 0,
    submitted = 0,
    postOnlyRejected = 0,
    eligible = 0,
    moSum = 0;
  const live = new Map(),
    daily = [],
    fills = [];
  let pending = null,
    decision = Math.ceil((start + M.minute) / M.minute) * M.minute;
  let currentDay = isoDay(start),
    previousClose = c.capital,
    equity = c.capital;
  const dayClose = () => {
    daily.push({
      day: currentDay,
      net: equity - previousClose,
      closeEquity: equity,
      position: pos,
    });
    previousClose = equity;
  };
  function flatten() {
    const x = closeValue(book, pos, c);
    executionPnl += x.gross - pos * (book?.mid || 0);
    cash += x.gross - x.fee;
    takerFees += x.fee;
    pos = 0;
    equity = cash;
    live.clear();
    pending = null;
  }
  function risk(t) {
    const x = closeValue(book, pos, c);
    equity = cash + x.gross - x.fee;
    peak = Math.max(peak, equity);
    maxDD = Math.max(maxDD, 1 - equity / peak);
    if (haltTime === null && 1 - equity / peak >= c.halt) {
      haltTime = t;
      flatten();
    }
  }
  function submit(t) {
    const f = M.features(books, t, c),
      orders = [];
    if (!f || haltTime !== null) return;
    // Reserve same-side in-flight capacity until the cancel reaches the exchange.
    for (const side of sides) {
      const previous = live.get(side);
      const reserved = previous && previous.until > t ? side * c.clip : 0;
      if (Math.abs(pos + reserved + side * c.clip) > c.limit + 1e-12) continue;
      eligible++;
      let px;
      if (policy === 'model') px = M.choose(model, side, f, pos, c)?.px;
      else if (policy === 'wide3') px = M.quotePrice(f.book.mid, side, 3);
      else if (policy.startsWith('touch')) px = side === 1 ? f.book.bid : f.book.ask;
      if (px !== undefined) orders.push({ side, px });
    }
    pending = { at: t + c.delay, orders };
    submitted += orders.length;
  }
  function activate(t) {
    live.clear();
    for (const o of pending.orders) {
      if (M.postOnly(book, o.side, o.px))
        live.set(o.side, { ...o, at: t, until: t + (policy === 'touch15' ? 15000 : c.life) });
      else postOnlyRejected++;
    }
    pending = null;
  }
  for (let segmentStart = start; segmentStart < end; segmentStart += windowSize) {
    const segmentEnd = Math.min(end, segmentStart + windowSize);
    const tape = yield {
      start: segmentStart,
      end: segmentEnd,
      readStart: segmentStart - M.minute - c.gap,
      readEnd: Math.min(end, segmentEnd + c.horizon + 1),
    };
    books = tape.books;
    const trades = tape.trades;
    const gaps = flatAcrossGaps ? accountGaps(tape, segmentStart, segmentEnd, book, c.gap) : [];
    missingIntervalMs += gaps.reduce((n, g) => n + g.end - g.start, 0);
    let nextGap = 0,
      currentGap;
    if (!flatAcrossGaps && tape.venueGaps?.some(([a, b]) => a < segmentEnd && b > segmentStart))
      throw Object.assign(Error('Gap in venue fill coverage at ' + segmentStart), {
        code: 'INCOMPLETE_MARKET_COVERAGE',
      });
    // Hourly checks alone can miss a long gap that straddles a window boundary.
    if (segmentStart > start && !flatAcrossGaps) {
      const next = books[M.indexAfter(books, segmentStart - 1)];
      if (!book || !next || next.time - book.time > c.gap)
        throw Object.assign(Error('Gap across replay windows at ' + segmentStart), {
          code: 'INCOMPLETE_MARKET_COVERAGE',
        });
    } else if (segmentStart === start) {
      book = M.asof(books, start - 1, c.gap);
    }
    if (!flatAcrossGaps) {
      try {
        M.verifyBooks(books, segmentStart, segmentEnd, c);
      } catch (error) {
        error.code = 'INCOMPLETE_MARKET_COVERAGE';
        throw error;
      }
    }
    const funding = fundingRows(tape.funding, segmentStart, segmentEnd, !c.allowMissingFunding);
    missingHours.push(...funding.missingHours);
    unpriced += funding.unpriced;
    let bi = M.indexAfter(books, segmentStart - 1),
      ti = M.indexAfter(trades, segmentStart - 1),
      fi = 0;
    while (true) {
      const t = Math.min(
        books[bi]?.time ?? Infinity,
        trades[ti]?.time ?? Infinity,
        funding.events[fi]?.time ?? Infinity,
        decision,
        pending?.at ?? Infinity,
        gaps[nextGap]?.start ?? Infinity,
      );
      if (t >= segmentEnd) break;
      if (gaps[nextGap]?.start === t) currentGap = gaps[nextGap++];
      if (currentGap && t >= currentGap.end) currentGap = undefined;
      if (currentGap) assertFlat(currentGap, t, pos, live, pending);
      if (isoDay(t) !== currentDay) {
        dayClose();
        currentDay = isoDay(t);
      }
      // Settlement and book updates precede simulated fills at equal timestamps.
      const revalue = books[bi]?.time === t || funding.events[fi]?.time === t;
      if (books[bi]?.time === t) {
        const next = books[bi++];
        if (book) inventoryPnl += pos * (next.mid - book.mid);
        book = next;
      }
      if (funding.events[fi]?.time === t) {
        const r = funding.events[fi++];
        const flow = -pos * r.rate * r.oraclePx;
        cash += flow;
        fundingPnl += flow;
      }
      if (revalue) risk(t);
      if (pending?.at === t) activate(t);
      while (trades[ti]?.time === t) {
        const tr = trades[ti++],
          side = tr.side === 'A' ? 1 : -1,
          o = live.get(side);
        if (!o || t <= o.at || t >= o.until || !M.qualifies(tr, side, o.px, c)) continue;
        live.delete(side);
        pos = Number((pos + side * c.clip).toFixed(8));
        if (Math.abs(pos) > c.limit + 1e-12) throw Error('Inventory cap violated');
        peakAbsInventory = Math.max(peakAbsInventory, Math.abs(pos));
        const fee = (c.clip * o.px * c.maker) / 1e4;
        cash -= side * c.clip * o.px + fee;
        makerFees += fee;
        executionPnl += side * c.clip * (book.mid - o.px);
        // Ex-post reporting only. This value is never read by the policy.
        const ref = M.asof(books, t + c.horizon, c.gap);
        if ((!ref && !flatAcrossGaps) || t + c.horizon >= end)
          throw Error('Truncated fill markout');
        const mo = ref ? M.markout(ref, side, o.px) : null;
        fills.push({ time: t, side, px: o.px, markoutBps: mo });
        if (mo === null) missingMarkouts++;
        else moSum += mo;
        if (side === 1) buys++;
        else sells++;
        risk(t);
      }
      if (t === decision) {
        if (t + c.delay + c.life + c.horizon < end) submit(t);
        decision += M.minute;
        if (currentGap) assertFlat(currentGap, t, pos, live, pending);
      }
    }
  }
  flatten();
  dayClose();
  const net = cash - c.capital;
  const markouts = fills.length - missingMarkouts;
  assert.ok(
    Math.abs(net - (inventoryPnl + executionPnl - makerFees - takerFees + fundingPnl)) <
      1e-6 * Math.max(1, daily.length),
  );
  return {
    policy,
    start,
    end,
    initialEquity: c.capital,
    finalEquity: cash,
    net,
    maxDD,
    haltTime,
    peakAbsInventory,
    eligible,
    submitted,
    postOnlyRejected,
    buys,
    sells,
    mean30sGrossBps: markouts ? moSum / markouts : null,
    mean30sAfterMakerBps: markouts ? moSum / markouts - c.maker : null,
    ...(flatAcrossGaps
      ? {
          diagnosticCoverage: {
            missingIntervalMs,
            missingMarkouts,
            observationsComplete: missingIntervalMs === 0,
          },
        }
      : {}),
    inventoryPnl,
    executionPnl,
    makerFees,
    takerFees,
    fundingPnl,
    fundingCoverage: {
      complete: missingHours.length === 0 && unpriced === 0,
      missingHours,
      missingOraclePrices: unpriced,
    },
    daily,
    fills,
  };
}
function replay(tape, start, end, policy, model = null, options = {}) {
  const state = accountWindows(start, end, policy, model, options, end - start);
  state.next();
  return state.next(tape).value;
}
function selfTest() {
  const boundary = Date.parse('2026-01-01Z'),
    start = boundary - 120000,
    end = boundary + 180000;
  const books = Array.from({ length: 60 }, (_, i) => ({
    time: start + i * 5000,
    levels: [[{ px: 9999, sz: 1 }], [{ px: 10001, sz: 1 }]],
  }));
  const trades = [{ tid: 1, time: start + 60600, px: 9998, sz: 1, side: 'A' }];
  const raw = {
    books,
    trades,
    funding: [{ time: boundary + 19, rate: 0.001, oraclePx: 10000, markPx: 8000 }],
  };
  const run = (r, policy = 'touch', c = {}) => replay(M.prepare(r), start, end, policy, null, c);
  const r = run(raw);
  assert.equal(r.buys, 1);
  assert.equal(r.sells, 0);
  assert.equal(r.daily[0].position, 0.001);
  assert.equal(r.daily[1].position, 0);
  assert.equal(r.fundingPnl, -0.01);
  assert.ok(Math.abs(r.net - (-0.01 - (9.999 * (1.5 + 4.5)) / 10000)) < 1e-9);
  assert.equal(run(raw, 'cash').net, 0);
  assert.deepEqual(
    run(raw, 'cash').daily.map((x) => x.net),
    [0, 0],
  );
  assert.throws(() => run({ ...raw, funding: [] }), /funding coverage/);
  assert.throws(
    () => run({ ...raw, funding: [{ time: boundary, rate: 0.01, markPx: 10000 }] }),
    /oracle/,
  );
  const halted = run(raw, 'touch', { capital: 0.06 });
  assert.ok(halted.maxDD >= 0.1);
  assert.ok(halted.haltTime !== null);
  assert.equal(halted.buys, 1);
  assert.equal(run({ ...raw, trades: [{ ...trades[0], time: start + 60500 }] }).buys, 0);
  assert.equal(
    run({ ...raw, trades: [{ ...trades[0], time: start + 120501 }] }, 'touch15').buys,
    1,
  );
  assert.equal(run({ ...raw, trades: [{ ...trades[0], time: start + 76000 }] }, 'touch15').buys, 0);
  const crossed = structuredClone(raw);
  crossed.books.push({
    time: start + 60300,
    levels: [[{ px: 9996, sz: 1 }], [{ px: 9998, sz: 1 }]],
  });
  assert.equal(run(crossed).postOnlyRejected, 1);
  assert.equal(run(crossed).buys, 0);
  assert.deepEqual(
    closeValue(
      {
        levels: [
          [
            { px: 100, sz: 1 },
            { px: 90, sz: 2 },
          ],
          [],
        ],
      },
      2,
      { taker: 0 },
    ),
    { gross: 190, fee: 0 },
  );
  console.log('account-replay replacement self-test passed');
}
module.exports = { replay, fundingRows, accountWindows };
if (require.main === module) {
  const [cmd, file, from, to, policy, modelPath, ...flags] = process.argv.slice(2);
  if (cmd === 'self-test') selfTest();
  else if (cmd === 'replay') {
    const model = policy === 'model' ? JSON.parse(fs.readFileSync(modelPath)) : null;
    const allowMissingFunding = [modelPath, ...flags].includes(
      '--development-without-full-funding',
    );
    console.log(
      JSON.stringify(
        replay(M.readTape(file), Date.parse(from), Date.parse(to), policy, model, {
          allowMissingFunding,
        }),
      ),
    );
  } else
    throw Error(
      'Use self-test or replay TAPE START_ISO END_ISO POLICY [MODEL] [--development-without-full-funding]',
    );
}
