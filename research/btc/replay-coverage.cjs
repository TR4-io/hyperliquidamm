'use strict';

const M = require('./btc-quote-model.cjs');

// Intervals are used only to flag accounting or reporting uncertainty.
// A gap never changes order selection, cancellation, execution or sizing.
function accountGaps(tape, start, end, priorBook, limit) {
  const gaps = [];
  function add(a, b, source) {
    a = Math.max(start, a);
    b = Math.min(end, b);
    if (a < b) gaps.push({ start: a, end: b, sources: [source] });
  }
  const books = tape.books;
  const first = M.indexAfter(books, start - 1);
  let prior = books[first - 1]?.time ?? priorBook?.time;
  for (let i = first; i < books.length; i++) {
    const t = books[i].time;
    if (prior === undefined || t - prior > limit)
      add(prior === undefined ? start : prior + 1, t, 'books');
    prior = t;
    if (t >= end) break;
  }
  if (prior === undefined || end - prior > limit)
    add(prior === undefined ? start : prior + 1, end, 'books');
  if (!Array.isArray(tape.venueGaps)) throw Error('Gap diagnostic requires a venue coverage audit');
  for (const [a, b] of tape.venueGaps) {
    if (!Number.isSafeInteger(a) || !Number.isSafeInteger(b) || a >= b)
      throw Error('Invalid venue gap');
    add(a, b, 'venue fills');
  }
  const merged = [];
  gaps.sort((a, b) => a.start - b.start);
  for (const gap of gaps) {
    const prev = merged.at(-1);
    if (prev && gap.start <= prev.end) {
      prev.end = Math.max(prev.end, gap.end);
      for (const source of gap.sources)
        if (!prev.sources.includes(source)) prev.sources.push(source);
    } else merged.push(gap);
  }
  return merged;
}

function assertFlat(gap, time, position, live, pending) {
  if (!gap || time >= gap.end) return;
  const active = [...live.values()].filter((o) => o.until > time).map((o) => o.side);
  const inFlight = pending?.orders.map((o) => o.side) ?? [];
  if (position === 0 && !active.length && !inFlight.length) return;
  const error = Error(
    'Account exposure overlaps missing observations at ' + new Date(time).toISOString(),
  );
  error.code = 'UNOBSERVABLE_ACCOUNT';
  error.detail = { time, gap, position, liveSides: active, pendingSides: inFlight };
  throw error;
}

module.exports = { accountGaps, assertFlat };
