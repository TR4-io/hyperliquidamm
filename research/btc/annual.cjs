'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const M = require('./btc-quote-model.cjs');
const { accountWindows } = require('./btc-account-replay.cjs');
const { readPackets, train } = require('./first-week.cjs');

const [trainStart, trainEnd] = M.training;
const finalStart = M.finalStart;
const finalEnd = Date.parse('2026-12-30Z');
const oldPilotEnd = trainStart + 7 * M.day;
const policies = Object.freeze({
  model: ['model', {}],
  touch: ['touch', {}],
  touch15: ['touch15', {}],
  wide3: ['wide3', {}],
  cash: ['cash', {}],
  doubleSize: ['model', { sizeFactor: 2 }],
  through1bp: ['model', { extraThrough: 1 }],
  delay1500: ['model', { delay: 1500 }],
  feesPlus1bp: ['model', { maker: 2.5, taker: 5.5 }],
});
const specification = {
  trainStart,
  trainEnd,
  validationEnd: finalStart,
  finalEnd,
  config: M.defaults,
  policies,
  primary:
    'mean daily net USDC against cash; empirical 5th percentile via circular block bootstrap',
  bootstrap: {
    block: 7,
    sensitivities: [3, 14],
    seed: 20261121,
    replicates: 20000,
  },
  accountAtSplit:
    'Begin each independent evaluation flat with 10000 USDC. Carry state across every UTC day within it. Purge orders with lifetime or markout crossing a split. Liquidate once at period end.',
  promote: 'positive total and positive primary lower bound, complete usable coverage and no halt',
};
const date = (t) => new Date(t).toISOString().slice(0, 10);
const sha = (bytes) => crypto.createHash('sha256').update(bytes).digest('hex');
const fileHash = (file) => sha(fs.readFileSync(file));
const parse = (file) => JSON.parse(fs.readFileSync(file));
const shaJSON = (value) => sha(JSON.stringify(value));
function codeHashes() {
  return Object.fromEntries(
    [
      'btc-quote-model.cjs',
      'btc-account-replay.cjs',
      'first-week.cjs',
      'annual.cjs',
      'inference.py',
      'replay-coverage.cjs',
    ].map((name) => [name, fileHash(path.join(__dirname, name))]),
  );
}
function writeNew(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temp = file + '.' + crypto.randomUUID() + '.tmp';
  try {
    fs.writeFileSync(temp, JSON.stringify(obj) + '\n', { flag: 'wx' });
    fs.linkSync(temp, file);
  } finally {
    fs.rmSync(temp, { force: true });
  }
}
function split(t) {
  if (t < trainStart || t >= finalEnd) throw Error('Timestamp outside the study');
  if (t < trainEnd) return 'training';
  if (t < finalStart) return 'validation';
  return 'sealed-final';
}
function folderFor(root, t) {
  return t < oldPilotEnd
    ? path.join(root, 'first-week')
    : path.join(root, 'annual', split(t), date(t).replaceAll('-', ''));
}
function eachHour(start, end, fn) {
  for (let t = start; t < end; t += M.hour) fn(t);
}
function manifestDigest(root, start, end, verifyPackets = false) {
  // Hash only source manifests here. The frozen-final seal never interprets market outcomes.
  const digest = crypto.createHash('sha256');
  eachHour(start, end, (t) => {
    const base = path.join(folderFor(root, t), String(t));
    const raw = fs.readFileSync(base + '.manifest.json');
    digest.update(sha(raw));
    if (verifyPackets) {
      const m = JSON.parse(raw);
      assert.equal(m.hour, t);
      assert.equal(fileHash(base + '.json.gz'), m.packet_sha256, 'Changed packet ' + t);
    }
  });
  return digest.digest('hex');
}
function combine(model, part) {
  model.excludedDecisions += part.excludedDecisions;
  for (const [key, g] of Object.entries(part.groups)) {
    const a = (model.groups[key] ||= { n: 0, fills: 0, sum: 0 });
    a.n += g.n;
    a.fills += g.fills;
    a.sum += g.sum;
  }
}
function fitDevelopment(root, output, stopAt = trainEnd, splitEnd = trainEnd) {
  if (
    ![stopAt, splitEnd].every((t) => Number.isSafeInteger(t) && t % M.day === 0) ||
    !(trainStart < stopAt && stopAt <= splitEnd && splitEnd <= trainEnd)
  )
    throw Error('Use a complete training-day interval');
  // The code hash isolates checkpoints after a model or packet-reader change.
  const version = shaJSON({
    model: fileHash(require.resolve('./btc-quote-model.cjs')),
    reader: fileHash(require.resolve('./first-week.cjs')),
    runner: [fitDevelopment, combine, folderFor, manifestDigest, split].map((fn) =>
      sha(fn.toString()),
    ),
    config: M.defaults,
    splitEnd,
  });
  const directory = path.join(output, 'checkpoints', version);
  const model = {
    version: 1,
    start: trainStart,
    end: Math.min(stopAt + M.defaults.delay + M.defaults.life + M.defaults.horizon, splitEnd),
    decisionEnd: stopAt,
    config: M.defaults,
    groups: {},
    excludedDecisions: 0,
    complete: stopAt === trainEnd,
  };
  const fingerprints = [];
  for (let day = trainStart; day < stopAt; day += M.day) {
    const end = day + M.day;
    // Packet partition vs exchange-time spillover requires two neighboring packets:
    // one for features or label horizon, another for events spilling past the file boundary.
    const from = Math.max(trainStart, day - 2 * M.hour);
    const to = Math.min(splitEnd, end + 2 * M.hour);
    const digest = manifestDigest(root, from, to);
    const file = path.join(directory, date(day) + '.json');
    let saved;
    if (fs.existsSync(file)) {
      saved = parse(file);
      assert.equal(saved.manifestDigest, digest, 'Changed checkpoint source for ' + date(day));
      assert.equal(saved.version, version);
      assert.equal(saved.start, day);
      assert.equal(saved.end, end);
      assert.equal(saved.entryHash, shaJSON(saved.part), 'Changed checkpoint payload');
    } else {
      const source = readPackets((t) => folderFor(root, t), to, from, to);
      assert.equal(source.trainingManifestSha256, digest);
      const part = train(source.interval, day, end, trainStart, splitEnd);
      saved = {
        start: day,
        end,
        version,
        manifestDigest: digest,
        part,
        entryHash: shaJSON(part),
      };
      writeNew(file, saved);
      console.log('Fitted UTC training day', date(day));
    }
    fingerprints.push([day, digest, saved.entryHash]);
    combine(model, saved.part);
  }
  return {
    ...model,
    lastTrainingSource: Math.min(splitEnd, stopAt + 2 * M.hour),
    sourceSha256: shaJSON(fingerprints),
    fitVersion: version,
    modelCodeSha256: fileHash(require.resolve('./btc-quote-model.cjs')),
  };
}

function replayWindows(
  interval,
  funding,
  start,
  end,
  model,
  cases = policies,
  chunk = M.hour,
  frozen = false,
) {
  const runs = Object.entries(cases).map(([name, [policy, opts]]) => {
    const iter = accountWindows(start, end, policy, model, opts, chunk, frozen);
    return { name, iter, next: iter.next() };
  });
  for (;;) {
    const active = runs.filter((r) => !r.next.done);
    if (!active.length) break;
    const want = active[0].next.value;
    for (const runner of active) assert.deepEqual(runner.next.value, want);
    const tape = interval(Math.max(start, want.readStart), Math.min(end, want.readEnd));
    const i = M.indexAfter(funding, want.start - 1),
      j = M.indexAfter(funding, want.end - 1);
    tape.funding = funding.slice(i, j);
    for (const runner of active) {
      try {
        runner.next = runner.iter.next(tape);
      } catch (error) {
        const guarded = cases[runner.name][1].coverageMode === 'flatAcrossGaps';
        if (
          error.code !== 'UNOBSERVABLE_EXIT' &&
          !(guarded && error.code === 'UNOBSERVABLE_ACCOUNT')
        )
          throw error;
        // An ambiguous account has no valid full-period P&L. Other accounts continue.
        runner.next = {
          done: true,
          value: {
            status: 'unobservable',
            cause: error.code,
            reason: error.message,
            ...error.detail,
          },
        };
      }
    }
  }
  return Object.fromEntries(runs.map((r) => [r.name, r.next.value]));
}

function confirmedFunding(file, start, end) {
  const rows = parse(file);
  if (rows.length !== (end - start) / M.hour) throw Error('Incomplete verified funding interval');
  rows.forEach((row, i) => {
    if (
      Math.floor(row.time / M.hour) !== start / M.hour + i ||
      !row.sourceKey ||
      !/^[a-f0-9]{64}$/.test(row.sourceSha256) ||
      !['oraclePx', 'oraclePrice'].includes(row.sourceField) ||
      row.priceEvidence !== 'verified settlement oracle' ||
      !['hl-mainnet-node-data', 'hyperliquid-archive'].includes(row.sourceBucket) ||
      row.rateSource !== 'https://api.hyperliquid.xyz/info' ||
      !/^[a-f0-9]{64}$/.test(row.rateSourceSha256)
    )
      throw Error('Unverified funding row ' + i);
    M.timestamp(row.time);
    M.numeric(row.rate);
    M.positive(row.oraclePx);
    M.positive(row.markPx);
  });
  return rows;
}
function checkModel(model) {
  if (
    !model.complete ||
    model.start !== trainStart ||
    model.decisionEnd !== trainEnd ||
    model.end !== trainEnd ||
    model.modelCodeSha256 !== fileHash(require.resolve('./btc-quote-model.cjs'))
  )
    throw Error('A complete training-only fit from this code is required');
  assert.deepEqual(model.config, M.defaults);
}

// Rates are cross-checked against the original official API pages used to derive each UTC day.
function proxyFunding(root, start, end) {
  const rows = [];
  for (let day = start; day < end; day += M.day) {
    const dir = path.join(root, 'annual', split(day), date(day).replaceAll('-', ''));
    const ratesFile = path.join(dir, 'fundingHistory.raw.json'),
      pricesFile = path.join(dir, 'context-prices.json');
    const meta = parse(path.join(dir, 'funding.manifest.json'));
    assert.equal(meta.source, 'https://api.hyperliquid.xyz/info');
    assert.equal(meta.dailyRowsSha256, fileHash(ratesFile));
    const rates = parse(ratesFile),
      prices = parse(pricesFile);
    const pageFile = path.join(root, 'funding', split(day), meta.rawPage);
    assert.equal(fileHash(pageFile), meta.rawPageSha256);
    assert.deepEqual(
      parse(pageFile).filter((r) => r.time >= day && r.time < day + M.day),
      rates,
    );
    assert.equal(rates.length, 24);
    if (prices.length !== 24) throw Error('Incomplete hourly oracle proxy on ' + date(day));
    prices.forEach((p, i) => {
      const r = rates[i];
      assert.equal(r.coin, 'BTC');
      assert.equal(Math.floor(r.time / M.hour), day / M.hour + i);
      assert.equal(p.time, r.time);
      assert.equal(p.sampleTime, day + i * M.hour);
      assert.equal(p.sourceBucket, 'hyperliquid-archive');
      assert.equal(p.sourceField, 'oracle_px');
      assert.equal(p.sourceKey, 'asset_ctxs/' + date(day).replaceAll('-', '') + '.csv.lz4');
      assert.match(p.sourceSha256, /^[a-f0-9]{64}$/);
      assert.equal(p.priceEvidence, 'minute-sampled context proxy');
      rows.push({
        time: r.time,
        rate: M.numeric(r.fundingRate),
        oraclePx: M.positive(p.oraclePx),
        markPx: M.positive(p.markPx),
        priceSourceSha256: p.sourceSha256,
        rateSourceSha256: meta.rawPageSha256,
      });
    });
  }
  return rows;
}
function diagnosticValidation(root, modelFile) {
  const model = parse(modelFile);
  checkModel(model);
  const start = trainEnd,
    end = finalStart,
    funding = proxyFunding(root, start, end);
  const source = readPackets((t) => folderFor(root, t), end, start, end);
  const guarded = Object.fromEntries(
    Object.entries(policies).map(([name, [policy, opts]]) => [
      name,
      [policy, { ...opts, coverageMode: 'flatAcrossGaps' }],
    ]),
  );
  return {
    start,
    end,
    sourceSha256: source.trainingManifestSha256,
    fundingSha256: shaJSON(funding),
    modelSha256: shaJSON(model),
    code: codeHashes(),
    fundingPrices: 'sampled context proxy',
    primary: false,
    results: replayWindows(source.interval, funding, start, end, model, guarded),
  };
}
function evaluate(root, model, fundingFile, start, end, frozen = false) {
  checkModel(model);
  const funding = confirmedFunding(fundingFile, start, end);
  const source = readPackets((t) => folderFor(root, t), end, start, end);
  const results = replayWindows(
    source.interval,
    funding,
    start,
    end,
    model,
    policies,
    M.hour,
    frozen,
  );
  return {
    start,
    end,
    sourceSha256: source.trainingManifestSha256,
    fundingSha256: fileHash(fundingFile),
    modelSha256: shaJSON(model),
    code: codeHashes(),
    results,
  };
}
function evaluateValidation(root, modelFile, fundingFile) {
  return evaluate(root, parse(modelFile), fundingFile, trainEnd, finalStart);
}

function lockDevelopment(root, output, modelFile, validationFile) {
  const model = parse(modelFile),
    validation = parse(validationFile);
  assert.ok(model.complete && model.decisionEnd === trainEnd && model.end === trainEnd);
  assert.equal(model.modelCodeSha256, fileHash(require.resolve('./btc-quote-model.cjs')));
  assert.deepEqual(model.config, M.defaults);
  assert.equal(validation.start, trainEnd);
  assert.equal(validation.end, finalStart);
  assert.equal(validation.modelSha256, shaJSON(model));
  assert.deepEqual(validation.code, codeHashes());
  assert.equal(validation.sourceSha256, manifestDigest(root, trainEnd, finalStart, true));
  const rebuilt = fitDevelopment(root, output);
  assert.deepEqual(rebuilt, model);
  return {
    version: 1,
    specification,
    code: codeHashes(),
    modelSha256: fileHash(modelFile),
    validationSha256: fileHash(validationFile),
    trainingManifestSha256: manifestDigest(root, trainStart, trainEnd, true),
    validationManifestSha256: validation.sourceSha256,
  };
}
function verifiedFees(evidence) {
  const periods = (evidence.periods || [evidence]).slice().sort((a, b) => a.from - b.from);
  let through = trainStart;
  for (const p of periods) {
    const url = new URL(p.officialUrl);
    const official = ['hyperliquid.xyz', 'hyperliquid.gitbook.io', 'hyperfoundation.org'].some(
      (host) =>
        url.protocol === 'https:' && (url.hostname === host || url.hostname.endsWith('.' + host)),
    );
    assert.ok(
      official && /^[a-f0-9]{64}$/.test(p.sourceSha256),
      'Official fee provenance required',
    );
    assert.ok(
      p.verifiedStudyPeriod === true &&
        Number.isSafeInteger(p.from) &&
        Number.isSafeInteger(p.to) &&
        p.from < p.to,
      'Unverified fee effective interval',
    );
    if (p.to <= trainStart || p.from >= finalEnd) continue;
    assert.ok(
      p.maker === M.defaults.maker && p.taker === M.defaults.taker,
      'Effective rates differ from the locked schedule',
    );
    assert.ok(p.from <= through, 'Gap in effective fee evidence');
    through = Math.max(through, p.to);
  }
  assert.ok(through >= finalEnd, 'Incomplete effective fee evidence');
}
function verifyLock(seal, lockFile) {
  assert.equal(seal.developmentLockSha256, fileHash(lockFile), 'Changed development lock');
  const lock = parse(lockFile);
  for (const key of ['version', 'specification', 'code', 'modelSha256', 'validationSha256'])
    assert.deepEqual(seal[key], lock[key], 'Changed locked field ' + key);
}
function freeze(root, output, modelFile, validationFile, finalFundingFile, feeEvidenceFile) {
  if (Date.now() < finalEnd) throw Error('Final coverage is still in the future');
  const lock = lockDevelopment(root, output, modelFile, validationFile);
  const lockFile = path.join(output, 'development-lock.json');
  if (fs.existsSync(lockFile)) assert.deepEqual(parse(lockFile), lock);
  else writeNew(lockFile, lock);
  const fees = parse(feeEvidenceFile);
  // Historical schedule evidence is an input to the freeze. A live fee page is insufficient.
  verifiedFees(fees);
  // No rate, price, return or fill statistic is emitted or used to choose the frozen policy.
  const sealed = manifestDigest(root, finalStart, finalEnd, true);
  return {
    version: 1,
    specification,
    code: codeHashes(),
    modelSha256: fileHash(modelFile),
    validationSha256: fileHash(validationFile),
    developmentLockSha256: fileHash(lockFile),
    finalManifestSha256: sealed,
    fundingSha256: fileHash(finalFundingFile),
    feesSha256: fileHash(feeEvidenceFile),
  };
}
function score(root, sealFile, modelFile, finalFundingFile, outputFile) {
  if (Date.now() < finalEnd) throw Error('Final test is still sealed');
  const seal = parse(sealFile);
  verifyLock(seal, path.join(path.dirname(sealFile), 'development-lock.json'));
  assert.equal(seal.version, 1);
  assert.deepEqual(seal.specification, specification);
  assert.deepEqual(seal.code, codeHashes());
  assert.equal(seal.modelSha256, fileHash(modelFile));
  assert.equal(seal.fundingSha256, fileHash(finalFundingFile));
  assert.equal(seal.finalManifestSha256, manifestDigest(root, finalStart, finalEnd, true));
  const existing = fs.existsSync(outputFile);
  if (existing) {
    assert.equal(
      parse(outputFile).freezeSha256,
      fileHash(sealFile),
      'Different existing evaluation',
    );
    return;
  }
  let result;
  try {
    result = evaluate(root, parse(modelFile), finalFundingFile, finalStart, finalEnd, true);
  } catch (error) {
    if (error.code !== 'INCOMPLETE_MARKET_COVERAGE') throw error;
    writeNew(outputFile, {
      status: 'inconclusive',
      primary: null,
      promotionCandidate: false,
      reason: error.message,
      freezeSha256: fileHash(sealFile),
    });
    return;
  }
  const r = result.results.model;
  if (r.status === 'unobservable') {
    writeNew(outputFile, {
      ...result,
      status: 'inconclusive',
      primary: null,
      promotionCandidate: false,
      freezeSha256: fileHash(sealFile),
    });
    return;
  }
  assert.equal(r.daily.length, 65);
  assert.equal(r.daily[0].day, date(finalStart));
  const proc = spawnSync(
    'python3',
    [
      '-c',
      'import sys,json;from inference import summarize;print(json.dumps(summarize(json.load(sys.stdin))))',
    ],
    { cwd: __dirname, input: JSON.stringify(r.daily), encoding: 'utf8' },
  );
  if (proc.status !== 0) throw Error(proc.stderr);
  const inference = JSON.parse(proc.stdout);
  writeNew(outputFile, {
    ...result,
    freezeSha256: fileHash(sealFile),
    inference,
    promotionCandidate:
      r.net > 0 &&
      inference.primary_lower_usd_per_day > 0 &&
      r.haltTime === null &&
      r.fundingCoverage.complete,
  });
}
function readyThrough(root) {
  let t = trainStart;
  for (; t < trainEnd; t += M.hour) {
    const p = path.join(folderFor(root, t), String(t));
    if (!fs.existsSync(p + '.manifest.json') || !fs.existsSync(p + '.json.gz')) break;
  }
  if (t === trainEnd) return t;
  console.log('Next incomplete UTC source hour', new Date(t).toISOString());
  return Math.max(trainStart, Math.floor((t - 2 * M.hour) / M.day) * M.day);
}
module.exports = {
  fitDevelopment,
  replayWindows,
  policies,
  folderFor,
  confirmedFunding,
  freeze,
  score,
  evaluateValidation,
  lockDevelopment,
  readyThrough,
  diagnosticValidation,
  verifiedFees,
  verifyLock,
};
if (require.main === module) {
  const [cmd, root, output, ...args] = process.argv.slice(2);
  if ((cmd === 'fit' && args.length <= 1) || (cmd === 'fit-ready' && !args.length)) {
    const stopAt =
      cmd === 'fit-ready' ? readyThrough(root) : args.length ? Date.parse(args[0]) : trainEnd;
    if (stopAt === trainStart) {
      console.log('No complete training day with label context yet');
      return;
    }
    const model = fitDevelopment(root, output, stopAt);
    const file = path.join(
      output,
      model.complete ? 'model.json' : 'model-through-' + date(stopAt - M.day) + '.json',
    );
    if (fs.existsSync(file)) assert.deepEqual(parse(file), model);
    else writeNew(file, model);
    console.log(file);
  } else if (cmd === 'validate' && args.length === 2)
    writeNew(output, evaluateValidation(root, ...args));
  else if (cmd === 'diagnose-validation' && args.length === 1)
    writeNew(output, diagnosticValidation(root, ...args));
  else if (cmd === 'lock' && args.length === 2)
    writeNew(path.join(output, 'development-lock.json'), lockDevelopment(root, output, ...args));
  else if (cmd === 'freeze' && args.length === 4)
    writeNew(path.join(output, 'freeze.json'), freeze(root, output, ...args));
  else if (cmd === 'score' && args.length === 3) score(root, ...args, output);
  else
    throw Error(
      'Use fit-ready DATA OUT_DIR, fit DATA OUT_DIR [EXCLUSIVE_DAY], validate DATA OUT MODEL FUNDING, diagnose-validation DATA OUT MODEL, lock DATA OUT_DIR MODEL VALIDATION, freeze DATA OUT_DIR MODEL VALIDATION FINAL_FUNDING FEE_EVIDENCE, or score DATA OUT SEAL MODEL FINAL_FUNDING',
    );
}
