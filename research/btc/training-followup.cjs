'use strict';

// Prespecified training-only follow-up. No candidate selection, parameter search or refit.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const M = require('./btc-quote-model.cjs');
const { readPackets } = require('./first-week.cjs');
const { replayWindows, policies, folderFor, proxyFunding } = require('./annual.cjs');
const { certifyFlat } = require('./coverage-audit.cjs');
const { audit } = require('./prediction-audit.cjs');

const expected = '857582d8e2a4fca8b06e951c395dee99109419df11c35fabd7b60d605797c268';
const digest = (b) => crypto.createHash('sha256').update(b).digest('hex');
function followup(root, checkpoint, from = Date.parse('2026-01-06Z'), to = Date.parse('2026-01-10Z')) {
  assert.ok(from >= M.training[0] && from < to && to <= M.training[1], 'Training dates required');
  assert.ok(from % M.day === 0 && to % M.day === 0, 'Use complete UTC days');
  const source = path.join(root, 'first-week'),
    modelBytes = fs.readFileSync(checkpoint || path.join(source, 'model.json'));
  const model = JSON.parse(modelBytes);
  assert.deepEqual(model.config, M.defaults);
  assert.equal(model.start, M.training[0]);
  assert.ok(model.end <= from, 'Fit labels overlap the replay');
  if (checkpoint) {
    assert.ok(model.start < model.decisionEnd && model.decisionEnd <= model.end);
    assert.ok(model.end <= model.lastTrainingSource && model.lastTrainingSource <= from,
      'Training source packets overlap the replay');
    assert.match(model.sourceSha256, /^[a-f0-9]{64}$/);
    assert.equal(model.modelCodeSha256, digest(fs.readFileSync(require.resolve('./btc-quote-model.cjs'))));
  } else {
    assert.equal(digest(modelBytes), expected, 'Expected original, unchanged five-day fit');
    assert.equal(model.end, model.start + 5 * M.day);
    assert.equal(readPackets(source, model.end).trainingManifestSha256, model.trainingManifestSha256);
  }
  const certificate = certifyFlat(model);
  const funding = proxyFunding(root, from, to);
  const sourceHours = readPackets((t) => folderFor(root, t), to, from, to);
  const guarded = Object.fromEntries(
    Object.entries(policies).map(([name, [policy, opts]]) => [
      name,
      [policy, { ...opts, coverageMode: 'flatAcrossGaps' }],
    ]),
  );
  const results = replayWindows(sourceHours.interval, funding, from, to, model, guarded);
  const predictions = audit(model, sourceHours.interval, from, to);
  const files = [
    'training-followup.cjs',
    'btc-account-replay.cjs',
    'btc-quote-model.cjs',
    'first-week.cjs',
    'annual.cjs',
    'replay-coverage.cjs',
    'prediction-audit.cjs',
    'coverage-audit.cjs',
  ];
  return {
    start: from,
    end: to,
    capital: M.defaults.capital,
    phase: 'chronological training-only development',
    modelSha256: digest(modelBytes),
    ...(checkpoint
      ? { trainingCheckpointSourceSha256: model.sourceSha256, lastTrainingSource: model.lastTrainingSource }
      : { trainingManifestSha256: model.trainingManifestSha256 }),
    replayManifestSha256: sourceHours.trainingManifestSha256,
    fundingSha256: digest(JSON.stringify(funding)),
    fundingRows: funding.length,
    fundingPrices: 'minute-sampled official context oracle proxy',
    codeSha256: Object.fromEntries(
      files.map((f) => [f, digest(fs.readFileSync(path.join(__dirname, f)))]),
    ),
    cashEquivalentFromFlat: certificate,
    promotionEligible: false,
    policies,
    results,
    predictions,
  };
}
if (require.main === module) {
  const [root, out, ...rest] = process.argv.slice(2);
  if (!root || !out || ![0, 3].includes(rest.length))
    throw Error('Use DATA_ROOT RESULT_FILE [CHECKPOINT START_UTC END_UTC_EXCLUSIVE]');
  const [checkpoint, start, end] = rest;
  const result = rest.length
    ? followup(root, checkpoint, Date.parse(start), Date.parse(end))
    : followup(root);
  fs.writeFileSync(out, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  console.log(out);
}
