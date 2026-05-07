import test from 'node:test';
import assert from 'node:assert/strict';

import { normalizePairCodeInput } from './pairing-ui.js';
import {
  filterPendingPairRequestsForProjection,
  isPairRequestExpired,
  pairRequestExpiresAt,
} from '../identity/sw/pending.js';

test('normalizePairCodeInput accepts pasted operator text', () => {
  assert.equal(normalizePairCodeInput('209845'), '209845');
  assert.equal(normalizePairCodeInput('Pairing code: 209845 expires soon'), '209845');
  assert.equal(normalizePairCodeInput('209 845'), '209845');
  assert.equal(normalizePairCodeInput('209-845'), '209845');
});

test('isPairRequestExpired honors request ttl units', () => {
  assert.equal(pairRequestExpiresAt({ ts: 1_000, ttl: 120 }), 121_000);
  assert.equal(pairRequestExpiresAt({ ts: 1_000, ttlMs: 500 }), 1_500);
  assert.equal(isPairRequestExpired({ ts: 1_000, ttl: 120 }, 120_999), false);
  assert.equal(isPairRequestExpired({ ts: 1_000, ttl: 120 }, 121_001), true);
  assert.equal(isPairRequestExpired({ ts: 1_000, ttlMs: 500 }, 1_501), true);
});

test('filterPendingPairRequests removes resolved, known, and expired requests', () => {
  const now = 300_000;
  const pending = filterPendingPairRequestsForProjection([
    { id: 'ok', status: 'pending', devicePk: 'new', ts: now - 1_000, ttl: 120 },
    { id: 'done', approved: true },
    { id: 'known', status: 'pending', devicePk: 'known' },
    { id: 'expired', status: 'pending', ts: now - 200_000, ttl: 120 },
  ], [{ pk: 'known' }], now);
  assert.deepEqual(pending.map((request) => request.id), ['ok']);
});
