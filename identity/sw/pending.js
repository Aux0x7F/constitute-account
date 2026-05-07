// FILE: identity/sw/pending.js

import { kvGet, kvSet } from './idb.js';
import { getIdentity } from './identityStore.js';
import { blockedIs } from './blocklist.js';

export function pairRequestExpiresAt(request) {
  const ts = Number(request?.ts || request?.createdAt || 0);
  if (!Number.isFinite(ts) || ts <= 0) return 0;
  const explicitTtlMs = Number(request?.ttlMs || 0);
  const ttlSeconds = Number(request?.ttl || 0);
  const ttlMs = Number.isFinite(explicitTtlMs) && explicitTtlMs > 0
    ? explicitTtlMs
    : (Number.isFinite(ttlSeconds) && ttlSeconds > 0 ? ttlSeconds * 1000 : 0);
  return ttlMs > 0 ? ts + ttlMs : 0;
}

export function isPairRequestExpired(request, nowMs = Date.now()) {
  const expiresAt = pairRequestExpiresAt(request);
  if (!expiresAt) return false;
  return nowMs > expiresAt;
}

export function filterPendingPairRequestsForProjection(reqs, identityDevices, nowMs = Date.now()) {
  const knownPks = new Set((identityDevices || []).map((d) => d.pk).filter(Boolean));
  const knownDids = new Set((identityDevices || []).map((d) => d.did).filter(Boolean));

  return (Array.isArray(reqs) ? reqs : []).flatMap((request) => {
    const expiresAt = pairRequestExpiresAt(request);
    if (request.status && request.status !== 'pending') return [];
    if (request.state && request.state !== 'pending') return [];
    if (request.resolved === true) return [];
    if (request.approved === true || request.rejected === true) return [];
    if (request.devicePk && knownPks.has(request.devicePk)) return [];
    if (request.deviceDid && knownDids.has(request.deviceDid)) return [];
    if (isPairRequestExpired(request, nowMs)) return [];
    return [{ ...request, expiresAt: expiresAt || undefined }];
  });
}

export async function pendingAdd(req) {
  const list = (await kvGet('pairPending')) || [];
  if (!list.some(x => x.id === req.id)) list.unshift(req);
  await kvSet('pairPending', list);
}

export async function pendingList() {
  const list = (await kvGet('pairPending')) || [];
  const ident = await getIdentity();

  // Filter out:
  // - requests for already-known devices (stale)
  // - requests from blocked/rejected/revoked devices
  // - expired requests according to their projected TTL
  const out = [];
  for (const r of (Array.isArray(list) ? list : [])) {
    if (await blockedIs({ pk: r?.devicePk, did: r?.deviceDid })) continue;
    out.push(r);
  }
  const projected = filterPendingPairRequestsForProjection(out, ident?.devices || []);
  if (projected.length !== (Array.isArray(list) ? list : []).length) {
    await kvSet('pairPending', projected);
  }
  return projected;
}

export async function pendingRemove(id) {
  const list = (await kvGet('pairPending')) || [];
  await kvSet('pairPending', (Array.isArray(list) ? list : []).filter(x => x.id !== id));
}
