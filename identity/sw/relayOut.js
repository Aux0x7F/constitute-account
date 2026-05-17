// FILE: identity/sw/relayOut.js

import { signEventUnsigned } from './nostr.js';
import { ensureDevice } from './deviceStore.js';

const APP_TAG = 'constitute';
const DISCOVERY_TAG = 'swarm_discovery';
const SUB_ID = 'constitute_sub_v2';
const RELAY_SUBSCRIPTION_WINDOW_SEC = 10 * 60;
const RELAY_SCOPED_FILTER_LIMIT = 120;
const RELAY_DISCOVERY_FILTER_LIMIT = 100;

function nowSec() { return Math.floor(Date.now() / 1000); }

export function getSubId() { return SUB_ID; }
export function getAppTag() { return APP_TAG; }

function normalizeTags(tags) {
  const out = [];
  const seen = new Set();
  for (const tag of Array.isArray(tags) ? tags : []) {
    if (!Array.isArray(tag)) continue;
    const name = String(tag[0] || '').trim();
    const value = String(tag[1] || '').trim();
    if (!name || !value) continue;
    const key = `${name}\u0000${value}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push([name, value]);
  }
  return out;
}

function inferredAppTags(payloadObj = {}) {
  const payload = payloadObj && typeof payloadObj === 'object' ? payloadObj : {};
  const tags = [];
  const identity = String(payload.identity || payload.identityLabel || payload.pairIdentity || '').trim();
  if (identity) tags.push(['i', identity]);
  const zoneValues = [
    payload.zone,
    ...(Array.isArray(payload.zoneKeys) ? payload.zoneKeys : []),
  ].map((value) => String(value || '').trim()).filter(Boolean);
  for (const zone of zoneValues) tags.push(['z', zone]);
  const peer = String(payload.toPk || payload.toDevicePk || payload.targetPk || '').trim();
  if (peer) tags.push(['p', peer]);
  return normalizeTags(tags);
}

function subscriptionFilters(ident, dev, zones = []) {
  const since = nowSec() - RELAY_SUBSCRIPTION_WINDOW_SEC;
  const filters = [];
  const devicePk = String(dev?.nostr?.pk || '').trim();
  const identityLabel = String(ident?.label || '').trim();
  if (devicePk) {
    filters.push({ kinds: [1], '#t': [APP_TAG], '#p': [devicePk], since, limit: RELAY_SCOPED_FILTER_LIMIT });
  }
  if (identityLabel) {
    filters.push({ kinds: [1], '#t': [APP_TAG], '#i': [identityLabel], since, limit: RELAY_SCOPED_FILTER_LIMIT });
  }
  for (const zone of Array.isArray(zones) ? zones : []) {
    const key = String(zone?.key || zone || '').trim();
    if (key) filters.push({ kinds: [1], '#t': [APP_TAG], '#z': [key], since, limit: RELAY_SCOPED_FILTER_LIMIT });
  }
  filters.push({ kinds: [30078], '#t': [DISCOVERY_TAG], since, limit: RELAY_DISCOVERY_FILTER_LIMIT });
  return filters;
}

export async function relaySend(sw, frameArr) {
  const frame = JSON.stringify(frameArr);
  const clients = await sw.clients.matchAll({ type: 'window', includeUncontrolled: true });
  for (const c of clients) c.postMessage({ type: 'relay.tx', data: frame });
}

export async function subscribeOnRelayOpen(sw, ident, logFn, zones = []) {
  const dev = await ensureDevice();
  const since = nowSec() - RELAY_SUBSCRIPTION_WINDOW_SEC;
  const filters = subscriptionFilters(ident, dev, zones);
  await relaySend(sw, ['REQ', SUB_ID, ...filters]);
  if (logFn) logFn(`sent REQ subscribe since=${since} filters=${filters.length}`);
}

export async function publishAppEvent(sw, payloadObj, extraTags = []) {
  const dev = await ensureDevice();

  const unsigned = {
    kind: 1,
    created_at: nowSec(),
    tags: normalizeTags([['t', APP_TAG], ...inferredAppTags(payloadObj), ...extraTags]),
    content: JSON.stringify(payloadObj),
    pubkey: dev.nostr.pk,
  };

  const ev = signEventUnsigned(unsigned, dev.nostr.skHex);
  await relaySend(sw, ['EVENT', ev]);
  return ev.id;
}
