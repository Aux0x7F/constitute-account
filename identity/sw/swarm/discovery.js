// FILE: identity/sw/swarm/discovery.js
import {
  finalizeEvent,
  verifyEvent,
} from 'https://cdn.jsdelivr.net/npm/nostr-tools@2.7.2/+esm';

import { kvGet, kvSet } from '../idb.js';
import { ensureDevice } from '../deviceStore.js';
import { getIdentity } from '../identityStore.js';
import { hexToBytes } from '../nostr.js';

const RECORD_KIND = 30078;
const RECORD_TAG = 'swarm_discovery';
const MAX_SKEW_SEC = 10 * 60;
const ID_INDEX_KEY = 'swarm:index:identity';
const DEV_INDEX_KEY = 'swarm:index:device';
const DHT_INDEX_KEY = 'swarm:index:dht';
const BROWSER_ROLE = 'browser';
const SERVICE_VERSION = 'web-dev';

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function recordKey(type, id) {
  return `swarm:${type}:${String(id || '').trim()}`;
}

function dhtIndexId(scope, key) {
  return `${encodeURIComponent(String(scope || '').trim())}|${encodeURIComponent(String(key || '').trim())}`;
}

function parseDhtIndexId(id) {
  const raw = String(id || '');
  const [scopeEnc, keyEnc] = raw.split('|');
  if (!scopeEnc || !keyEnc) return null;
  return {
    scope: decodeURIComponent(scopeEnc),
    key: decodeURIComponent(keyEnc),
  };
}

function asTags(type, role = '') {
  const tags = [
    ['t', RECORD_TAG],
    ['type', type],
  ];
  const normalizedRole = String(role || '').trim();
  if (normalizedRole) tags.push(['role', normalizedRole]);
  return tags;
}

function parseContent(ev) {
  try { return JSON.parse(ev?.content || ''); } catch { return null; }
}

function clockOk(createdAt) {
  const now = nowSec();
  if (!createdAt) return false;
  if (createdAt > now + MAX_SKEW_SEC) return false;
  // Allow older records; payload may set its own expiresAt.
  return true;
}

export async function makeIdentityRecord() {
  const ident = await getIdentity();
  if (!ident?.linked || !ident?.id) throw new Error('no linked identity');
  const dev = await ensureDevice();

  const payload = {
    identityId: ident.id,
    label: ident.label || '',
    devicePks: (ident.devices || []).map(d => d.pk).filter(Boolean),
    updatedAt: Date.now(),
    expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    serviceVersion: SERVICE_VERSION,
  };

  const unsigned = {
    kind: RECORD_KIND,
    created_at: nowSec(),
    tags: asTags('identity'),
    content: JSON.stringify(payload),
    pubkey: dev.nostr.pk,
  };

  return finalizeEvent(unsigned, hexToBytes(dev.nostr.skHex));
}

export async function makeDeviceRecord() {
  const dev = await ensureDevice();
  const ident = await getIdentity();
  if (!dev?.nostr?.pk) throw new Error('no device key');

  const payload = {
    devicePk: dev.nostr.pk,
    identityId: ident?.id || '',
    deviceLabel: dev.label || '',
    updatedAt: Date.now(),
    expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    role: BROWSER_ROLE,
    relays: [],
    serviceVersion: SERVICE_VERSION,
  };

  const unsigned = {
    kind: RECORD_KIND,
    created_at: nowSec(),
    tags: asTags('device', BROWSER_ROLE),
    content: JSON.stringify(payload),
    pubkey: dev.nostr.pk,
  };

  return finalizeEvent(unsigned, hexToBytes(dev.nostr.skHex));
}

export async function makeDhtRecord(scope, key, value, opts = {}) {
  const dev = await ensureDevice();
  if (!dev?.nostr?.pk) throw new Error('no device key');
  const dhtScope = String(scope || '').trim();
  const dhtKey = String(key || '').trim();
  if (!dhtScope || !dhtKey) throw new Error('missing scope or key');

  const payload = {
    scope: dhtScope,
    key: dhtKey,
    value,
    authorPk: dev.nostr.pk,
    updatedAt: Number(opts?.updatedAt || Date.now()),
    expiresAt: Number(opts?.expiresAt || (Date.now() + 24 * 60 * 60 * 1000)),
  };

  const unsigned = {
    kind: RECORD_KIND,
    created_at: nowSec(),
    tags: asTags('dht'),
    content: JSON.stringify(payload),
    pubkey: dev.nostr.pk,
  };

  return finalizeEvent(unsigned, hexToBytes(dev.nostr.skHex));
}

export async function putIdentityRecord(ev) {
  const ok = await validateRecord(ev, 'identity');
  if (!ok) return { ok: false };
  const payload = parseContent(ev);
  await kvSet(recordKey('identity', payload.identityId), ev);
  await addToIndex(ID_INDEX_KEY, payload.identityId);
  return { ok: true };
}

export async function putDeviceRecord(ev) {
  const ok = await validateRecord(ev, 'device');
  if (!ok) return { ok: false };
  const payload = parseContent(ev);
  await kvSet(recordKey('device', payload.devicePk), ev);
  await addToIndex(DEV_INDEX_KEY, payload.devicePk);
  return { ok: true };
}

export async function putDhtRecord(ev) {
  const ok = await validateRecord(ev, 'dht');
  if (!ok) return { ok: false };
  const payload = parseContent(ev);
  const id = dhtIndexId(payload.scope, payload.key);
  await kvSet(recordKey('dht', id), ev);
  await addToIndex(DHT_INDEX_KEY, id);
  return { ok: true };
}

export async function getIdentityRecord(identityId) {
  return await kvGet(recordKey('identity', identityId));
}

export async function getDeviceRecord(devicePk) {
  return await kvGet(recordKey('device', devicePk));
}

export async function getDhtRecord(scope, key) {
  const id = dhtIndexId(scope, key);
  return await kvGet(recordKey('dht', id));
}

export async function listIdentityRecords() {
  const ids = (await kvGet(ID_INDEX_KEY)) || [];
  const out = [];
  for (const id of (Array.isArray(ids) ? ids : [])) {
    const ev = await getIdentityRecord(id);
    if (!ev) continue;
    const ok = await validateRecord(ev, 'identity');
    if (!ok) continue;
    out.push(JSON.parse(ev.content || '{}'));
  }
  return out;
}

export async function listDeviceRecords() {
  const ids = (await kvGet(DEV_INDEX_KEY)) || [];
  const out = [];
  for (const id of (Array.isArray(ids) ? ids : [])) {
    const ev = await getDeviceRecord(id);
    if (!ev) continue;
    const ok = await validateRecord(ev, 'device');
    if (!ok) continue;
    out.push(JSON.parse(ev.content || '{}'));
  }
  return out;
}

export async function listDhtRecords() {
  const ids = (await kvGet(DHT_INDEX_KEY)) || [];
  const out = [];
  for (const id of (Array.isArray(ids) ? ids : [])) {
    const parsed = parseDhtIndexId(id);
    if (!parsed) continue;
    const ev = await getDhtRecord(parsed.scope, parsed.key);
    if (!ev) continue;
    const ok = await validateRecord(ev, 'dht');
    if (!ok) continue;
    out.push(JSON.parse(ev.content || '{}'));
  }
  return out;
}

export async function validateRecord(ev, expectedType) {
  if (!ev || ev.kind !== RECORD_KIND) return false;
  if (!clockOk(ev.created_at)) return false;
  if (!verifyEvent(ev)) return false;
  const tags = Array.isArray(ev.tags) ? ev.tags : [];
  const hasTag = tags.some(t => Array.isArray(t) && t[0] === 't' && t[1] === RECORD_TAG);
  if (!hasTag) return false;
  const typeTag = tags.find(t => Array.isArray(t) && t[0] === 'type');
  if (!typeTag || typeTag[1] !== expectedType) return false;

  const payload = parseContent(ev);
  if (!payload) return false;
  if (payload.expiresAt && Date.now() > Number(payload.expiresAt)) return false;

  if (expectedType === 'identity') {
    if (!payload.identityId) return false;
    const devices = Array.isArray(payload.devicePks) ? payload.devicePks : [];
    if (!devices.includes(ev.pubkey)) return false;
  }

  if (expectedType === 'device') {
    if (!payload.devicePk) return false;
    if (payload.devicePk !== ev.pubkey) return false;
    const roleTag = tags.find(t => Array.isArray(t) && t[0] === 'role');
    const rolePayload = String(payload.role || '').trim();
    if (roleTag && rolePayload && roleTag[1] !== rolePayload) return false;
  }

  if (expectedType === 'dht') {
    if (!payload.scope || !payload.key) return false;
    if (!Object.prototype.hasOwnProperty.call(payload, 'value')) return false;
    const authorPk = String(payload.authorPk || '').trim();
    if (authorPk && authorPk !== ev.pubkey) return false;
  }

  return true;
}

async function addToIndex(key, id) {
  const k = String(id || '').trim();
  if (!k) return;
  const list = (await kvGet(key)) || [];
  const arr = Array.isArray(list) ? list : [];
  if (arr.includes(k)) return;
  arr.unshift(k);
  if (arr.length > 500) arr.length = 500;
  await kvSet(key, arr);
}
