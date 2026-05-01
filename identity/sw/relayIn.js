import { verifyEvent } from 'https://cdn.jsdelivr.net/npm/nostr-tools@2.7.2/+esm';
// FILE: identity/sw/relayIn.js

import { nip04Decrypt, nip04Encrypt } from './nostr.js';
import { randomBytes, b64url } from './crypto.js';
import { ensureDevice, getDevice } from './deviceStore.js';
import { getIdentity, setIdentity } from './identityStore.js';
import { notifAdd, notifClear, notifRemove } from './notifs.js';
import { pendingAdd, pendingRemove } from './pending.js';
import { getSubId, getAppTag } from './relayOut.js';
import { emit, log, pokeUi } from './uiBus.js';
import { blockedAdd, blockedIs, blockedRemove } from './blocklist.js';
import { kvGet, kvSet } from './idb.js';
import { directoryUpsert } from './directory.js';
import { isZoneJoined, joinZone, addSelfToZoneList, publishZonePresence, publishZoneList, publishZoneListRequest, publishZoneMeta, publishZoneMetaRequest, publishZoneProbe, setZoneList, getZoneList, updateZoneName, listZones } from './zone.js';
import { putIdentityRecord, putDeviceRecord, putDhtRecord, getDhtRecord, validateRecord, makeIdentityRecord, makeDeviceRecord, makeDhtRecord } from './swarm/index.js';
import { publishAppEvent } from './relayOut.js';

const REPLAY_WINDOW_SEC = 10 * 60;
const REPLAY_SKEW_SEC = 2 * 60;
const REPLAY_CAP = 400;
const PAIR_OFFER_KEY = 'pairOffer';
const PAIR_CLAIM_ACTIVE_KEY = 'pairClaimActive';


function pairingTags(identityLabel, zones = [], toPk = '') {
  const tags = [['i', String(identityLabel || '').trim()]];
  for (const z of (Array.isArray(zones) ? zones : [])) {
    const key = String(z?.key || '').trim();
    if (key) tags.push(['z', key]);
  }
  const peerPk = String(toPk || '').trim();
  if (peerPk) tags.push(['p', peerPk]);
  return tags;
}

async function autoApprovePairRequest(sw, ident, req) {
  if (!ident?.linked || !ident?.roomKeyB64) return false;
  if (!req?.devicePk) return false;

  ident.devices = Array.isArray(ident.devices) ? ident.devices : [];
  const exists = ident.devices.some(d => d.pk === req.devicePk);
  if (!exists) {
    ident.devices.push({
      pk: req.devicePk,
      did: req.deviceDid || '',
      label: req.deviceLabel || '',
    });
    await setIdentity(ident);
  }

  const dev = await ensureDevice();
  const payload = JSON.stringify({
    identityId: ident.id,
    roomKeyB64: ident.roomKeyB64,
    devices: ident.devices,
  });

  const encryptedRoomKey = await nip04Encrypt(dev.nostr.skHex, req.devicePk, payload);
  const zones = await listZones(ident || {}).catch(() => []);

  await publishAppEvent(sw, {
    type: 'pair_approve',
    identity: req.identityLabel,
    code: req.code,
    toPk: req.devicePk,
    fromPk: dev.nostr.pk,
    encryptedRoomKey,
  }, pairingTags(req.identityLabel, zones, req.devicePk));

  await publishAppEvent(sw, {
    type: 'pair_resolved',
    identity: req.identityLabel,
    requestId: req.id,
    code: req.code,
    devicePk: req.devicePk,
    status: 'approved',
  }, pairingTags(req.identityLabel, zones, req.devicePk));

  await notifAdd({
    id: `n-auto-approve-${req.id}`,
    kind: 'pairing',
    title: 'Gateway auto-approved',
    body: `${req.deviceLabel || 'Gateway'} added to ${req.identityLabel}`,
    ts: Date.now(),
    read: false,
  });

  return true;
}

function createdAtOk(createdAt) {
  const ts = Number(createdAt || 0);
  if (!ts) return false;
  const now = Math.floor(Date.now() / 1000);
  if (ts < now - REPLAY_WINDOW_SEC) return false;
  if (ts > now + REPLAY_SKEW_SEC) return false;
  return true;
}

function payloadTimeOk(payload) {
  if (!payload || typeof payload !== 'object') return false;
  const hasTs = Object.prototype.hasOwnProperty.call(payload, 'ts');
  if (!hasTs) return true;
  const ts = Number(payload.ts || 0);
  if (!Number.isFinite(ts) || ts <= 0) return false;
  const now = Date.now();
  if (ts > now + (REPLAY_SKEW_SEC * 1000)) return false;

  const hasTtl = Object.prototype.hasOwnProperty.call(payload, 'ttl');
  if (!hasTtl) return ts >= now - (REPLAY_WINDOW_SEC * 1000);

  const ttl = Number(payload.ttl || 0);
  if (!Number.isFinite(ttl) || ttl <= 0) return false;
  if (now > ts + (ttl * 1000)) return false;
  return true;
}

async function replayAccept(identityLabel, ev) {
  const id = String(ev?.id || '').trim();
  const ts = Number(ev?.created_at || 0);
  if (!identityLabel || !id || !ts) return false;

  const now = Math.floor(Date.now() / 1000);
  if (ts < now - REPLAY_WINDOW_SEC) return false;
  if (ts > now + REPLAY_SKEW_SEC) return false;

  const key = `replay:${identityLabel}`;
  const list = (await kvGet(key)) || [];
  const keep = [];
  let seen = false;

  for (const it of (Array.isArray(list) ? list : [])) {
    if (!it?.id || !it?.ts) continue;
    if (it.ts < now - REPLAY_WINDOW_SEC) continue;
    if (it.id === id) { seen = true; continue; }
    keep.push(it);
  }

  if (seen) return false;

  keep.unshift({ id, ts });
  if (keep.length > REPLAY_CAP) keep.length = REPLAY_CAP;
  await kvSet(key, keep);
  return true;
}

function zoneTagsFromEvent(ev) {
  const out = [];
  const tags = Array.isArray(ev?.tags) ? ev.tags : [];
  for (const t of tags) {
    if (!Array.isArray(t) || t[0] !== 'z') continue;
    const key = String(t[1] || '').trim();
    if (!key) continue;
    if (!out.some((x) => Array.isArray(x) && x[0] === 'z' && x[1] === key)) {
      out.push(['z', key]);
    }
  }
  return out;
}


export async function handleRelayFrame(sw, raw) {
  const s = String(raw || '');
  if (!s) return;

  let msg;
  try { msg = JSON.parse(s); } catch { return; }
  const [type] = msg;

  if (type === 'NOTICE') { log(sw, `NOTICE: ${String(msg[1] || '').slice(0, 160)}`); return; }
  if (type === 'EOSE') { return; }
  if (type !== 'EVENT') return;

  const subId = msg[1];
  const ev = msg[2];
  if (subId !== getSubId() || !ev) return;

  const tags = Array.isArray(ev.tags) ? ev.tags : [];
  const hasAppTag = tags.some(t => Array.isArray(t) && t[0] === 't' && t[1] === getAppTag());
  const hasDiscoveryTag = tags.some(t => Array.isArray(t) && t[0] === 't' && t[1] === 'swarm_discovery');
  if (!hasAppTag && !hasDiscoveryTag) return;
  if (!verifyEvent(ev)) return;
  if (!createdAtOk(ev.created_at)) return;

  // Drop frames from blocked senders (minimal implicit trust).
  // NOTE: This is separate from payload.devicePk checks below.
  const senderPk = String(ev.pubkey || '').trim();
  if (senderPk && await blockedIs({ pk: senderPk })) return;

  if (Number(ev.kind || 0) === 30078 && hasDiscoveryTag) {
    const typeTag = tags.find(t => Array.isArray(t) && t[0] === 'type');
    const recType = String(typeTag?.[1] || '').trim();
    if (recType === 'identity') {
      const ok = await validateRecord(ev, 'identity');
      if (!ok) return;
      await putIdentityRecord(ev).catch(() => {});
      log(sw, 'swarm identity record stored');
      pokeUi(sw);
      return;
    }
    if (recType === 'device') {
      const ok = await validateRecord(ev, 'device');
      if (!ok) return;
      const discoveryPayload = (() => {
        try { return JSON.parse(ev.content || '{}'); } catch { return {}; }
      })();
      const result = await putDeviceRecord(ev).catch(() => ({ ok: false }));
      if (!result?.ok) return;
      if (result?.stale) {
        log(sw, 'swarm device record stale (ignored)');
        return;
      }
      const hostedCount = Array.isArray(discoveryPayload?.hostedServices) ? discoveryPayload.hostedServices.length : 0;
      log(sw, hostedCount > 0 ? `swarm device record stored hosted=${hostedCount}` : 'swarm device record stored');
      pokeUi(sw);
      return;
    }
    if (recType === 'dht') {
      const ok = await validateRecord(ev, 'dht');
      if (!ok) return;
      await putDhtRecord(ev).catch(() => {});
      log(sw, 'swarm dht record stored');
      pokeUi(sw);
      return;
    }
    return;
  }

  if (!hasAppTag) return;

  let payload = null;
  try { payload = JSON.parse(ev.content || ''); } catch { return; }
  if (!payload?.type) return;
  if (!payloadTimeOk(payload)) return;

  const dev = await getDevice();
  const ident = await getIdentity();

  const identityTag = tags.find(t => Array.isArray(t) && t[0] === 'i');
  const scopedLabel = identityTag?.[1] || null;

  const replayIdentity = String(payload.identity || scopedLabel || '').trim();
  if (replayIdentity) {
    const ok = await replayAccept(replayIdentity, ev);
    if (!ok) return;
  }

  // optional identity scoping for identity-bound events
  if (ident?.label && scopedLabel && scopedLabel !== ident.label) {
    if (payload.type !== 'pair_request') return;
  }

  // --- Device blocked / unblocked (blacklist convergence) ---
  if (payload.type === 'device_blocked') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;
    const targetPk = String(payload.targetPk || '').trim();
    if (!targetPk) return;

    await blockedAdd({ pk: targetPk, reason: payload.reason || 'blocked' });

    // Remove any pending requests from that device
    await pendingRemove(`${payload.identity}:${payload.code || ''}:${targetPk}`).catch(() => {});

    pokeUi(sw);
    return;
  }

  if (payload.type === 'device_unblocked') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;
    const targetPk = String(payload.targetPk || '').trim();
    if (!targetPk) return;
    await blockedRemove({ pk: targetPk });
    pokeUi(sw);
    return;
  }

  // --- Zone presence (directory) ---
  if (payload.type === 'zone_presence') {
    if (!ident?.linked || !ident?.id) return;
    const z = String(payload.zone || '').trim();
    if (!z) return;
    const ok = await isZoneJoined(ident, z);
    if (!ok) return;
    if (payload.devicePk && payload.devicePk === dev?.nostr?.pk) return;
    await directoryUpsert({
      zone: z,
      lastSeen: Date.now(),
      devicePk: String(payload.devicePk || '').trim(),
      swarm: String(payload.swarm || ''),
      role: String(payload.role || ''),
      relays: Array.isArray(payload.relays) ? payload.relays : [],
      serviceVersion: String(payload.serviceVersion || ''),
      hostPlatform: String(payload.hostPlatform || ''),
      releaseChannel: String(payload.releaseChannel || ''),
      releaseTrack: String(payload.releaseTrack || ''),
      releaseBranch: String(payload.releaseBranch || ''),
    });
    pokeUi(sw);
    return;
  }

  // --- Zone list ---
  if (payload.type === 'zone_list') {
    if (!ident?.linked || !ident?.id) return;
    const z = String(payload.zone || '').trim();
    if (!z) return;
    const ok = await isZoneJoined(ident, z);
    if (!ok) return;
    const ts = Number(payload.ts || 0);
    const curr = await getZoneList(z);
    if (ts && curr?.ts && ts <= curr.ts) return;
      const members = Array.isArray(payload.members) ? payload.members : [];
      const selfPk = String(dev?.nostr?.pk || '').trim();
      if (selfPk && !members.some(m => m?.devicePk === selfPk)) {
        members.unshift({ devicePk: selfPk, swarm: '' });
      }
      const zname = String(payload.name || '').trim();
      const isPlaceholder = !zname || zname === 'Joined' || zname.startsWith('Zone ');
      log(sw, `[zone_list] payload list: ${JSON.stringify(payload)}`);
      log(sw, `[zone_list] interpreted name: ${zname || '(empty)'}`);
      if (!isPlaceholder) await updateZoneName(z, zname).catch(() => {});
    await setZoneList(z, members, ts || Date.now());
    for (const m of members) {
      if (!m?.devicePk) continue;
      if (m.devicePk === dev?.nostr?.pk) continue;
      await directoryUpsert({
        zone: z,
        lastSeen: Date.now(),
        devicePk: String(m.devicePk || '').trim(),
        swarm: String(m.swarm || ''),
      });
    }
    pokeUi(sw);
    return;
  }

  // --- Zone list request ---
  if (payload.type === 'zone_list_request') {
    if (!ident?.linked || !ident?.id) return;
    const z = String(payload.zone || '').trim();
    if (!z) return;
    const ok = await isZoneJoined(ident, z);
    if (!ok) return;
    const dev = await getDevice();
    await addSelfToZoneList(dev, z).catch(() => {});
    const curr = await getZoneList(z);
    await publishZonePresence(sw, ident, dev, z).catch(() => {});
    const zones = await listZones(ident || {});
    const hit = zones.find(x => x.key === z);
    const zname = hit?.name || '';
    const isPlaceholder = !zname || zname === 'Joined' || zname.startsWith('Zone ');
    const nameForList = isPlaceholder ? '' : zname;
    await publishZoneList(sw, ident, z, curr.members || [], curr.ts || Date.now(), nameForList).catch(() => {});
    if (nameForList) await publishZoneMeta(sw, ident, z, nameForList).catch(() => {});
    return;
  }

  // --- Zone meta ---
  if (payload.type === 'zone_meta') {
    const z = String(payload.zone || '').trim();
    const n = String(payload.name || '').trim();
    if (!z || !n) return;
    const ok = await isZoneJoined(ident, z);
    if (!ok) return;
    await updateZoneName(z, n).catch(() => {});
    pokeUi(sw);
    return;
  }

  if (payload.type === 'zone_meta_request') {
    const z = String(payload.zone || '').trim();
    if (!z) return;
    const ok = await isZoneJoined(ident, z);
    if (!ok) return;
    const zones = await listZones(ident || {});
    const hit = zones.find(x => x.key === z);
    if (hit?.name) await publishZoneMeta(sw, ident, z, hit.name).catch(() => {});
    return;
  }

  // --- Zone joined (identity-wide sync) ---
  if (payload.type === 'zone_joined') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;
    const z = String(payload.zone || '').trim();
    if (!z) return;
    const already = await isZoneJoined(ident, z);
    if (!already) {
      await joinZone(ident, z, String(payload.name || 'Zone')).catch(() => {});
    }
    const dev = await getDevice();
    await addSelfToZoneList(dev, z).catch(() => {});
    const list = await getZoneList(z);
    await publishZonePresence(sw, ident, dev, z).catch(() => {});
    await publishZoneList(sw, ident, z, list.members || [], list.ts || Date.now()).catch(() => {});
    await publishZoneListRequest(sw, ident, z).catch(() => {});
    await publishZoneProbe(sw, ident, z).catch(() => {});
    pokeUi(sw);
    return;
  }

  // --- Zone probe (request presence) ---
  if (payload.type === 'zone_probe') {
    if (!ident?.linked || !ident?.id) return;
    const z = String(payload.zone || '').trim();
    if (!z) return;
    const ok = await isZoneJoined(ident, z);
    if (!ok) return;
    const dev = await getDevice();
    await publishZonePresence(sw, ident, dev, z).catch(() => {});
    return;
  }

  // --- Identity label update ---
  if (payload.type === 'identity_label_update') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;
    const nextLabel = String(payload.newLabel || '').trim();
    if (!nextLabel) return;
    ident.label = nextLabel;
    await setIdentity(ident);
    await relaySubscribeOnRelayOpen(sw, ident).catch(() => {});
    pokeUi(sw);
    return;
  }

  // --- Device label update ---
  if (payload.type === 'device_label_update') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;
    const targetPk = String(payload.devicePk || '').trim();
    if (!targetPk) return;
    ident.devices = Array.isArray(ident.devices) ? ident.devices : [];
    for (const d of ident.devices) {
      if (d?.pk === targetPk) {
        d.label = String(payload.deviceLabel || d.label || '').trim();
        if (payload.deviceDid) d.did = String(payload.deviceDid || d.did || '').trim();
      }
    }
    await setIdentity(ident);
    pokeUi(sw);
    return;
  }

  // --- Pair claim (owner enters joiner-generated code) ---
  if (payload.type === 'pair_claim') {
    const pendingLabel = String((await kvGet('pendingJoinIdentityLabel')) || '').trim();
    if (!pendingLabel) return;
    if (String(payload.identity || '').trim() !== pendingLabel) return;

    const offer = (await kvGet(PAIR_OFFER_KEY)) || null;
    if (!offer || String(offer.identityLabel || '').trim() !== pendingLabel) return;

    const nowMs = Date.now();
    const expiresAt = Number(offer.expiresAt || 0);
    if (expiresAt && nowMs > expiresAt) {
      await kvSet(PAIR_OFFER_KEY, null);
      return;
    }

    const claimHash = String(payload.codeHash || '').trim();
    const offerHash = String(offer.codeHash || '').trim();
    if (!claimHash || !offerHash || claimHash !== offerHash) return;

    const ownerPk = String(payload.fromPk || ev.pubkey || '').trim();
    if (!ownerPk) return;
    if (String(payload.fromPk || '').trim() && ownerPk !== String(ev.pubkey || '').trim()) return;

    const reqId = `req-${b64url(randomBytes(8))}`;
    await publishAppEvent(sw, {
      type: 'pair_request',
      identity: pendingLabel,
      requestId: reqId,
      claimId: String(payload.claimId || '').trim(),
      codeHash: offerHash,
      devicePk: String(offer.devicePk || dev?.nostr?.pk || '').trim(),
      deviceDid: String(offer.deviceDid || dev?.did || '').trim(),
      deviceLabel: String(offer.deviceLabel || '').trim(),
      ts: Date.now(),
      ttl: 120,
    }, [['i', pendingLabel], ['p', ownerPk], ...zoneTagsFromEvent(ev)]);

    log(sw, `pair_claim matched; pair_request published requestId=${reqId}`);
    pokeUi(sw);
    return;
  }

  // --- Pair request ---
  if (payload.type === 'pair_request') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) {
      log(sw, `pair_request ignored: label mismatch payload=${payload.identity} ident=${ident.label}`);
      return;
    }

    // Ignore requests from blocked devices.
    if (await blockedIs({ pk: payload.devicePk, did: payload.deviceDid })) return;

    const known = new Set((ident.devices || []).map(d => d.pk).filter(Boolean));
    if (payload.devicePk && known.has(payload.devicePk)) {
      const staleId = `${payload.identity}:${payload.code}:${payload.devicePk}`;
      await pendingRemove(staleId);
      return;
    }

    const codeHash = String(payload.codeHash || '').trim();
    let autoApprove = false;
    if (codeHash) {
      const activeClaim = (await kvGet(PAIR_CLAIM_ACTIVE_KEY)) || null;
      const nowMs = Date.now();
      const claimOk = !!activeClaim
        && String(activeClaim.identityLabel || '').trim() === String(payload.identity || '').trim()
        && String(activeClaim.codeHash || '').trim() === codeHash
        && nowMs <= Number(activeClaim.expiresAt || 0);
      if (!claimOk) {
        log(sw, 'pair_request ignored: no active matching code claim');
        return;
      }
      autoApprove = !!activeClaim?.autoApprove;
      await kvSet(PAIR_CLAIM_ACTIVE_KEY, null);
    }

    const reqId = String(payload.requestId || '').trim()
      || `${payload.identity}:${payload.codeHash || payload.code || ''}:${payload.devicePk}`;
    const req = {
      id: reqId,
      identityLabel: payload.identity,
      code: payload.code,
      codeHash,
      devicePk: payload.devicePk,
      deviceDid: payload.deviceDid,
      deviceLabel: payload.deviceLabel || '',
      ts: Date.now(),
      status: 'pending',
    };

    if (autoApprove) {
      try {
        const approved = await autoApprovePairRequest(sw, ident, req);
        if (!approved) {
          log(sw, 'pair_request auto-approve failed: missing identity state');
          return;
        }
      } catch (e) {
        log(sw, `pair_request auto-approve failed: ${String(e?.message || e)}`);
        return;
      }
      pokeUi(sw);
      return;
    }

    await pendingAdd(req);

    await notifAdd({
      id: `n-pair-${reqId}`,
      kind: 'pairing',
      title: 'Pairing request',
      body: `${req.deviceLabel || 'Device'} wants to join ${payload.identity} (code ${payload.code})`,
      ts: Date.now(),
      read: false,
    });

    pokeUi(sw);
    return;
  }

  // --- Pair approve (encrypted room key) ---
  if (payload.type === 'pair_approve') {
    if (payload.toPk !== dev.nostr.pk) return;

    try {
      const plaintext = await nip04Decrypt(dev.nostr.skHex, payload.fromPk, payload.encryptedRoomKey);
      const obj = JSON.parse(plaintext);

      // Adopt identity
      await setIdentity({
        id: String(obj.identityId || ''),
        label: String(payload.identity || ''),
        roomKeyB64: String(obj.roomKeyB64 || ''),
        linked: true,
        devices: Array.isArray(obj.devices) ? obj.devices : [],
      });
      await kvSet(PAIR_OFFER_KEY, null);
      await kvSet('pendingJoinIdentityLabel', '');

      await notifAdd({
        id: `n-approve-${payload.identity}-${payload.code}`,
        kind: 'pairing',
        title: 'Pairing approved',
        body: `Approved for ${payload.identity} (code ${payload.code})`,
        ts: Date.now(),
        read: false,
      });

      pokeUi(sw);
    } catch (e) {
      log(sw, `pair_approve decrypt failed: ${String(e?.message || e)}`);
    }
    return;
  }

  // --- Pair reject ---
  if (payload.type === 'pair_reject') {
    if (payload.toPk !== dev.nostr.pk) return;

    // Add blocker for the identity that rejected us (defensive).
    if (payload.fromPk) await blockedAdd({ pk: payload.fromPk, reason: 'rejected_by' });

    await notifAdd({
      id: `n-reject-${payload.identity}-${payload.code}`,
      kind: 'pairing',
      title: 'Pairing rejected',
      body: `Request rejected for ${payload.identity} (code ${payload.code})`,
      ts: Date.now(),
      read: false,
    });

    pokeUi(sw);
    return;
  }

  // --- Pair resolved ---
  if (payload.type === 'pair_resolved') {
    if (!ident?.label || payload.identity !== ident.label) return;
    const rid = String(payload.requestId || '');
    if (rid) await pendingRemove(rid);
    if (rid) await notifRemove(`n-pair-${rid}`).catch(() => {});
    if (payload.devicePk && payload.code) {
      const alt = `${payload.identity}:${payload.code}:${payload.devicePk}`;
      await pendingRemove(alt);
    }
    pokeUi(sw);
    return;
  }

  // --- Notifications clear ---
  if (payload.type === 'notifications_clear') {
    if (!ident?.label || payload.identity !== ident.label) return;
    await notifClear();
    pokeUi(sw);
    return;
  }

  // --- Room key update (PHASE-1 rotation distribution) ---
  if (payload.type === 'room_key_update') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;
    if (payload.toPk !== dev.nostr.pk) return;

    try {
      const plaintext = await nip04Decrypt(dev.nostr.skHex, payload.fromPk, payload.encryptedRoomKey);
      const obj = JSON.parse(plaintext);
      if (!obj?.roomKeyB64) throw new Error('bad key update payload');

      ident.roomKeyB64 = obj.roomKeyB64;
      await setIdentity(ident);
      pokeUi(sw);
    } catch (e) {
      log(sw, `room_key_update decrypt failed: ${String(e?.message || e)}`);
    }
    return;
  }

  // --- Device revoked (PHASE-1 convergence) ---
  if (payload.type === 'device_revoked') {
    if (!ident?.linked || !ident?.label) return;
    if (payload.identity !== ident.label) return;

    const targetPk = String(payload.targetPk || '').trim();
    if (!targetPk) return;

    // Add to local blacklist.
    await blockedAdd({ pk: targetPk, reason: 'revoked' });

    // If it’s us: unlink immediately (we will not receive new keys)
    if (targetPk === dev.nostr.pk) {
      await setIdentity({
        id: '',
        label: '',
        roomKeyB64: '',
        linked: false,
        devices: [],
      });
      await notifAdd({
        id: `n-revoked-${Date.now()}`,
        kind: 'security',
        title: 'Device revoked',
        body: 'This device was removed from the identity.',
        ts: Date.now(),
        read: false,
      });
      pokeUi(sw);
      return;
    }

    // Otherwise remove from our known devices list
    ident.devices = (ident.devices || []).filter(d => d.pk !== targetPk);
    await setIdentity(ident);
    pokeUi(sw);
    return;
  }

  // --- Swarm discovery / DHT records ---
  if (payload.type === 'swarm_identity_record' && payload.record) {
    const ok = await validateRecord(payload.record, 'identity');
    if (!ok) { log(sw, 'swarm identity record rejected'); return; }
    await putIdentityRecord(payload.record).catch(() => {});
    log(sw, 'swarm identity record stored');
    pokeUi(sw);
    return;
  }
  if (payload.type === 'swarm_device_record' && payload.record) {
    const ok = await validateRecord(payload.record, 'device');
    if (!ok) { log(sw, 'swarm device record rejected'); return; }
    await putDeviceRecord(payload.record).catch(() => {});
    log(sw, 'swarm device record stored');
    pokeUi(sw);
    return;
  }
  if (payload.type === 'swarm_dht_record' && payload.record) {
    const ok = await validateRecord(payload.record, 'dht');
    if (!ok) { log(sw, 'swarm dht record rejected'); return; }
    await putDhtRecord(payload.record).catch(() => {});
    log(sw, 'swarm dht record stored');
    pokeUi(sw);
    return;
  }
  if (payload.type === 'swarm_record_request') {
    if (!ident?.linked) return;
    const requestId = String(payload.requestId || '').trim();
    const want = Array.isArray(payload.want) && payload.want.length
      ? payload.want.map(String)
      : ['identity', 'device', 'dht'];
    const wantedIdentityId = String(payload.identityId || '').trim();
    const wantedDevicePk = String(payload.devicePk || '').trim();

    if (want.includes('identity')) {
      if (!wantedIdentityId || wantedIdentityId === String(ident.id || '').trim()) {
        const rec = await makeIdentityRecord().catch(() => null);
        if (rec) await publishAppEvent(sw, { type: 'swarm_identity_record', requestId, record: rec }).catch(() => {});
      }
    }
    if (want.includes('device')) {
      if (!wantedDevicePk || wantedDevicePk === String(dev?.nostr?.pk || '').trim()) {
        const rec = await makeDeviceRecord().catch(() => null);
        if (rec) await publishAppEvent(sw, { type: 'swarm_device_record', requestId, record: rec }).catch(() => {});
      }
    }
    if (want.includes('dht')) {
      const scope = String(payload.scope || payload.dhtScope || '').trim();
      const key = String(payload.key || payload.dhtKey || '').trim();
      if (scope && key) {
        const rec = await getDhtRecord(scope, key).catch(() => null);
        if (rec) await publishAppEvent(sw, { type: 'swarm_dht_record', requestId, record: rec }).catch(() => {});
      }
    }
    if (requestId) {
      await publishAppEvent(sw, { type: 'swarm_record_response', requestId, status: 'complete', ts: Date.now() }).catch(() => {});
    }
    return;
  }
  if (payload.type === 'swarm_dht_get') {
    const requestId = String(payload.requestId || '').trim();
    const scope = String(payload.scope || payload.dhtScope || '').trim();
    const key = String(payload.key || payload.dhtKey || '').trim();
    if (!scope || !key) return;
    const rec = await getDhtRecord(scope, key).catch(() => null);
    if (rec) await publishAppEvent(sw, { type: 'swarm_dht_record', requestId, record: rec }).catch(() => {});
    if (requestId) {
      await publishAppEvent(sw, { type: 'swarm_record_response', requestId, status: 'complete', ts: Date.now() }).catch(() => {});
    }
    return;
  }
  if (payload.type === 'swarm_dht_put') {
    const requestId = String(payload.requestId || '').trim();
    const scope = String(payload.scope || payload.dhtScope || '').trim();
    const key = String(payload.key || payload.dhtKey || '').trim();
    if (!scope || !key) return;
    if (!Object.prototype.hasOwnProperty.call(payload, 'value')) return;

    const rec = await makeDhtRecord(scope, key, payload.value, {
      updatedAt: Number(payload.updatedAt || Date.now()),
      expiresAt: Number(payload.expiresAt || (Date.now() + 24 * 60 * 60 * 1000)),
    }).catch(() => null);

    if (rec) {
      await putDhtRecord(rec).catch(() => {});
      await publishAppEvent(sw, { type: 'swarm_dht_record', requestId, record: rec }).catch(() => {});
      pokeUi(sw);
    }

    if (requestId) {
      await publishAppEvent(sw, { type: 'swarm_record_response', requestId, status: 'complete', ts: Date.now() }).catch(() => {});
    }
    return;
  }
  if (payload.type === 'swarm_record_response') {
    emit(sw, {
      type: 'swarm_record_response',
      requestId: String(payload.requestId || '').trim(),
      status: String(payload.status || ''),
      ts: Number(payload.ts || Date.now()),
    });
    return;
  }

  if (payload.type === 'gateway_service_install_status') {
    emit(sw, {
      type: 'gateway_service_install_status',
      requestId: String(payload.requestId || '').trim(),
      status: String(payload.status || '').trim(),
      service: String(payload.service || '').trim(),
      action: String(payload.action || '').trim(),
      gatewayPk: String(payload.gatewayPk || '').trim(),
      toDevicePk: String(payload.toDevicePk || '').trim(),
      identityId: String(payload.identityId || '').trim(),
      reason: String(payload.reason || '').trim(),
      detail: String(payload.detail || '').trim(),
      zone: String(payload.zone || '').trim(),
      ts: Number(payload.ts || Date.now()),
    });
    return;
  }

  if (payload.type === 'gateway_zone_sync_status') {
    emit(sw, {
      type: 'gateway_zone_sync_status',
      requestId: String(payload.requestId || '').trim(),
      status: String(payload.status || '').trim(),
      gatewayPk: String(payload.gatewayPk || '').trim(),
      toDevicePk: String(payload.toDevicePk || '').trim(),
      identityId: String(payload.identityId || '').trim(),
      reason: String(payload.reason || '').trim(),
      detail: String(payload.detail || '').trim(),
      zone: String(payload.zone || '').trim(),
      zoneKeys: Array.isArray(payload.zoneKeys) ? payload.zoneKeys.map((z) => String(z || '').trim()).filter(Boolean) : [],
      extraZoneKeys: Array.isArray(payload.extraZoneKeys) ? payload.extraZoneKeys.map((z) => String(z || '').trim()).filter(Boolean) : [],
      restartRequired: payload.restartRequired === true,
      ts: Number(payload.ts || Date.now()),
    });
    return;
  }

  if (payload.type === 'gateway_managed_launch_status') {
    const toDevicePk = String(payload.toDevicePk || '').trim();
    const devicePk = String(payload.devicePk || '').trim();
    const localPk = String(dev?.nostr?.pk || '').trim();
    if (localPk) {
      const directedToOtherDevice =
        (toDevicePk && toDevicePk !== localPk)
        && (devicePk && devicePk !== localPk);
      if (directedToOtherDevice) return;
    }
    emit(sw, {
      type: 'gateway_managed_launch_status',
      requestId: String(payload.requestId || '').trim(),
      status: String(payload.status || '').trim(),
      gatewayPk: String(payload.gatewayPk || '').trim(),
      toDevicePk,
      identityId: String(payload.identityId || '').trim(),
      devicePk,
      servicePk: String(payload.servicePk || '').trim(),
      service: String(payload.service || '').trim(),
      capability: String(payload.capability || '').trim(),
      launchToken: String(payload.launchToken || '').trim(),
      expiresAt: Number(payload.expiresAt || 0),
      display: payload.display ?? null,
      reason: String(payload.reason || '').trim(),
      detail: String(payload.detail || '').trim(),
      ts: Number(payload.ts || Date.now()),
    });
    return;
  }

  if (payload.type === 'gateway_signal_status') {
    const devicePk = String(payload.devicePk || '').trim();
    if (devicePk && devicePk !== String(dev?.nostr?.pk || '').trim()) return;
    emit(sw, {
      type: 'gateway_signal_status',
      requestId: String(payload.requestId || '').trim(),
      status: String(payload.status || '').trim(),
      gatewayPk: String(payload.gatewayPk || '').trim(),
      identityId: String(payload.identityId || '').trim(),
      devicePk,
      servicePk: String(payload.servicePk || '').trim(),
      service: String(payload.service || '').trim(),
      signalType: String(payload.signalType || '').trim(),
      reason: String(payload.reason || '').trim(),
      detail: String(payload.detail || '').trim(),
      ts: Number(payload.ts || Date.now()),
    });
    return;
  }

  if (payload.type === 'gateway_signal') {
    const devicePk = String(payload.devicePk || '').trim();
    if (devicePk && devicePk !== String(dev?.nostr?.pk || '').trim()) return;
    emit(sw, {
      type: 'gateway_signal',
      requestId: String(payload.requestId || '').trim(),
      gatewayPk: String(payload.gatewayPk || '').trim(),
      identityId: String(payload.identityId || '').trim(),
      devicePk,
      servicePk: String(payload.servicePk || '').trim(),
      service: String(payload.service || '').trim(),
      signalType: String(payload.signalType || '').trim(),
      payload: payload.payload ?? null,
      ts: Number(payload.ts || Date.now()),
    });
    return;
  }

  // --- Swarm signal (WebRTC signaling) ---
  if (payload.type === 'swarm_signal') {
    const to = String(payload.to || '').trim();
    if (!to || to !== dev?.nostr?.pk) return;
    emit(sw, {
      type: 'swarm_signal',
      from: String(payload.from || '').trim(),
      signalType: String(payload.signalType || '').trim(),
      data: payload.data ?? null,
    });
    return;
  }
}
