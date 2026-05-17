import {
  relayRxIngressClassification,
  relayRxIngressLanePosture,
} from './runtime-relay-admission.js';

const RELAY_WORKER_BUILD_ID = '2026-04-06-runtime-stage3';
const MAX_RECENT_EVENT_IDS = 2048;
const RELAY_INGRESS_STATUS_INTERVAL_MS = 5_000;

const endpoints = new Set();
const relays = new Map();
const recentEventIds = new Map();
const relayIngressCounters = {
  accepted: 0,
  forwarded: 0,
  filtered: 0,
  duplicate: 0,
  invalid: 0,
  staleReplay: 0,
  futureReplay: 0,
  irrelevant: 0,
  nonEvent: 0,
  directory: 0,
  account: 0,
  authority: 0,
  route: 0,
  projection: 0,
  acceptedByLane: {},
  acceptedByReason: {},
  filteredByReason: {},
};
let desiredRelayUrls = [];
let dedicatedEndpoint = null;
let relayIngressLastStatusAt = 0;
let relayIngressDirty = false;
let relayAdmissionContext = { surface: 'constitute-account' };

function endpointPost(endpoint, msg) {
  try {
    if (endpoint.kind === 'shared') endpoint.port.postMessage(msg);
    else self.postMessage(msg);
  } catch {}
}

function broadcast(msg) {
  for (const endpoint of endpoints) endpointPost(endpoint, msg);
}

function currentRelayUrls() {
  return Array.from(relays.keys());
}

function relaySnapshot() {
  const out = {};
  for (const [url, relay] of relays.entries()) {
    out[url] = {
      state: relay.state,
      code: relay.code ?? null,
      reason: relay.reason ?? '',
      attempt: relay.reconnectAttempt ?? 0,
    };
  }
  return out;
}

function relayIngressSnapshot() {
  const observedAt = Date.now();
  const laneCounts = (lane) => {
    const total = Number(relayIngressCounters[lane] || 0);
    const accepted = Number(relayIngressCounters.acceptedByLane?.[lane] || 0);
    return {
      total,
      accepted,
      filtered: Math.max(0, total - accepted),
    };
  };
  const filteredCounts = () => ({
    total: Number(relayIngressCounters.filtered || 0),
    invalid: Number(relayIngressCounters.invalid || 0),
    staleReplay: Number(relayIngressCounters.staleReplay || 0),
    futureReplay: Number(relayIngressCounters.futureReplay || 0),
    irrelevant: Number(relayIngressCounters.irrelevant || 0),
    duplicate: Number(relayIngressCounters.duplicate || 0),
  });
  return {
    kind: 'relay.ingress.snapshot',
    observedAt,
    counters: { ...relayIngressCounters },
    lanes: {
      'relay.directory': relayRxIngressLanePosture({
        lane: 'directory',
        laneKind: 'control',
        counts: laneCounts('directory'),
        limits: { recentEventIds: MAX_RECENT_EVENT_IDS },
        relevanceRefs: ['relay.tag:swarm_discovery'],
        sampledAt: observedAt,
      }),
      'relay.account': relayRxIngressLanePosture({
        lane: 'account',
        laneKind: 'app',
        counts: laneCounts('account'),
        limits: { statusIntervalMs: RELAY_INGRESS_STATUS_INTERVAL_MS },
        relevanceRefs: ['relay.tag:constitute'],
        sampledAt: observedAt,
      }),
      'relay.authority': relayRxIngressLanePosture({
        lane: 'authority',
        laneKind: 'control',
        counts: laneCounts('authority'),
        limits: { statusIntervalMs: RELAY_INGRESS_STATUS_INTERVAL_MS },
        relevanceRefs: ['relay.payload:pair_*'],
        sampledAt: observedAt,
      }),
      'relay.route': relayRxIngressLanePosture({
        lane: 'route',
        laneKind: 'control',
        counts: laneCounts('route'),
        limits: { statusIntervalMs: RELAY_INGRESS_STATUS_INTERVAL_MS },
        relevanceRefs: ['relay.payload:swarm_signal'],
        sampledAt: observedAt,
      }),
      'relay.projection': relayRxIngressLanePosture({
        lane: 'projection',
        laneKind: 'projection',
        counts: laneCounts('projection'),
        limits: { statusIntervalMs: RELAY_INGRESS_STATUS_INTERVAL_MS },
        relevanceRefs: ['relay.payload:swarm_*'],
        sampledAt: observedAt,
      }),
      'relay.filtered': relayRxIngressLanePosture({
        lane: 'irrelevant',
        laneKind: 'drop-before-app',
        counts: filteredCounts(),
        limits: {
          recentEventIds: MAX_RECENT_EVENT_IDS,
          statusIntervalMs: RELAY_INGRESS_STATUS_INTERVAL_MS,
        },
        relevanceRefs: ['relay.filter:pre-sw'],
        sampledAt: observedAt,
      }),
    },
  };
}

function emitIngressStatus({ force = false } = {}) {
  const now = Date.now();
  if (!force && (!relayIngressDirty || now - relayIngressLastStatusAt < RELAY_INGRESS_STATUS_INTERVAL_MS)) return;
  relayIngressDirty = false;
  relayIngressLastStatusAt = now;
  broadcast({
    type: 'relay.ingress.status',
    version: RELAY_WORKER_BUILD_ID,
    ingress: relayIngressSnapshot(),
  });
}

function incrementIngressCounter(classification) {
  const lane = String(classification?.lane || 'invalid');
  const reason = String(classification?.reason || 'unknown');
  if (classification?.accepted) relayIngressCounters.accepted += 1;
  else relayIngressCounters.filtered += 1;
  if (Object.prototype.hasOwnProperty.call(relayIngressCounters, lane)) {
    relayIngressCounters[lane] += 1;
  } else if (!classification?.accepted) {
    relayIngressCounters.irrelevant += 1;
  }
  if (classification?.accepted) {
    relayIngressCounters.acceptedByLane[lane] = (relayIngressCounters.acceptedByLane[lane] || 0) + 1;
    relayIngressCounters.acceptedByReason[reason] = (relayIngressCounters.acceptedByReason[reason] || 0) + 1;
  } else {
    relayIngressCounters.filteredByReason[reason] = (relayIngressCounters.filteredByReason[reason] || 0) + 1;
  }
  relayIngressDirty = true;
}

function aggregateRelayState() {
  if (relays.size === 0) return { state: 'offline', code: null, reason: 'no relays configured' };
  const snapshot = Array.from(relays.values());
  if (snapshot.some((relay) => relay.state === 'open')) return { state: 'open', code: null, reason: '' };
  if (snapshot.some((relay) => relay.state === 'connecting')) {
    return { state: 'connecting', code: null, reason: 'waiting for relay open' };
  }
  const firstFailure = snapshot.find((relay) => relay.code != null || relay.reason);
  return {
    state: 'error',
    code: firstFailure?.code ?? null,
    reason: firstFailure?.reason || 'all relays unavailable',
  };
}

function emitStatus() {
  const aggregate = aggregateRelayState();
  broadcast({
    type: 'relay.status',
    version: RELAY_WORKER_BUILD_ID,
    state: aggregate.state,
    code: aggregate.code,
    reason: aggregate.reason,
    urls: currentRelayUrls(),
    relays: relaySnapshot(),
    ingress: relayIngressSnapshot(),
  });
}

function clearReconnect(relay) {
  if (relay.reconnectTimer) {
    clearTimeout(relay.reconnectTimer);
    relay.reconnectTimer = null;
  }
}

function rememberEventId(id) {
  const key = String(id || '').trim();
  if (!key) return true;
  if (recentEventIds.has(key)) return false;
  recentEventIds.set(key, Date.now());
  while (recentEventIds.size > MAX_RECENT_EVENT_IDS) {
    const oldestKey = recentEventIds.keys().next().value;
    if (oldestKey == null) break;
    recentEventIds.delete(oldestKey);
  }
  return true;
}

function normalizeRelayAdmissionContext(input = {}) {
  const source = input && typeof input === 'object' ? input : {};
  const zoneKeys = [
    ...(Array.isArray(source.zoneKeys) ? source.zoneKeys : []),
    ...(Array.isArray(source.zones) ? source.zones : []),
  ].map((value) => String(value?.key || value || '').trim()).filter(Boolean);
  return {
    surface: String(source.surface || 'constitute-account').trim(),
    memberRef: String(source.memberRef || '').trim(),
    subscriberRef: String(source.subscriberRef || source.memberRef || '').trim(),
    identity: String(source.identity || source.identityLabel || '').trim(),
    ownerId: String(source.ownerId || '').trim(),
    zoneKeys: Array.from(new Set(zoneKeys)),
  };
}

function classifyRelayWorkerFrame(frame, observedAt = Date.now()) {
  const classification = relayRxIngressClassification(frame, {
    observedAt,
    context: relayAdmissionContext,
  });
  const eventId = String(classification?.admission?.subject?.relayEventId || '').trim();
  return {
    ...classification,
    eventId,
  };
}

function scheduleReconnect(relay) {
  clearReconnect(relay);
  if (!desiredRelayUrls.includes(relay.url)) return;
  relay.reconnectAttempt += 1;
  const delayMs = Math.min(15_000, 1_000 * Math.max(1, relay.reconnectAttempt));
  relay.state = 'connecting';
  relay.reason = `reconnect in ${delayMs}ms`;
  emitStatus();
  relay.reconnectTimer = setTimeout(() => {
    relay.reconnectTimer = null;
    connectRelay(relay, { isRetry: true });
  }, delayMs);
}

function connectRelay(relay, { isRetry = false } = {}) {
  clearReconnect(relay);
  try { if (relay.ws) relay.ws.close(); } catch {}
  relay.ws = null;
  relay.state = 'connecting';
  relay.code = null;
  relay.reason = isRetry ? `retry ${relay.reconnectAttempt}` : '';
  emitStatus();

  const ws = new WebSocket(relay.url);
  relay.ws = ws;

  ws.onopen = () => {
    relay.reconnectAttempt = 0;
    relay.state = 'open';
    relay.code = null;
    relay.reason = '';
    emitStatus();
  };

  ws.onerror = () => {
    relay.state = 'error';
    relay.reason = relay.reason || 'socket error';
    emitStatus();
  };

  ws.onclose = (event) => {
    relay.ws = null;
    relay.state = 'closed';
    relay.code = event?.code ?? null;
    relay.reason = event?.reason ?? relay.reason ?? '';
    emitStatus();
    scheduleReconnect(relay);
  };

  ws.onmessage = (event) => {
    const data = String(event.data || '');
    const classification = classifyRelayWorkerFrame(data);
    if (classification.accepted && !rememberEventId(classification.eventId)) {
      incrementIngressCounter({ accepted: false, lane: 'duplicate', reason: 'duplicateEvent' });
      emitIngressStatus();
      return;
    }
    incrementIngressCounter(classification);
    if (classification.accepted) relayIngressCounters.forwarded += 1;
    emitIngressStatus();
    if (!classification.accepted) return;
    broadcast({ type: 'relay.rx', data, url: relay.url, workerAdmission: classification });
  };
}

function ensureRelay(url) {
  const key = String(url || '').trim();
  if (!key) return null;
  if (relays.has(key)) return relays.get(key);
  const relay = {
    url: key,
    ws: null,
    state: 'offline',
    code: null,
    reason: '',
    reconnectAttempt: 0,
    reconnectTimer: null,
  };
  relays.set(key, relay);
  return relay;
}

function closeRelay(url) {
  const relay = relays.get(url);
  if (!relay) return;
  clearReconnect(relay);
  try { if (relay.ws) relay.ws.close(); } catch {}
  relays.delete(url);
}

function updateRelayTargets(urls) {
  const nextUrls = Array.isArray(urls)
    ? urls.map((url) => String(url || '').trim()).filter(Boolean)
    : [];
  desiredRelayUrls = nextUrls;

  for (const url of currentRelayUrls()) {
    if (!nextUrls.includes(url)) closeRelay(url);
  }

  for (const url of nextUrls) {
    const relay = ensureRelay(url);
    if (!relay) continue;
    if (relay.ws && relay.ws.readyState === WebSocket.OPEN) continue;
    if (relay.ws && relay.ws.readyState === WebSocket.CONNECTING) continue;
    connectRelay(relay);
  }

  emitStatus();
}

function send(frame) {
  let sent = false;
  for (const relay of relays.values()) {
    if (!relay.ws || relay.ws.readyState !== WebSocket.OPEN) continue;
    relay.ws.send(String(frame));
    sent = true;
  }
  if (!sent) throw new Error('relay not open');
}

function handleControlMessage(msg, endpoint) {
  try {
    if (msg.type === 'relay.connect') {
      relayAdmissionContext = normalizeRelayAdmissionContext(msg.context || relayAdmissionContext);
      const urls = Array.isArray(msg.urls) ? msg.urls : (msg.url ? [msg.url] : []);
      updateRelayTargets(urls);
      endpointPost(endpoint, { type: 'relay.ack', ok: true });
      return;
    }
    if (msg.type === 'relay.context') {
      relayAdmissionContext = normalizeRelayAdmissionContext(msg.context || {});
      endpointPost(endpoint, { type: 'relay.ack', ok: true });
      return;
    }
    if (msg.type === 'relay.send') {
      send(msg.frame);
      endpointPost(endpoint, { type: 'relay.ack', ok: true });
      return;
    }
    if (msg.type === 'relay.status') {
      const aggregate = aggregateRelayState();
      endpointPost(endpoint, {
        type: 'relay.status',
        version: RELAY_WORKER_BUILD_ID,
        state: aggregate.state,
        code: aggregate.code,
        reason: aggregate.reason,
        urls: currentRelayUrls(),
        relays: relaySnapshot(),
        ingress: relayIngressSnapshot(),
      });
      return;
    }
    if (msg.type === 'relay.close') {
      desiredRelayUrls = [];
      for (const url of currentRelayUrls()) closeRelay(url);
      emitStatus();
      endpointPost(endpoint, { type: 'relay.ack', ok: true });
    }
  } catch (err) {
    endpointPost(endpoint, { type: 'relay.ack', ok: false, error: String(err?.message || err) });
  }
}

function attachSharedPort(port) {
  const endpoint = { kind: 'shared', port };
  endpoints.add(endpoint);
  port.onmessage = (event) => handleControlMessage(event.data || {}, endpoint);
  port.start();
  endpointPost(endpoint, {
    type: 'relay.status',
    version: RELAY_WORKER_BUILD_ID,
    state: aggregateRelayState().state,
    code: aggregateRelayState().code,
    reason: aggregateRelayState().reason,
    urls: currentRelayUrls(),
    relays: relaySnapshot(),
    ingress: relayIngressSnapshot(),
  });
}

function ensureDedicatedEndpoint() {
  if (dedicatedEndpoint) return dedicatedEndpoint;
  dedicatedEndpoint = { kind: 'dedicated' };
  endpoints.add(dedicatedEndpoint);
  endpointPost(dedicatedEndpoint, {
    type: 'relay.status',
    version: RELAY_WORKER_BUILD_ID,
    state: aggregateRelayState().state,
    code: aggregateRelayState().code,
    reason: aggregateRelayState().reason,
    urls: currentRelayUrls(),
    relays: relaySnapshot(),
    ingress: relayIngressSnapshot(),
  });
  return dedicatedEndpoint;
}

self.onconnect = (event) => {
  const port = event.ports?.[0];
  if (port) attachSharedPort(port);
};

self.addEventListener('message', (event) => {
  const endpoint = ensureDedicatedEndpoint();
  handleControlMessage(event.data || {}, endpoint);
});

