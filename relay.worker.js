const RELAY_WORKER_BUILD_ID = '2026-04-06-runtime-stage3';
const MAX_RECENT_EVENT_IDS = 2048;

const endpoints = new Set();
const relays = new Map();
const recentEventIds = new Map();
let desiredRelayUrls = [];
let dedicatedEndpoint = null;

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

function extractEventId(frame) {
  try {
    const parsed = JSON.parse(String(frame || ''));
    if (!Array.isArray(parsed) || parsed[0] !== 'EVENT') return '';
    const event = parsed.length >= 3 ? parsed[2] : parsed[1];
    return event && typeof event === 'object' ? String(event.id || '').trim() : '';
  } catch {
    return '';
  }
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
    const eventId = extractEventId(data);
    if (!rememberEventId(eventId)) return;
    broadcast({ type: 'relay.rx', data, url: relay.url });
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
      const urls = Array.isArray(msg.urls) ? msg.urls : (msg.url ? [msg.url] : []);
      updateRelayTargets(urls);
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

