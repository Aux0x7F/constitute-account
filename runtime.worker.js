const RUNTIME_WORKER_BUILD_ID = '2026-04-03-runtime-v1';
const CONTEXT_TTL_FALLBACK_MS = 2 * 60 * 1000;

const endpoints = new Map();
const launchContexts = new Map();
const pendingBrokerRequests = new Map();
const projectionStore = {
  discoverable: new Map(),
  owned: new Map(),
  granted: new Map(),
  session: new Map(),
};
const runtimeStatus = {
  buildId: RUNTIME_WORKER_BUILD_ID,
  updatedAt: 0,
  shell: null,
  services: {},
};

let brokerClientId = '';
let dedicatedEndpoint = null;

function nowMs() {
  return Date.now();
}

function safeClone(value) {
  if (value == null) return value;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

function projectionCategoryMap(category) {
  const key = String(category || '').trim().toLowerCase();
  return projectionStore[key] || null;
}

function cloneProjectionStore() {
  const out = {};
  for (const [category, store] of Object.entries(projectionStore)) {
    out[category] = Object.fromEntries(Array.from(store.entries()).map(([key, value]) => [key, safeClone(value)]));
  }
  return out;
}

function cleanupLaunchContexts() {
  const now = nowMs();
  for (const [launchId, context] of launchContexts.entries()) {
    const expiresAt = Number(context?.expiresAt || 0) || (Number(context?.createdAt || 0) + CONTEXT_TTL_FALLBACK_MS);
    if (expiresAt && expiresAt <= now) {
      launchContexts.delete(launchId);
    }
  }
}

function runtimeSnapshot() {
  cleanupLaunchContexts();
  return {
    buildId: RUNTIME_WORKER_BUILD_ID,
    updatedAt: runtimeStatus.updatedAt || nowMs(),
    brokerClientId,
    shell: safeClone(runtimeStatus.shell),
    services: safeClone(runtimeStatus.services),
    launchContextCount: launchContexts.size,
    projections: cloneProjectionStore(),
  };
}

function endpointPost(endpoint, message) {
  try {
    if (endpoint.kind === 'shared') {
      endpoint.port.postMessage(message);
      return;
    }
    self.postMessage(message);
  } catch {}
}

function broadcast(message) {
  for (const endpoint of endpoints.values()) {
    endpointPost(endpoint, message);
  }
}

function broadcastSnapshot() {
  const snapshot = runtimeSnapshot();
  broadcast({
    type: 'status.snapshot',
    snapshot,
  });
}

function ensureEndpoint(clientId, endpoint, meta = {}) {
  const key = String(clientId || '').trim();
  if (!key) return null;
  const previousKey = String(endpoint?.clientId || '').trim();
  if (previousKey && previousKey !== key) {
    endpoints.delete(previousKey);
  }
  endpoint.clientId = key;
  endpoint.surface = String(meta.surface || '').trim();
  endpoint.broker = meta.broker === true;
  endpoints.set(key, endpoint);
  if (endpoint.broker) brokerClientId = key;
  return endpoint;
}

function deleteEndpoint(clientId) {
  const key = String(clientId || '').trim();
  if (!key) return;
  endpoints.delete(key);
  if (brokerClientId === key) {
    brokerClientId = '';
    for (const endpoint of endpoints.values()) {
      if (endpoint.broker) {
        brokerClientId = endpoint.clientId;
        break;
      }
    }
  }
}

function handleStatusUpdate(message, endpoint) {
  const role = String(message.role || message.surface || endpoint?.surface || '').trim().toLowerCase();
  const payload = message.status && typeof message.status === 'object' ? message.status : {};
  runtimeStatus.updatedAt = nowMs();

  if (role === 'shell') {
    runtimeStatus.shell = {
      ...safeClone(payload),
      updatedAt: runtimeStatus.updatedAt,
    };
    broadcastSnapshot();
    return { ok: true };
  }

  const service = String(message.service || payload.service || endpoint?.surface || '').trim().toLowerCase();
  if (!service) {
    return { ok: false, error: 'missing service status target' };
  }
  runtimeStatus.services[service] = {
    ...safeClone(payload),
    service,
    updatedAt: runtimeStatus.updatedAt,
  };
  broadcastSnapshot();
  return { ok: true };
}

function handleLaunchContextPut(message) {
  const context = message.context && typeof message.context === 'object' ? message.context : null;
  const launchId = String(context?.launchId || '').trim();
  if (!launchId) return { ok: false, error: 'missing launchId' };
  launchContexts.set(launchId, safeClone(context));
  cleanupLaunchContexts();
  broadcastSnapshot();
  return { ok: true };
}

function handleLaunchContextGet(message) {
  cleanupLaunchContexts();
  const launchId = String(message.launchId || '').trim();
  if (!launchId) return { ok: false, error: 'missing launchId' };
  return { ok: true, result: launchContexts.get(launchId) || null };
}

function handleProjectionPut(message) {
  const category = String(message.category || '').trim().toLowerCase();
  const key = String(message.key || '').trim();
  const store = projectionCategoryMap(category);
  if (!store) return { ok: false, error: 'unsupported projection category' };
  if (!key) return { ok: false, error: 'missing projection key' };
  store.set(key, safeClone(message.value));
  broadcastSnapshot();
  return { ok: true };
}

function handleProjectionGet(message) {
  const category = String(message.category || '').trim().toLowerCase();
  const key = String(message.key || '').trim();
  const store = projectionCategoryMap(category);
  if (!store) return { ok: false, error: 'unsupported projection category' };
  if (!key) return { ok: false, error: 'missing projection key' };
  return { ok: true, result: safeClone(store.get(key) || null) };
}

function forwardBrokerRequest(kind, message, endpoint) {
  const requestId = String(message.requestId || '').trim();
  const requesterId = String(endpoint?.clientId || '').trim();
  if (!requestId) return { ok: false, error: 'missing requestId' };
  if (!brokerClientId) return { ok: false, error: 'runtime broker unavailable' };
  const broker = endpoints.get(brokerClientId);
  if (!broker) return { ok: false, error: 'runtime broker missing' };
  pendingBrokerRequests.set(requestId, {
    requesterId,
    requesterEndpoint: endpoint || null,
    kind,
    createdAt: nowMs(),
  });
  endpointPost(broker, {
    type: 'runtime.broker.request',
    requestId,
    kind,
    payload: safeClone(message.payload || {}),
    sourceClientId: requesterId,
  });
  return { ok: true };
}

function handleBrokerResponse(kind, message) {
  const requestId = String(message.requestId || '').trim();
  if (!requestId) return { ok: false, error: 'missing requestId' };
  const pending = pendingBrokerRequests.get(requestId);
  if (!pending) return { ok: false, error: 'request not pending' };
  pendingBrokerRequests.delete(requestId);
  const response = {
    type: 'runtime.response',
    requestId,
    kind,
    ok: message.ok !== false,
    result: safeClone(message.result),
    error: String(message.error || '').trim(),
  };
  const endpoint = pending.requesterEndpoint || endpoints.get(pending.requesterId);
  if (endpoint) {
    endpointPost(endpoint, response);
    return { ok: true };
  }
  let delivered = false;
  for (const candidate of endpoints.values()) {
    if (candidate.clientId === brokerClientId) continue;
    endpointPost(candidate, response);
    delivered = true;
  }
  if (delivered) return { ok: true, warning: 'broadcast fallback' };
  return { ok: false, error: 'requester not attached' };
}

function handleControlMessage(message, endpoint) {
  const type = String(message?.type || '').trim();
  if (!type) return;

  let response = null;

  switch (type) {
    case 'runtime.attach': {
      const clientId = String(message.clientId || '').trim();
      const entry = ensureEndpoint(clientId, endpoint, {
        surface: message.surface,
        broker: message.broker === true,
      });
      response = {
        type: 'runtime.attached',
        buildId: RUNTIME_WORKER_BUILD_ID,
        clientId: entry?.clientId || '',
        brokerClientId,
        snapshot: runtimeSnapshot(),
      };
      break;
    }
    case 'runtime.detach': {
      deleteEndpoint(message.clientId);
      response = {
        type: 'runtime.detached',
        buildId: RUNTIME_WORKER_BUILD_ID,
        brokerClientId,
      };
      break;
    }
    case 'status.update': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'status.update',
        ...handleStatusUpdate(message, endpoint),
      };
      break;
    }
    case 'status.snapshot': {
      response = {
        type: 'status.snapshot',
        snapshot: runtimeSnapshot(),
      };
      break;
    }
    case 'launchContext.put': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'launchContext.put',
        ...handleLaunchContextPut(message),
      };
      break;
    }
    case 'launchContext.get': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'launchContext.get',
        ...handleLaunchContextGet(message),
      };
      break;
    }
    case 'projection.put': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'projection.put',
        ...handleProjectionPut(message),
      };
      break;
    }
    case 'projection.get': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'projection.get',
        ...handleProjectionGet(message),
      };
      break;
    }
    case 'gateway.signal.request': {
      const result = forwardBrokerRequest('gateway.signal.request', message, endpoint);
      if (!result.ok) {
        response = {
          type: 'runtime.response',
          requestId: String(message.requestId || '').trim(),
          kind: 'gateway.signal.request',
          ...result,
        };
      }
      break;
    }
    case 'gateway.launch.request': {
      const result = forwardBrokerRequest('gateway.launch.request', message, endpoint);
      if (!result.ok) {
        response = {
          type: 'runtime.response',
          requestId: String(message.requestId || '').trim(),
          kind: 'gateway.launch.request',
          ...result,
        };
      }
      break;
    }
    case 'gateway.signal.response': {
      response = {
        type: 'runtime.ack',
        kind: 'gateway.signal.response',
        ...handleBrokerResponse('gateway.signal.request', message),
      };
      break;
    }
    case 'gateway.launch.response': {
      response = {
        type: 'runtime.ack',
        kind: 'gateway.launch.response',
        ...handleBrokerResponse('gateway.launch.request', message),
      };
      break;
    }
    default:
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: type,
        ok: false,
        error: `unsupported runtime message: ${type}`,
      };
      break;
  }

  if (response) endpointPost(endpoint, response);
}

function attachSharedPort(port) {
  const endpoint = { kind: 'shared', port, clientId: '' };
  port.onmessage = (event) => handleControlMessage(event.data || {}, endpoint);
  port.start();
  endpointPost(endpoint, {
    type: 'runtime.attached',
    buildId: RUNTIME_WORKER_BUILD_ID,
    clientId: '',
    brokerClientId,
    snapshot: runtimeSnapshot(),
  });
}

function ensureDedicatedEndpoint() {
  if (dedicatedEndpoint) return dedicatedEndpoint;
  dedicatedEndpoint = { kind: 'dedicated', port: null, clientId: 'dedicated' };
  endpoints.set(dedicatedEndpoint.clientId, dedicatedEndpoint);
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
