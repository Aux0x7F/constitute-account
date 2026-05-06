import { BROKER, PROJECTION, assertProjectionPolicy, assertProjectionRecord } from "constitute-protocol";

const RUNTIME_VERSION = Object.freeze({ major: 2, minor: 12 });
const RUNTIME_WORKER_BUILD_ID = `runtime-${RUNTIME_VERSION.major}.${RUNTIME_VERSION.minor}`;
const CONTEXT_TTL_FALLBACK_MS = 2 * 60 * 1000;
const APPLIANCE_DISCOVERY_MAX_AGE_MS = 60 * 60 * 1000;
const RUNTIME_STATE_KEY = `runtime.shared.state.v${RUNTIME_VERSION.major}`;
const RUNTIME_META_KEY = `runtime.shared.meta.v${RUNTIME_VERSION.major}`;
const PROJECTION_POLICY_PUT = 'projection.policy.put';
const PROJECTION_SYNC_REQUEST_TIMEOUT_MS = 45_000;
const PROJECTION_SYNC_RETRY_MS = 5_000;
const DEFAULT_LOGGING_SYNC_TARGET_COUNT = 2_500;
const DEFAULT_LOGGING_POLICY_ID = 'logging.default.72h.low';
const LOGGING_SYNC_CHANNELS = Object.freeze([
  PROJECTION.CHANNEL.LOGGING_EVENTS,
  PROJECTION.CHANNEL.LOGGING_HEALTH,
]);

const DB_NAME = 'constitute_db';
const DB_VER = 1;
const IDB_OPERATION_TIMEOUT_MS = 1500;
let runtimeHydrationWarningLogged = false;

function withTimeout(label, work, timeoutMs = IDB_OPERATION_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const timer = self.setTimeout(() => {
      reject(new Error(`${label}: timed out`));
    }, timeoutMs);
    Promise.resolve()
      .then(work)
      .then((value) => {
        self.clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        self.clearTimeout(timer);
        reject(error);
      });
  });
}

function openDB() {
  return withTimeout('indexedDB.open', () => new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains('kv')) db.createObjectStore('kv');
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
    req.onblocked = () => reject(new Error('indexedDB.open blocked'));
  }));
}

async function withDbRetry(label, work, attempts = 2) {
  let lastError = null;
  for (let index = 0; index < attempts; index += 1) {
    try {
      const db = await openDB();
      return await work(db);
    } catch (error) {
      lastError = error;
      if (index < attempts - 1) {
        await new Promise((resolve) => setTimeout(resolve, 30 * (index + 1)));
      }
    }
  }
  throw new Error(`${label}: ${String(lastError?.message || lastError || 'database failure')}`);
}

async function kvGet(key) {
  return await withDbRetry(`kvGet(${String(key || '')})`, async (db) => await new Promise((resolve, reject) => {
    const tx = db.transaction('kv', 'readonly');
    const st = tx.objectStore('kv');
    const req = st.get(key);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  }));
}

async function kvSet(key, value) {
  await withDbRetry(`kvSet(${String(key || '')})`, async (db) => await new Promise((resolve, reject) => {
    const tx = db.transaction('kv', 'readwrite');
    tx.objectStore('kv').put(value, key);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  }));
}

async function kvDelete(key) {
  await withDbRetry(`kvDelete(${String(key || '')})`, async (db) => await new Promise((resolve, reject) => {
    const tx = db.transaction('kv', 'readwrite');
    tx.objectStore('kv').delete(key);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  }));
}

function normalizeRole(value) {
  return String(value || '').trim().toLowerCase();
}

function isGatewayRecord(rec) {
  const role = normalizeRole(rec?.role || rec?.type || '');
  const service = normalizeRole(rec?.service || '');
  return role === 'gateway' || service === 'gateway';
}

function isNvrRecord(rec) {
  const role = normalizeRole(rec?.role || rec?.type || '');
  const service = normalizeRole(rec?.service || '');
  return role === 'nvr' || service === 'nvr';
}

function isManagedServiceRecord(rec) {
  if (!rec || typeof rec !== 'object') return false;
  if (isGatewayRecord(rec)) return false;
  const deviceKind = normalizeRole(rec?.deviceKind || rec?.device_kind || '');
  const service = normalizeRole(rec?.service || '');
  const hostGatewayPk = String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim();
  const servicePk = String(rec?.servicePk || rec?.service_pk || '').trim();
  return Boolean(service && service !== 'none' && (deviceKind === 'service' || hostGatewayPk || servicePk));
}

function ownedPkSet(identityDevices) {
  return new Set(
    (Array.isArray(identityDevices) ? identityDevices : [])
      .map((d) => String(d?.pk || d?.devicePk || '').trim())
      .filter(Boolean),
  );
}

const endpoints = new Map();
const pendingBrokerRequests = new Map();
const projectionPolicies = new Map();
const pendingProjectionSyncRequests = new Map();
const serviceAccessContexts = new Map();
const retainedProjections = new Map();
const runtimeStatus = {
  shell: null,
  services: {},
};
const managedState = {
  sourceSnapshot: {
    identityDevices: [],
    swarmDevices: [],
    grantedRecords: [],
    managedServiceIssue: null,
  },
  applianceSnapshot: {
    owned: [],
    granted: [],
    discoverable: [],
  },
  resourceNames: {},
  managedServiceIssue: null,
  hostedGatewaySnapshots: {},
};

let brokerClientId = '';
let runtimeUpdatedAt = 0;
let hydratePromise = null;
let persistTimer = 0;
let projectionSyncTimer = 0;

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

function stableJson(value) {
  try {
    return JSON.stringify(value ?? null);
  } catch {
    return '';
  }
}

function withoutUpdatedAt(value) {
  const out = value && typeof value === 'object' ? safeClone(value) : {};
  delete out.updatedAt;
  return out;
}

function isLocalhostRuntime() {
  try {
    const host = String(self.location?.hostname || '').trim().toLowerCase();
    return host === 'localhost' || host === '127.0.0.1';
  } catch {
    return false;
  }
}

function normalizeArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeObject(value) {
  return (value && typeof value === 'object') ? value : {};
}

function touchRuntime() {
  runtimeUpdatedAt = nowMs();
}

function managedServiceAccessContextObject() {
  return Object.fromEntries(Array.from(serviceAccessContexts.entries()).map(([contextId, context]) => [contextId, safeClone(context)]));
}

function retainedProjectionObject() {
  const out = {};
  for (const [key, projection] of retainedProjections.entries()) {
    const cloned = safeClone(projection);
    out[key] = cloned;
    const channelId = String(projection?.channelId || '').trim();
    if (channelId && !out[channelId]) out[channelId] = cloned;
    const servicePk = String(projection?.servicePk || projection?.service_pk || '').trim();
    const service = String(projection?.service || '').trim();
    if ((servicePk || service) && channelId) {
      const serviceChannelKey = [servicePk || service, channelId].join('|');
      if (!out[serviceChannelKey]) out[serviceChannelKey] = cloned;
    }
  }
  return out;
}

function retainedProjectionStoreObject() {
  return Object.fromEntries(Array.from(retainedProjections.entries()).map(([key, projection]) => [key, safeClone(projection)]));
}

function retainedProjectionCoverageObject() {
  return Object.fromEntries(Array.from(retainedProjections.entries()).map(([key, projection]) => [key, projectionCoverage(projection)]));
}

function projectionPolicyObject() {
  return Object.fromEntries(Array.from(projectionPolicies.entries()).map(([key, policy]) => [key, safeClone(policy)]));
}

function projectionStoreKey(projection) {
  const servicePk = String(projection?.servicePk || projection?.service_pk || '').trim();
  const service = String(projection?.service || '').trim();
  const channelId = String(projection?.channelId || '').trim();
  const policyId = projectionPolicyId(projection);
  return [servicePk || service, channelId, policyId].filter(Boolean).join('|');
}

function projectionPolicyId(projection) {
  const policy = normalizeObject(projection?.payload?.policy || projection?.scope);
  return String(projection?.policyId || policy.policyId || 'default').trim();
}

function projectionPayloadCount(projection) {
  const payload = projection?.payload;
  if (Array.isArray(payload)) return payload.length;
  if (Array.isArray(payload?.events)) return payload.events.length;
  if (Array.isArray(payload?.items)) return payload.items.length;
  return 0;
}

function projectionReplacesEventSet(projection) {
  return Array.isArray(projection?.payload?.events)
    && Boolean(projection?.payload?.policy || projection?.scope || projection?.payload?.coverage);
}

function projectionEventArray(projection) {
  const events = projection?.payload?.events;
  return Array.isArray(events) ? events : [];
}

function projectionEventKey(event) {
  const direct = String(event?.eventId || event?.event_id || event?.logEventId || event?.id || '').trim();
  if (direct) return `id:${direct}`;
  const cursor = String(event?.cursor?.value || event?.cursor || '').trim();
  if (cursor) return `cursor:${cursor}`;
  return `shape:${stableJson({
    occurredAt: event?.occurredAt || event?.occurred_at || event?.ts || '',
    severity: event?.severity || '',
    category: event?.category || '',
    outcome: event?.outcome || '',
    producer: event?.producer || '',
    subject: event?.subject || null,
    resource: event?.resource || null,
    correlation: event?.correlation || null,
    tags: event?.tags || [],
    safeFacts: event?.safeFacts || event?.safe_facts || {},
  })}`;
}

function projectionEventTimeSeconds(event) {
  const raw = Number(event?.occurredAt || event?.occurred_at || event?.ts || event?.timestamp || 0);
  if (Number.isFinite(raw) && raw > 0) return raw > 9_999_999_999 ? Math.floor(raw / 1000) : raw;
  const parsed = Date.parse(String(event?.occurredAt || event?.occurred_at || ''));
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed / 1000) : 0;
}

function mergeProjectionEvents(existingEvents, nextEvents) {
  const byKey = new Map();
  for (const event of Array.isArray(existingEvents) ? existingEvents : []) {
    const key = projectionEventKey(event);
    if (key) byKey.set(key, event);
  }
  for (const event of Array.isArray(nextEvents) ? nextEvents : []) {
    const key = projectionEventKey(event);
    if (!key) continue;
    const existing = byKey.get(key);
    byKey.set(key, existing && typeof existing === 'object'
      ? { ...existing, ...event }
      : event);
  }
  return Array.from(byKey.values()).sort((left, right) => projectionEventTimeSeconds(right) - projectionEventTimeSeconds(left));
}

function mergeProjectionRecord(existing, next) {
  if (!existing || typeof existing !== 'object') return next;
  if (!next || typeof next !== 'object') return existing;
  const existingChannel = String(existing?.channelId || '').trim();
  const nextChannel = String(next?.channelId || '').trim();
  const sameChannel = existingChannel && existingChannel === nextChannel;
  const existingServicePk = String(existing?.servicePk || existing?.service_pk || '').trim();
  const nextServicePk = String(next?.servicePk || next?.service_pk || '').trim();
  const existingService = String(existing?.service || '').trim();
  const nextService = String(next?.service || '').trim();
  const sameService = (existingServicePk || existingService) === (nextServicePk || nextService);
  if (!sameChannel || !sameService) return next;
  const merged = {
    ...existing,
    ...next,
    payload: {
      ...(existing?.payload && typeof existing.payload === 'object' ? existing.payload : {}),
      ...(next?.payload && typeof next.payload === 'object' ? next.payload : {}),
    },
    safeFacts: {
      ...(existing?.safeFacts && typeof existing.safeFacts === 'object' ? existing.safeFacts : {}),
      ...(next?.safeFacts && typeof next.safeFacts === 'object' ? next.safeFacts : {}),
    },
  };
  const nextEvents = projectionEventArray(next);
  const replacesEventSet = projectionReplacesEventSet(next);
  const mergedEvents = replacesEventSet
    ? nextEvents.slice().sort((left, right) => projectionEventTimeSeconds(right) - projectionEventTimeSeconds(left))
    : mergeProjectionEvents(projectionEventArray(existing), nextEvents);
  if (mergedEvents.length) {
    const existingCoverage = existing?.payload?.coverage && typeof existing.payload.coverage === 'object' ? existing.payload.coverage : {};
    const nextCoverage = next?.payload?.coverage && typeof next.payload.coverage === 'object' ? next.payload.coverage : {};
    const targetCount = Math.max(
      replacesEventSet ? 0 : Number(existingCoverage.targetCount || 0),
      Number(nextCoverage.targetCount || 0),
      mergedEvents.length,
    );
    const completionRatio = targetCount > 0 ? Math.min(1, mergedEvents.length / targetCount) : 1;
    merged.payload = {
      ...merged.payload,
      events: mergedEvents,
      coverage: {
        ...existingCoverage,
        ...nextCoverage,
        materializedCount: mergedEvents.length,
        targetCount,
        completionRatio,
        syncState: completionRatio >= 1 ? 'completeEnough' : String(nextCoverage.syncState || existingCoverage.syncState || 'syncing'),
      },
    };
  }
  return merged;
}

function projectionSemanticShape(projection) {
  const clone = projection && typeof projection === 'object' ? safeClone(projection) : {};
  delete clone.retainedAt;
  delete clone.requestId;
  if (clone.cursor && typeof clone.cursor === 'object') delete clone.cursor.updatedAt;
  if (clone.freshness && typeof clone.freshness === 'object') {
    delete clone.freshness.updatedAt;
    delete clone.freshness.staleAfter;
  }
  return clone;
}

function projectionSemanticallyEqual(left, right) {
  return stableJson(projectionSemanticShape(left)) === stableJson(projectionSemanticShape(right));
}

function projectionCoverage(projection) {
  const coverage = projection?.payload?.coverage && typeof projection.payload.coverage === 'object'
    ? projection.payload.coverage
    : {};
  const materializedCount = projectionPayloadCount(projection);
  const targetCount = Math.max(Number(coverage.targetCount || 0), materializedCount);
  const completionRatio = targetCount > 0
    ? Math.min(1, materializedCount / targetCount)
    : 1;
  return {
    ...safeClone(coverage),
    materializedCount,
    targetCount,
    completionRatio,
    syncState: String(coverage.syncState || (completionRatio >= 1 ? 'completeEnough' : 'syncing')),
  };
}

function projectionFreshness(projection) {
  const freshness = projection?.freshness && typeof projection.freshness === 'object' ? projection.freshness : {};
  return {
    state: String(freshness.state || 'fresh'),
    updatedAt: Number(freshness.updatedAt || projection?.retainedAt || nowMs()),
    ...(freshness.staleAfter ? { staleAfter: Number(freshness.staleAfter) } : {}),
    ...(freshness.reason ? { reason: String(freshness.reason) } : {}),
  };
}

function projectionObserverUpdate(projection, changedCount) {
  return {
    projectionKey: projectionStoreKey(projection),
    changedCount: Math.max(0, Number(changedCount || 0)),
    coverage: projectionCoverage(projection),
    freshness: projectionFreshness(projection),
  };
}

function randomOpaqueId(prefix) {
  try {
    const bytes = new Uint8Array(12);
    self.crypto.getRandomValues(bytes);
    const token = Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
    return `${prefix}-${token}`;
  } catch {
    return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  }
}

function projectionPolicyStoreKey(policy) {
  return [
    String(policy?.service || '').trim(),
    String(policy?.channelId || '').trim(),
    String(policy?.policyId || '').trim(),
  ].filter(Boolean).join('|');
}

function projectionPolicyForChannel(policy, channelId) {
  const targetChannelId = String(channelId || policy?.channelId || '').trim();
  const basePolicyId = String(policy?.policyId || 'default').trim() || 'default';
  return {
    ...safeClone(policy),
    service: String(policy?.service || 'logging').trim() || 'logging',
    channelId: targetChannelId,
    policyId: targetChannelId === String(policy?.channelId || '').trim()
      ? basePolicyId
      : `${basePolicyId}.${targetChannelId.replace(/[^a-z0-9]+/gi, '.')}`,
  };
}

function normalizeProjectionPolicyForRuntime(value) {
  const raw = value && typeof value === 'object' ? value : {};
  const policy = {
    ...safeClone(raw),
    service: String(raw.service || 'logging').trim() || 'logging',
    channelId: String(raw.channelId || PROJECTION.CHANNEL.LOGGING_EVENTS).trim(),
    policyId: String(raw.policyId || DEFAULT_LOGGING_POLICY_ID).trim() || DEFAULT_LOGGING_POLICY_ID,
    scope: raw.scope && typeof raw.scope === 'object' && !Array.isArray(raw.scope) ? safeClone(raw.scope) : {},
    syncDepthTarget: raw.syncDepthTarget && typeof raw.syncDepthTarget === 'object' && !Array.isArray(raw.syncDepthTarget)
      ? safeClone(raw.syncDepthTarget)
      : { mode: 'policyComplete', targetCount: DEFAULT_LOGGING_SYNC_TARGET_COUNT },
    retentionTarget: raw.retentionTarget && typeof raw.retentionTarget === 'object' && !Array.isArray(raw.retentionTarget)
      ? safeClone(raw.retentionTarget)
      : {},
  };
  assertProjectionPolicy(policy);
  return policy;
}

function syncTargetCountForPolicy(policy) {
  const target = Number(policy?.syncDepthTarget?.targetCount || DEFAULT_LOGGING_SYNC_TARGET_COUNT);
  return Number.isFinite(target) && target > 0 ? Math.min(target, 5_000) : DEFAULT_LOGGING_SYNC_TARGET_COUNT;
}

function projectionForPolicyChannel(policy, channelId) {
  const channelPolicy = projectionPolicyForChannel(policy, channelId);
  const service = String(channelPolicy.service || '').trim();
  const policyId = String(channelPolicy.policyId || '').trim();
  const directKey = [service, String(channelPolicy.channelId || '').trim(), policyId].filter(Boolean).join('|');
  if (retainedProjections.has(directKey)) return retainedProjections.get(directKey);
  let fallback = null;
  for (const projection of retainedProjections.values()) {
    if (String(projection?.service || '').trim() !== service) continue;
    if (String(projection?.channelId || '').trim() !== String(channelPolicy.channelId || '').trim()) continue;
    if (projectionPolicyId(projection) === policyId) return projection;
    if (!fallback || Number(projection?.retainedAt || 0) > Number(fallback?.retainedAt || 0)) {
      fallback = projection;
    }
  }
  return fallback;
}

function projectionNeedsRuntimeSync(projection, policy, channelId) {
  if (!projection) return true;
  const freshness = projectionFreshness(projection);
  const staleAfter = Number(freshness.staleAfter || 0);
  const staleAfterMs = staleAfter > 0 && staleAfter < 9_999_999_999 ? staleAfter * 1000 : staleAfter;
  if (freshness.state === 'missing' || freshness.state === 'error') return true;
  if (staleAfterMs && staleAfterMs < nowMs()) return true;
  if (String(channelId || '') !== PROJECTION.CHANNEL.LOGGING_EVENTS) return false;
  const coverage = projectionCoverage(projection);
  const targetCount = syncTargetCountForPolicy(policy);
  return coverage.materializedCount < Math.min(targetCount, Number(coverage.targetCount || targetCount));
}

function syncChannelsForPolicy(policy) {
  const service = String(policy?.service || '').trim();
  if (service === 'logging') return LOGGING_SYNC_CHANNELS;
  return [String(policy?.channelId || '').trim()].filter(Boolean);
}

function broadcastProjectionSyncDiagnostic(operation, detail = {}) {
  broadcast({
    type: 'projection.sync.diagnostic',
    operation,
    detail: safeClone(detail),
  });
}

function scheduleProjectionSync(delayMs = 0) {
  if (projectionSyncTimer) return;
  projectionSyncTimer = self.setTimeout(() => {
    projectionSyncTimer = 0;
    startProjectionSync();
  }, Math.max(0, Number(delayMs || 0)));
}

function startProjectionSync() {
  const broker = brokerClientId ? endpoints.get(brokerClientId) : null;
  if (!broker) {
    if (projectionPolicies.size) {
      broadcastProjectionSyncDiagnostic('projection.sync.waiting_for_broker', {
        policyCount: projectionPolicies.size,
      });
      scheduleProjectionSync(PROJECTION_SYNC_RETRY_MS);
    }
    return;
  }
  for (const policy of projectionPolicies.values()) {
    for (const channelId of syncChannelsForPolicy(policy)) {
      queueProjectionSyncRequest(policy, channelId, broker);
    }
  }
}

function queueProjectionSyncRequest(policy, channelId, broker) {
  const channelPolicy = projectionPolicyForChannel(policy, channelId);
  const pendingKey = projectionPolicyStoreKey(channelPolicy);
  for (const pending of pendingProjectionSyncRequests.values()) {
    if (pending.pendingKey === pendingKey) return;
  }
  const existing = projectionForPolicyChannel(policy, channelId);
  if (!projectionNeedsRuntimeSync(existing, channelPolicy, channelId)) return;
  const requestId = randomOpaqueId('projection');
  const limit = channelId === PROJECTION.CHANNEL.LOGGING_EVENTS
    ? syncTargetCountForPolicy(policy)
    : undefined;
  const payload = {
    requestId,
    channelId,
    service: String(channelPolicy.service || '').trim(),
    ...(limit ? { limit } : {}),
    filters: {},
    policy: channelPolicy,
  };
  const timer = self.setTimeout(() => {
    pendingProjectionSyncRequests.delete(requestId);
    broadcastProjectionSyncDiagnostic('projection.sync.degraded', {
      channelId,
      requestId,
      error: 'projection request timed out',
    });
    scheduleProjectionSync(PROJECTION_SYNC_RETRY_MS);
  }, PROJECTION_SYNC_REQUEST_TIMEOUT_MS);
  pendingProjectionSyncRequests.set(requestId, {
    pendingKey,
    channelId,
    policyId: channelPolicy.policyId,
    service: channelPolicy.service,
    createdAt: nowMs(),
    timer,
  });
  broadcastProjectionSyncDiagnostic('projection.sync.request.sent', {
    channelId,
    requestId,
    limit: limit || 0,
  });
  endpointPost(broker, {
    type: 'runtime.broker.request',
    requestId,
    kind: BROKER.SERVICE_PROJECTION_REQUEST,
    payload,
    sourceClientId: 'runtime-projection-sync',
  });
}

function normalizeProjectionRecord(value) {
  const source = value?.projection && typeof value.projection === 'object' ? value.projection : value;
  if (!source || typeof source !== 'object') throw new Error('projection result missing');
  const projection = {
    ...safeClone(source),
    channelId: String(source.channelId || '').trim(),
    service: String(source.service || '').trim(),
    servicePk: String(source.servicePk || source.service_pk || '').trim(),
  };
  assertProjectionRecord(projection);
  return projection;
}

function storeProjectionRecord(value) {
  const projection = normalizeProjectionRecord(value);
  const key = projectionStoreKey(projection);
  const existing = retainedProjections.get(key);
  const existingCount = projectionPayloadCount(existing);
  const mergedProjection = mergeProjectionRecord(retainedProjections.get(key), projection);
  const storedProjection = {
    ...mergedProjection,
    retainedAt: nowMs(),
  };
  if (existing && projectionSemanticallyEqual(existing, storedProjection)) {
    const refreshedProjection = {
      ...existing,
      retainedAt: storedProjection.retainedAt,
      cursor: storedProjection.cursor || existing.cursor,
      freshness: storedProjection.freshness || existing.freshness,
    };
    retainedProjections.set(key, refreshedProjection);
    schedulePersist();
    return safeClone(refreshedProjection);
  }
  retainedProjections.set(key, storedProjection);
  const stored = retainedProjections.get(key);
  const changedCount = Math.max(0, projectionPayloadCount(stored) - existingCount);
  touchRuntime();
  schedulePersist();
  broadcast({
    type: 'projection.observer.update',
    update: projectionObserverUpdate(stored, changedCount),
    projection: safeClone(stored),
  });
  broadcastSnapshot();
  return safeClone(stored);
}

function normalizeHostedServices(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => (entry && typeof entry === 'object') ? safeClone(entry) : null)
    .filter(Boolean);
}

function gatewayHostedSnapshot(record) {
  const pk = String(record?.devicePk || record?.pk || '').trim();
  const updatedAt = Number(record?.updatedAt || record?.updated_at || record?.ts || 0);
  const hostedServices = normalizeHostedServices(record?.hostedServices || record?.hosted_services);
  return { pk, updatedAt, hostedServices };
}

function applyGatewayHostedSnapshot(record) {
  const role = String(record?.role || record?.type || '').trim().toLowerCase();
  const service = String(record?.service || '').trim().toLowerCase();
  const isGateway = role === 'gateway' || service === 'gateway';
  if (!isGateway) return record;

  const current = gatewayHostedSnapshot(record);
  if (!current.pk) return record;

  const cache = normalizeObject(managedState.hostedGatewaySnapshots);
  const cached = cache[current.pk] && typeof cache[current.pk] === 'object' ? cache[current.pk] : null;
  let effectiveHostedServices = current.hostedServices;

  if (effectiveHostedServices.length > 0) {
    cache[current.pk] = {
      updatedAt: current.updatedAt,
      hostedServices: effectiveHostedServices,
    };
    managedState.hostedGatewaySnapshots = cache;
  } else if (cached && Array.isArray(cached.hostedServices) && cached.hostedServices.length > 0) {
    const cachedUpdatedAt = Number(cached.updatedAt || 0);
    if (cachedUpdatedAt >= current.updatedAt) {
      effectiveHostedServices = cached.hostedServices;
    } else if (current.updatedAt >= cachedUpdatedAt) {
      cache[current.pk] = {
        updatedAt: current.updatedAt,
        hostedServices: [],
      };
      managedState.hostedGatewaySnapshots = cache;
    }
  }

  if (effectiveHostedServices === current.hostedServices) return record;
  return {
    ...record,
    hostedServices: effectiveHostedServices,
  };
}

function getSwarmSeenAt(pk) {
  const target = String(pk || '').trim();
  if (!target) return 0;
  for (const record of normalizeArray(managedState.sourceSnapshot?.swarmDevices)) {
    const devicePk = String(record?.devicePk || record?.pk || '').trim();
    if (devicePk !== target) continue;
    return Number(record?.updatedAt || record?.updated_at || record?.ts || record?.lastSeen || 0);
  }
  return 0;
}

function applianceSeenAt(rec) {
  if (String(rec?.managedAvailabilityAuthority || '').trim().toLowerCase() === 'gateway') {
    const authoritativeSeen = Number(
      rec?.managedAvailabilityUpdatedAt
      || rec?.managed_availability_updated_at
      || rec?.updatedAt
      || rec?.updated_at
      || 0,
    );
    return Math.max(0, authoritativeSeen);
  }
  const pk = String(rec?.devicePk || rec?.pk || '').trim();
  const nostrSeen = Number(rec?.updatedAt || rec?.updated_at || rec?.ts || rec?.lastSeen || 0);
  const swarmSeen = pk ? Number(getSwarmSeenAt(pk) || 0) : 0;
  const hostedSeen = Array.isArray(rec?.hostedServices || rec?.hosted_services)
    ? (rec.hostedServices || rec.hosted_services).reduce((latest, hosted) => {
        const hostedUpdatedAt = Number(hosted?.updatedAt || hosted?.updated_at || 0);
        return Math.max(latest, hostedUpdatedAt);
      }, 0)
    : 0;
  return Math.max(0, nostrSeen, swarmSeen, hostedSeen);
}

function effectiveApplianceSeenAt(rec, allRecords = []) {
  const baseSeen = applianceSeenAt(rec);
  if (!isGatewayRecord(rec)) return baseSeen;
  const gatewayPk = String(rec?.devicePk || rec?.pk || '').trim();
  if (!gatewayPk) return baseSeen;
  let hostedSeen = 0;
  for (const candidate of Array.isArray(allRecords) ? allRecords : []) {
    if (!isManagedServiceRecord(candidate)) continue;
    const hostGatewayPk = String(candidate?.hostGatewayPk || candidate?.host_gateway_pk || '').trim();
    if (!hostGatewayPk || hostGatewayPk !== gatewayPk) continue;
    hostedSeen = Math.max(hostedSeen, applianceSeenAt(candidate));
  }
  return Math.max(baseSeen, hostedSeen);
}

function mergeGatewayHostedServiceRecord(actualRecord, hostedRecord, gatewayRecord) {
  const actual = (actualRecord && typeof actualRecord === 'object') ? actualRecord : {};
  const hosted = (hostedRecord && typeof hostedRecord === 'object') ? hostedRecord : {};
  const gateway = (gatewayRecord && typeof gatewayRecord === 'object') ? gatewayRecord : {};
  const gatewayPk = String(
    hosted.hostGatewayPk
    || hosted.host_gateway_pk
    || gateway.devicePk
    || gateway.pk
    || actual.hostGatewayPk
    || actual.host_gateway_pk
    || '',
  ).trim();
  const hostedUpdatedAt = Number(hosted.updatedAt || hosted.updated_at || gateway.updatedAt || gateway.updated_at || 0);
  return {
    ...actual,
    devicePk: String(hosted.devicePk || hosted.device_pk || actual.devicePk || actual.pk || '').trim(),
    servicePk: String(hosted.servicePk || hosted.service_pk || actual.servicePk || actual.service_pk || '').trim(),
    deviceLabel: String(hosted.deviceLabel || hosted.device_label || actual.deviceLabel || actual.label || hosted.service || 'service').trim(),
    deviceKind: String(hosted.deviceKind || hosted.device_kind || actual.deviceKind || actual.device_kind || 'service').trim() || 'service',
    role: String(hosted.service || actual.role || actual.type || '').trim(),
    service: String(hosted.service || actual.service || '').trim(),
    hostGatewayPk: gatewayPk,
    serviceVersion: String(hosted.serviceVersion || hosted.service_version || actual.serviceVersion || actual.service_version || '').trim(),
    updatedAt: hostedUpdatedAt,
    freshnessMs: Number(hosted.freshnessMs || hosted.freshness_ms || actual.freshnessMs || actual.freshness_ms || 0),
    status: String(hosted.status || actual.status || '').trim(),
    cameraCount: Number(hosted.cameraCount || hosted.camera_count || actual.cameraCount || actual.camera_count || 0),
    managedAvailabilityAuthority: 'gateway',
    managedAvailabilityUpdatedAt: hostedUpdatedAt,
    managedAvailabilityGatewayPk: gatewayPk,
    directServiceUpdatedAt: Number(actual.updatedAt || actual.updated_at || actual.ts || actual.lastSeen || 0),
    hostedSynthetic: Boolean(actual.hostedSynthetic),
  };
}

function buildApplianceRecords(identityDevices, swarmDevices, grantedRecords = []) {
  const owned = ownedPkSet(identityDevices);
  const actual = [];
  const seen = new Set();
  const sourceRecords = Array.isArray(swarmDevices) ? swarmDevices : [];
  for (const rawRec of sourceRecords) {
    const rec = applyGatewayHostedSnapshot(rawRec);
    const pk = String(rec?.devicePk || rec?.pk || '').trim();
    if (!pk || seen.has(pk)) continue;
    if (!(isGatewayRecord(rec) || isManagedServiceRecord(rec))) continue;
    const ownedRec = owned.has(pk) || owned.has(String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim());
    const seenAt = applianceSeenAt(rec);
    const ageMs = seenAt ? Math.max(0, Date.now() - seenAt) : Number.POSITIVE_INFINITY;
    if (!ownedRec && ageMs > APPLIANCE_DISCOVERY_MAX_AGE_MS) continue;
    seen.add(pk);
    actual.push(rec);
  }

  const actualByPk = new Map(actual.map((rec) => [String(rec?.devicePk || rec?.pk || '').trim(), rec]).filter(([pk]) => pk));
  const recs = [...actual];
  const actualPkSet = new Set(actualByPk.keys());
  for (const rec of actual) {
    if (!isGatewayRecord(rec)) continue;
    const hostedServices = Array.isArray(rec?.hostedServices || rec?.hosted_services)
      ? (rec.hostedServices || rec.hosted_services)
      : [];
    for (const hosted of hostedServices) {
      const pk = String(hosted?.devicePk || hosted?.device_pk || '').trim();
      if (!pk) continue;
      const merged = mergeGatewayHostedServiceRecord(actualByPk.get(pk), hosted, rec);
      if (actualByPk.has(pk)) {
        const idx = recs.findIndex((candidate) => String(candidate?.devicePk || candidate?.pk || '').trim() === pk);
        if (idx >= 0) recs[idx] = merged;
        actualByPk.set(pk, merged);
        continue;
      }
      recs.push({
        ...merged,
        hostedSynthetic: true,
      });
      actualByPk.set(pk, merged);
      actualPkSet.add(pk);
    }
  }

  recs.sort((a, b) => Number(effectiveApplianceSeenAt(b, recs) || 0) - Number(effectiveApplianceSeenAt(a, recs) || 0));
  for (const granted of Array.isArray(grantedRecords) ? grantedRecords : []) {
    const pk = String(granted?.devicePk || granted?.pk || '').trim();
    if (!pk || actualPkSet.has(pk)) continue;
    recs.push({
      ...granted,
      grantedRecord: true,
    });
    actualPkSet.add(pk);
  }
  recs.sort((a, b) => Number(effectiveApplianceSeenAt(b, recs) || 0) - Number(effectiveApplianceSeenAt(a, recs) || 0));
  return recs;
}

function partitionApplianceRecords(recs, owned, isGrantedRecord) {
  const ownedRecords = [];
  const grantedRecords = [];
  const discoverableRecords = [];

  for (const rec of recs) {
    const pk = String(rec?.devicePk || rec?.pk || '').trim();
    const ownedRec = owned.has(pk) || owned.has(String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim());
    if (ownedRec) {
      ownedRecords.push(rec);
      continue;
    }
    if (isGrantedRecord(rec)) {
      grantedRecords.push(rec);
      continue;
    }
    if (isGatewayRecord(rec)) {
      discoverableRecords.push(rec);
    }
  }

  return {
    ownedRecords,
    grantedRecords,
    discoverableRecords,
  };
}

function rememberResourceName(map, pk, label) {
  const key = String(pk || '').trim();
  const text = String(label || '').trim();
  if (!key || !text) return;
  map[key] = text;
}

function buildResourceNames() {
  const names = {};
  for (const identityDevice of normalizeArray(managedState.sourceSnapshot?.identityDevices)) {
    rememberResourceName(
      names,
      identityDevice?.pk || identityDevice?.devicePk || identityDevice?.identityId,
      identityDevice?.label || identityDevice?.deviceLabel || identityDevice?.name,
    );
  }
  for (const bucket of Object.values(managedState.applianceSnapshot)) {
    for (const record of normalizeArray(bucket)) {
      rememberResourceName(
        names,
        record?.devicePk || record?.pk,
        record?.deviceLabel || record?.label || record?.serviceLabel || record?.name,
      );
      rememberResourceName(
        names,
        record?.hostGatewayPk || record?.host_gateway_pk,
        record?.hostGatewayLabel || record?.host_gateway_label,
      );
    }
  }
  return names;
}

function normalizeManagedSourceSnapshot(snapshot) {
  const payload = normalizeObject(snapshot);
  return {
    identityDevices: normalizeArray(payload.identityDevices).map((entry) => safeClone(entry)),
    swarmDevices: normalizeArray(payload.swarmDevices).map((entry) => safeClone(entry)),
    grantedRecords: normalizeArray(payload.grantedRecords).map((entry) => safeClone(entry)),
    managedServiceIssue: payload.managedServiceIssue ? safeClone(payload.managedServiceIssue) : null,
  };
}

function rebuildManagedApplianceSnapshot() {
  const source = managedState.sourceSnapshot;
  const identityDevices = normalizeArray(source.identityDevices);
  const swarmDevices = normalizeArray(source.swarmDevices);
  const grantedRecords = normalizeArray(source.grantedRecords);
  const combined = buildApplianceRecords(identityDevices, swarmDevices, grantedRecords);
  const owned = ownedPkSet(identityDevices);
  const partitions = partitionApplianceRecords(
    combined,
    owned,
    (record) => record?.sharedProjection === true || record?.grantedRecord === true,
  );
  managedState.applianceSnapshot = {
    owned: safeClone(partitions.ownedRecords),
    granted: safeClone(partitions.grantedRecords),
    discoverable: safeClone(partitions.discoverableRecords),
  };
  managedState.resourceNames = buildResourceNames();
  managedState.managedServiceIssue = source.managedServiceIssue ? safeClone(source.managedServiceIssue) : null;
}

function cleanupServiceAccessContexts() {
  const now = nowMs();
  let changed = false;
  for (const [contextId, context] of serviceAccessContexts.entries()) {
    const expiresAt = Number(context?.expiresAt || 0) || (Number(context?.createdAt || 0) + CONTEXT_TTL_FALLBACK_MS);
    if (expiresAt && expiresAt <= now) {
      serviceAccessContexts.delete(contextId);
      changed = true;
    }
  }
  return changed;
}

function runtimeSnapshot() {
  if (cleanupServiceAccessContexts()) {
    touchRuntime();
    schedulePersist();
  }
  const brokerEndpoint = brokerClientId ? endpoints.get(brokerClientId) : null;
  return {
    buildId: RUNTIME_WORKER_BUILD_ID,
    updatedAt: runtimeUpdatedAt || nowMs(),
    broker: {
      available: Boolean(brokerEndpoint),
      surface: String(brokerEndpoint?.surface || '').trim(),
    },
    shell: safeClone(runtimeStatus.shell),
    services: safeClone(runtimeStatus.services),
    managedAppliances: safeClone(managedState.applianceSnapshot),
    resourceNames: safeClone(managedState.resourceNames),
    managedServiceIssue: safeClone(managedState.managedServiceIssue),
    projections: retainedProjectionObject(),
    projectionCoverage: retainedProjectionCoverageObject(),
    projectionPolicies: projectionPolicyObject(),
    serviceAccessContextCount: serviceAccessContexts.size,
  };
}

function endpointPost(endpoint, message) {
  try {
    endpoint?.port?.postMessage(message);
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
    type: 'runtime.snapshot',
    buildId: RUNTIME_WORKER_BUILD_ID,
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

function serializedRuntimeState() {
  return {
    buildId: RUNTIME_WORKER_BUILD_ID,
    updatedAt: runtimeUpdatedAt || nowMs(),
    shell: safeClone(runtimeStatus.shell),
    services: safeClone(runtimeStatus.services),
    managedAppliancesSource: safeClone(managedState.sourceSnapshot),
    managedAppliances: safeClone(managedState.applianceSnapshot),
    resourceNames: safeClone(managedState.resourceNames),
    managedServiceIssue: safeClone(managedState.managedServiceIssue),
    hostedGatewaySnapshots: safeClone(managedState.hostedGatewaySnapshots),
    serviceAccessContexts: managedServiceAccessContextObject(),
    projections: retainedProjectionStoreObject(),
    projectionCoverage: retainedProjectionCoverageObject(),
    projectionPolicies: projectionPolicyObject(),
  };
}

async function clearPersistedState() {
  await Promise.all([
    kvDelete(RUNTIME_STATE_KEY).catch(() => {}),
    kvDelete(RUNTIME_META_KEY).catch(() => {}),
  ]);
}

async function persistRuntimeState() {
  const payload = serializedRuntimeState();
  await Promise.all([
    kvSet(RUNTIME_STATE_KEY, payload),
    kvSet(RUNTIME_META_KEY, {
      buildId: RUNTIME_WORKER_BUILD_ID,
      schemaVersion: RUNTIME_VERSION.major,
      updatedAt: payload.updatedAt,
    }),
  ]);
}

function schedulePersist() {
  if (persistTimer) return;
  persistTimer = self.setTimeout(() => {
    persistTimer = 0;
    void persistRuntimeState().catch(() => {});
  }, 0);
}

async function hydrateRuntimeState() {
  const meta = await kvGet(RUNTIME_META_KEY).catch(() => null);
  const schemaVersion = Number(meta?.schemaVersion || RUNTIME_VERSION.major);
  if (isLocalhostRuntime() && String(meta?.buildId || '').trim() && schemaVersion !== RUNTIME_VERSION.major) {
    await clearPersistedState();
  }
  const persisted = await kvGet(RUNTIME_STATE_KEY).catch(() => null);
  if (persisted && typeof persisted === 'object') {
    const payload = persisted;
    runtimeUpdatedAt = Number(payload.updatedAt || 0);
    runtimeStatus.shell = payload.shell && typeof payload.shell === 'object' ? safeClone(payload.shell) : null;
    runtimeStatus.services = payload.services && typeof payload.services === 'object' ? safeClone(payload.services) : {};
    managedState.sourceSnapshot = normalizeManagedSourceSnapshot(payload.managedAppliancesSource);
    managedState.applianceSnapshot = payload.managedAppliances && typeof payload.managedAppliances === 'object'
      ? {
          owned: normalizeArray(payload.managedAppliances.owned).map((entry) => safeClone(entry)),
          granted: normalizeArray(payload.managedAppliances.granted).map((entry) => safeClone(entry)),
          discoverable: normalizeArray(payload.managedAppliances.discoverable).map((entry) => safeClone(entry)),
        }
      : {
          owned: [],
          granted: [],
          discoverable: [],
        };
    managedState.resourceNames = payload.resourceNames && typeof payload.resourceNames === 'object'
      ? safeClone(payload.resourceNames)
      : {};
    managedState.managedServiceIssue = payload.managedServiceIssue ? safeClone(payload.managedServiceIssue) : null;
    managedState.hostedGatewaySnapshots = payload.hostedGatewaySnapshots && typeof payload.hostedGatewaySnapshots === 'object'
      ? safeClone(payload.hostedGatewaySnapshots)
      : {};
    serviceAccessContexts.clear();
    const persistedContexts = payload.serviceAccessContexts && typeof payload.serviceAccessContexts === 'object'
      ? payload.serviceAccessContexts
      : {};
    for (const [contextId, context] of Object.entries(persistedContexts)) {
      const key = String(contextId || '').trim();
      if (!key || !context || typeof context !== 'object') continue;
      serviceAccessContexts.set(key, safeClone(context));
    }
    retainedProjections.clear();
    const persistedProjections = payload.projections && typeof payload.projections === 'object'
      ? payload.projections
      : {};
    for (const [channelId, projection] of Object.entries(persistedProjections)) {
      try {
        const stored = normalizeProjectionRecord({
          channelId,
          ...(projection && typeof projection === 'object' ? projection : {}),
        });
        const key = projectionStoreKey(stored) || stored.channelId;
        const nextProjection = {
          ...stored,
          retainedAt: Number(projection?.retainedAt || 0) || Number(stored.freshness?.updatedAt || 0) || nowMs(),
        };
        retainedProjections.set(key, mergeProjectionRecord(retainedProjections.get(key), nextProjection));
      } catch {}
    }
    projectionPolicies.clear();
    const persistedPolicies = payload.projectionPolicies && typeof payload.projectionPolicies === 'object'
      ? payload.projectionPolicies
      : {};
    for (const policy of Object.values(persistedPolicies)) {
      try {
        const normalized = normalizeProjectionPolicyForRuntime(policy);
        projectionPolicies.set(projectionPolicyStoreKey(normalized), normalized);
      } catch {}
    }
  }
  rebuildManagedApplianceSnapshot();
  if (cleanupServiceAccessContexts()) {
    touchRuntime();
    schedulePersist();
  }
}

function ensureHydrated() {
  if (!hydratePromise) {
    hydratePromise = hydrateRuntimeState().then(() => {
      broadcastSnapshot();
      return true;
    }).catch((error) => {
      hydratePromise = null;
      throw error;
    });
  }
  return hydratePromise;
}

function startHydrationInBackground() {
  void ensureHydrated().catch((error) => {
    if (runtimeHydrationWarningLogged) return;
    runtimeHydrationWarningLogged = true;
    console.warn('[runtime] hydration failed', String(error?.message || error));
  });
}

function handleStatusPut(message, endpoint) {
  const role = String(message.role || message.surface || endpoint?.surface || '').trim().toLowerCase();
  const payload = message.status && typeof message.status === 'object' ? message.status : {};

  if (role === 'shell') {
    const next = withoutUpdatedAt(payload);
    if (stableJson(withoutUpdatedAt(runtimeStatus.shell)) === stableJson(next)) {
      return { ok: true, result: runtimeSnapshot(), unchanged: true };
    }
    touchRuntime();
    runtimeStatus.shell = {
      ...next,
      updatedAt: runtimeUpdatedAt,
    };
    schedulePersist();
    broadcastSnapshot();
    return { ok: true, result: runtimeSnapshot() };
  }

  const service = String(message.service || payload.service || endpoint?.surface || '').trim().toLowerCase();
  if (!service) {
    return { ok: false, error: 'missing service status target' };
  }
  const next = {
    ...withoutUpdatedAt(payload),
    service,
  };
  if (stableJson(withoutUpdatedAt(runtimeStatus.services[service])) === stableJson(next)) {
    return { ok: true, result: runtimeSnapshot(), unchanged: true };
  }
  touchRuntime();
  runtimeStatus.services[service] = {
    ...next,
    updatedAt: runtimeUpdatedAt,
  };
  schedulePersist();
  broadcastSnapshot();
  return { ok: true, result: runtimeSnapshot() };
}

function handleServiceAccessContextPut(message) {
  const context = message.context && typeof message.context === 'object' ? message.context : null;
  const contextId = String(context?.contextId || '').trim();
  if (!contextId) return { ok: false, error: 'missing contextId' };
  serviceAccessContexts.set(contextId, safeClone(context));
  cleanupServiceAccessContexts();
  touchRuntime();
  schedulePersist();
  broadcastSnapshot();
  return { ok: true, result: safeClone(serviceAccessContexts.get(contextId) || null) };
}

function handleServiceAccessContextGet(message) {
  cleanupServiceAccessContexts();
  const contextId = String(message.contextId || '').trim();
  if (!contextId) return { ok: false, error: 'missing contextId' };
  return { ok: true, result: safeClone(serviceAccessContexts.get(contextId) || null) };
}

function handleServiceAccessContextDelete(message) {
  const contextId = String(message.contextId || '').trim();
  if (!contextId) return { ok: false, error: 'missing contextId' };
  const existed = serviceAccessContexts.delete(contextId);
  if (existed) {
    touchRuntime();
    schedulePersist();
    broadcastSnapshot();
  }
  return { ok: true, result: { deleted: existed } };
}

function handleProjectionPut(message) {
  try {
    const projection = storeProjectionRecord(message.projection || message.result || message.payload);
    return { ok: true, result: projection };
  } catch (error) {
    return { ok: false, error: String(error?.message || error || 'invalid projection') };
  }
}

function handleProjectionGet(message) {
  const channelId = String(message.channelId || message?.payload?.channelId || '').trim();
  const servicePk = String(message.servicePk || message?.payload?.servicePk || '').trim();
  const service = String(message.service || message?.payload?.service || '').trim();
  const policyId = String(message.policyId || message?.payload?.policyId || '').trim();
  if (channelId && (servicePk || service || policyId)) {
    const candidates = [];
    if (servicePk) candidates.push([servicePk, channelId, policyId || 'default'].filter(Boolean).join('|'));
    if (service) candidates.push([service, channelId, policyId || 'default'].filter(Boolean).join('|'));
    for (const candidate of candidates) {
      if (retainedProjections.has(candidate)) {
        return { ok: true, result: safeClone(retainedProjections.get(candidate)) };
      }
    }
  }
  if (channelId) {
    for (const projection of retainedProjections.values()) {
      if (String(projection?.channelId || '').trim() === channelId) {
        return { ok: true, result: safeClone(projection) };
      }
    }
    return { ok: true, result: null };
  }
  return { ok: true, result: retainedProjectionObject() };
}

function handleProjectionPolicyPut(message) {
  try {
    const policy = normalizeProjectionPolicyForRuntime(message.policy || message?.payload?.policy || message.payload);
    const key = projectionPolicyStoreKey(policy);
    if (!key) return { ok: false, error: 'missing projection policy key' };
    const existing = projectionPolicies.get(key);
    projectionPolicies.set(key, policy);
    touchRuntime();
    schedulePersist();
    broadcastSnapshot();
    broadcastProjectionSyncDiagnostic('projection.policy.applied', {
      policyId: policy.policyId,
      service: policy.service,
      channelId: policy.channelId,
    });
    if (stableJson(existing) !== stableJson(policy)) {
      scheduleProjectionSync(0);
    }
    return { ok: true, result: safeClone(policy) };
  } catch (error) {
    return { ok: false, error: String(error?.message || error || 'invalid projection policy') };
  }
}

function handleRuntimeProjectionSyncResponse(message) {
  const requestId = String(message.requestId || '').trim();
  const pending = pendingProjectionSyncRequests.get(requestId);
  if (!pending) return null;
  pendingProjectionSyncRequests.delete(requestId);
  self.clearTimeout(pending.timer);
  if (message.ok === false) {
    broadcastProjectionSyncDiagnostic('projection.sync.degraded', {
      channelId: pending.channelId,
      requestId,
      error: String(message.error || 'projection request failed'),
    });
    scheduleProjectionSync(PROJECTION_SYNC_RETRY_MS);
    return { ok: true };
  }
  const source = message?.result?.projection || message?.result || null;
  try {
    const stored = storeProjectionRecord(source);
    broadcastProjectionSyncDiagnostic('projection.sync.batch.received', {
      channelId: pending.channelId,
      requestId,
      materialized: projectionCoverage(stored).materializedCount,
      targetCount: projectionCoverage(stored).targetCount,
      syncState: projectionCoverage(stored).syncState,
    });
    scheduleProjectionSync(250);
    return { ok: true };
  } catch (error) {
    broadcastProjectionSyncDiagnostic('projection.sync.degraded', {
      channelId: pending.channelId,
      requestId,
      error: String(error?.message || error || 'invalid service projection response'),
    });
    scheduleProjectionSync(PROJECTION_SYNC_RETRY_MS);
    return { ok: false, error: String(error?.message || error || 'invalid service projection response') };
  }
}

function handleServiceProjectionResponse(message) {
  const runtimeSyncResult = handleRuntimeProjectionSyncResponse(message);
  if (runtimeSyncResult) return runtimeSyncResult;
  const source = message?.result?.projection || message?.result || null;
  let stored = null;
  if (message.ok !== false && source && typeof source === 'object') {
    try {
      stored = storeProjectionRecord(source);
    } catch (error) {
      return { ok: false, error: String(error?.message || error || 'invalid service projection response') };
    }
  }
  const responseMessage = stored
    ? {
        ...message,
        result: {
          ...(message.result && typeof message.result === 'object' ? message.result : {}),
          projection: stored,
        },
      }
    : message;
  return handleBrokerResponse(BROKER.SERVICE_PROJECTION_REQUEST, responseMessage);
}

function handleManagedSourceSnapshotPut(message) {
  const next = normalizeManagedSourceSnapshot(message.sourceSnapshot);
  if (stableJson(managedState.sourceSnapshot) === stableJson(next)) {
    return { ok: true, result: runtimeSnapshot(), unchanged: true };
  }
  managedState.sourceSnapshot = next;
  rebuildManagedApplianceSnapshot();
  touchRuntime();
  schedulePersist();
  broadcastSnapshot();
  scheduleProjectionSync(0);
  return { ok: true, result: runtimeSnapshot() };
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
  if (!endpoint) return { ok: false, error: 'requester not attached' };
  endpointPost(endpoint, response);
  return { ok: true };
}

async function handleControlMessage(message, endpoint) {
  const type = String(message?.type || '').trim();
  if (!type) return;

  if (type === 'runtime.attach') {
    const clientId = String(message.clientId || '').trim();
    const brokerWasAvailable = Boolean(brokerClientId && endpoints.get(brokerClientId));
    const entry = ensureEndpoint(clientId, endpoint, {
      surface: message.surface,
      broker: message.broker === true,
    });
    startHydrationInBackground();
    endpointPost(endpoint, {
      type: 'runtime.attached',
      buildId: RUNTIME_WORKER_BUILD_ID,
      clientId: entry?.clientId || '',
      snapshot: runtimeSnapshot(),
    });
    const brokerIsAvailable = Boolean(brokerClientId && endpoints.get(brokerClientId));
    if (brokerWasAvailable !== brokerIsAvailable || entry?.broker) {
      broadcastSnapshot();
      scheduleProjectionSync(0);
    }
    return;
  }

  // Hydration repairs retained state, but it must not gate runtime writes,
  // broker forwarding, or projection repair. A slow gateway/relay path should
  // never make local runtime put/get messages time out.
  startHydrationInBackground();

  let response = null;

  switch (type) {
    case 'runtime.detach': {
      deleteEndpoint(message.clientId);
      response = {
        type: 'runtime.detached',
        buildId: RUNTIME_WORKER_BUILD_ID,
      };
      break;
    }
    case 'runtime.snapshot.get': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'runtime.snapshot.get',
        ok: true,
        result: runtimeSnapshot(),
      };
      break;
    }
    case 'runtime.status.put': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'runtime.status.put',
        ...handleStatusPut(message, endpoint),
      };
      break;
    }
    case BROKER.SERVICE_ACCESS_CONTEXT_PUT: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: BROKER.SERVICE_ACCESS_CONTEXT_PUT,
        ...handleServiceAccessContextPut(message),
      };
      break;
    }
    case BROKER.SERVICE_ACCESS_CONTEXT_GET: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: BROKER.SERVICE_ACCESS_CONTEXT_GET,
        ...handleServiceAccessContextGet(message),
      };
      break;
    }
    case BROKER.SERVICE_ACCESS_CONTEXT_DELETE: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: BROKER.SERVICE_ACCESS_CONTEXT_DELETE,
        ...handleServiceAccessContextDelete(message),
      };
      break;
    }
    case BROKER.PROJECTION_PUT: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: BROKER.PROJECTION_PUT,
        ...handleProjectionPut(message),
      };
      break;
    }
    case BROKER.PROJECTION_GET: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: BROKER.PROJECTION_GET,
        ...handleProjectionGet(message),
      };
      break;
    }
    case PROJECTION_POLICY_PUT: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: PROJECTION_POLICY_PUT,
        ...handleProjectionPolicyPut(message),
      };
      break;
    }
    case 'managedAppliances.sourceSnapshot.put': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'managedAppliances.sourceSnapshot.put',
        ...handleManagedSourceSnapshotPut(message),
      };
      break;
    }
    case BROKER.SERVICE_SIGNAL_REQUEST:
    case BROKER.SERVICE_ACCESS_REQUEST:
    case BROKER.SERVICE_PROJECTION_REQUEST:
    case 'gateway.grant.request':
    case 'gateway.service.install.request':
    case 'gateway.zones.sync.request': {
      const result = forwardBrokerRequest(type, message, endpoint);
      if (!result.ok) {
        response = {
          type: 'runtime.response',
          requestId: String(message.requestId || '').trim(),
          kind: type,
          ...result,
        };
      }
      break;
    }
    case BROKER.SERVICE_SIGNAL_RESPONSE: {
      response = {
        type: 'runtime.ack',
        kind: BROKER.SERVICE_SIGNAL_RESPONSE,
        ...handleBrokerResponse(BROKER.SERVICE_SIGNAL_REQUEST, message),
      };
      break;
    }
    case BROKER.SERVICE_PROJECTION_RESPONSE: {
      response = {
        type: 'runtime.ack',
        kind: BROKER.SERVICE_PROJECTION_RESPONSE,
        ...handleServiceProjectionResponse(message),
      };
      break;
    }
    case BROKER.SERVICE_ACCESS_RESPONSE: {
      response = {
        type: 'runtime.ack',
        kind: BROKER.SERVICE_ACCESS_RESPONSE,
        ...handleBrokerResponse(BROKER.SERVICE_ACCESS_REQUEST, message),
      };
      break;
    }
    case 'gateway.grant.response': {
      response = {
        type: 'runtime.ack',
        kind: 'gateway.grant.response',
        ...handleBrokerResponse('gateway.grant.request', message),
      };
      break;
    }
    case 'gateway.service.install.response': {
      response = {
        type: 'runtime.ack',
        kind: 'gateway.service.install.response',
        ...handleBrokerResponse('gateway.service.install.request', message),
      };
      break;
    }
    case 'gateway.zones.sync.response': {
      response = {
        type: 'runtime.ack',
        kind: 'gateway.zones.sync.response',
        ...handleBrokerResponse('gateway.zones.sync.request', message),
      };
      break;
    }
    default: {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: type,
        ok: false,
        error: `unsupported runtime message: ${type}`,
      };
      break;
    }
  }

  if (response) endpointPost(endpoint, response);
}

function attachSharedPort(port) {
  const endpoint = { port, clientId: '', broker: false, surface: '' };
  port.onmessage = (event) => {
    void handleControlMessage(event.data || {}, endpoint).catch((error) => {
      endpointPost(endpoint, {
        type: 'runtime.response',
        requestId: String(event?.data?.requestId || '').trim(),
        kind: String(event?.data?.type || '').trim(),
        ok: false,
        error: String(error?.message || error || 'runtime failure'),
      });
    });
  };
  port.start();
}

self.onconnect = (event) => {
  const port = event.ports?.[0];
  if (port) attachSharedPort(port);
};
