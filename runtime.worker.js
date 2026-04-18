const RUNTIME_VERSION = Object.freeze({ major: 2, minor: 8 });
const RUNTIME_WORKER_BUILD_ID = `runtime-${RUNTIME_VERSION.major}.${RUNTIME_VERSION.minor}`;
const CONTEXT_TTL_FALLBACK_MS = 2 * 60 * 1000;
const APPLIANCE_DISCOVERY_MAX_AGE_MS = 60 * 60 * 1000;
const RUNTIME_STATE_KEY = `runtime.shared.state.v${RUNTIME_VERSION.major}`;
const RUNTIME_META_KEY = `runtime.shared.meta.v${RUNTIME_VERSION.major}`;

const DB_NAME = 'constitute_db';
const DB_VER = 1;

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains('kv')) db.createObjectStore('kv');
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
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
  const role = normalizeRole(rec?.role || rec?.nodeType || rec?.type || '');
  const service = normalizeRole(rec?.service || '');
  return role === 'gateway' || service === 'gateway';
}

function isNvrRecord(rec) {
  const role = normalizeRole(rec?.role || rec?.nodeType || rec?.type || '');
  const service = normalizeRole(rec?.service || '');
  return role === 'nvr' || service === 'nvr';
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
const launchContexts = new Map();
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

function managedLaunchContextObject() {
  return Object.fromEntries(Array.from(launchContexts.entries()).map(([launchId, context]) => [launchId, safeClone(context)]));
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
  const role = String(record?.role || record?.nodeType || record?.type || '').trim().toLowerCase();
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
    if (!isNvrRecord(candidate)) continue;
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
    deviceLabel: String(hosted.deviceLabel || hosted.device_label || actual.deviceLabel || actual.label || hosted.service || 'service').trim(),
    deviceKind: String(hosted.deviceKind || hosted.device_kind || actual.deviceKind || actual.device_kind || 'service').trim() || 'service',
    role: String(hosted.service || actual.role || actual.nodeType || actual.type || '').trim(),
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
    if (!(isGatewayRecord(rec) || isNvrRecord(rec))) continue;
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

function cleanupLaunchContexts() {
  const now = nowMs();
  let changed = false;
  for (const [launchId, context] of launchContexts.entries()) {
    const expiresAt = Number(context?.expiresAt || 0) || (Number(context?.createdAt || 0) + CONTEXT_TTL_FALLBACK_MS);
    if (expiresAt && expiresAt <= now) {
      launchContexts.delete(launchId);
      changed = true;
    }
  }
  return changed;
}

function runtimeSnapshot() {
  if (cleanupLaunchContexts()) {
    touchRuntime();
    schedulePersist();
  }
  return {
    buildId: RUNTIME_WORKER_BUILD_ID,
    updatedAt: runtimeUpdatedAt || nowMs(),
    shell: safeClone(runtimeStatus.shell),
    services: safeClone(runtimeStatus.services),
    managedAppliances: safeClone(managedState.applianceSnapshot),
    resourceNames: safeClone(managedState.resourceNames),
    managedServiceIssue: safeClone(managedState.managedServiceIssue),
    launchContextCount: launchContexts.size,
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
    launchContexts: managedLaunchContextObject(),
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
    launchContexts.clear();
    const persistedContexts = payload.launchContexts && typeof payload.launchContexts === 'object'
      ? payload.launchContexts
      : {};
    for (const [launchId, context] of Object.entries(persistedContexts)) {
      const key = String(launchId || '').trim();
      if (!key || !context || typeof context !== 'object') continue;
      launchContexts.set(key, safeClone(context));
    }
  }
  rebuildManagedApplianceSnapshot();
  if (cleanupLaunchContexts()) {
    touchRuntime();
    schedulePersist();
  }
}

function ensureHydrated() {
  if (!hydratePromise) {
    hydratePromise = hydrateRuntimeState().catch((error) => {
      hydratePromise = null;
      throw error;
    });
  }
  return hydratePromise;
}

function handleStatusPut(message, endpoint) {
  const role = String(message.role || message.surface || endpoint?.surface || '').trim().toLowerCase();
  const payload = message.status && typeof message.status === 'object' ? message.status : {};
  touchRuntime();

  if (role === 'shell') {
    runtimeStatus.shell = {
      ...safeClone(payload),
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
  runtimeStatus.services[service] = {
    ...safeClone(payload),
    service,
    updatedAt: runtimeUpdatedAt,
  };
  schedulePersist();
  broadcastSnapshot();
  return { ok: true, result: runtimeSnapshot() };
}

function handleLaunchContextPut(message) {
  const context = message.context && typeof message.context === 'object' ? message.context : null;
  const launchId = String(context?.launchId || '').trim();
  if (!launchId) return { ok: false, error: 'missing launchId' };
  launchContexts.set(launchId, safeClone(context));
  cleanupLaunchContexts();
  touchRuntime();
  schedulePersist();
  broadcastSnapshot();
  return { ok: true, result: safeClone(launchContexts.get(launchId) || null) };
}

function handleLaunchContextGet(message) {
  cleanupLaunchContexts();
  const launchId = String(message.launchId || '').trim();
  if (!launchId) return { ok: false, error: 'missing launchId' };
  return { ok: true, result: safeClone(launchContexts.get(launchId) || null) };
}

function handleLaunchContextDelete(message) {
  const launchId = String(message.launchId || '').trim();
  if (!launchId) return { ok: false, error: 'missing launchId' };
  const existed = launchContexts.delete(launchId);
  if (existed) {
    touchRuntime();
    schedulePersist();
    broadcastSnapshot();
  }
  return { ok: true, result: { deleted: existed } };
}

function handleManagedSourceSnapshotPut(message) {
  managedState.sourceSnapshot = normalizeManagedSourceSnapshot(message.sourceSnapshot);
  rebuildManagedApplianceSnapshot();
  touchRuntime();
  schedulePersist();
  broadcastSnapshot();
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
  await ensureHydrated();

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
        snapshot: runtimeSnapshot(),
      };
      break;
    }
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
    case 'launchContext.delete': {
      response = {
        type: 'runtime.response',
        requestId: String(message.requestId || '').trim(),
        kind: 'launchContext.delete',
        ...handleLaunchContextDelete(message),
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
    case 'gateway.signal.request':
    case 'gateway.launch.request':
    case 'gateway.grant.request': {
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
    case 'gateway.grant.response': {
      response = {
        type: 'runtime.ack',
        kind: 'gateway.grant.response',
        ...handleBrokerResponse('gateway.grant.request', message),
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
