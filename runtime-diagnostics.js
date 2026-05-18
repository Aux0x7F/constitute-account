import { SWARM, assertConsumerFloor, assertMaterializationBudget } from "constitute-protocol";

export const RUNTIME_DIAGNOSTICS_STORAGE_KEY = "constitute.runtime.diagnostics.enabled";
export const RUNTIME_DIAGNOSTICS_RING_LIMIT = 300;
export const RUNTIME_DIAGNOSTICS_SUBSCRIBE = "runtime.diagnostics.subscribe";
export const RUNTIME_DIAGNOSTICS_UNSUBSCRIBE = "runtime.diagnostics.unsubscribe";
export const RUNTIME_DIAGNOSTICS_COMMAND = "runtime.diagnostics.command";
export const RUNTIME_DIAGNOSTIC_PLANES = Object.freeze({
  AUTHORITY: "authority",
  ROUTE: "route",
  ACTIVATION: "activation",
  PROJECTION: "projection",
  PROJECTION_REPAIR: "projectionRepair",
  RETENTION: "retention",
  DIAGNOSTIC: "diagnostic",
  DEV_BRIDGE: "devBridge",
  LOGGING_REPLAY: "loggingReplay",
  BULK_RETAINED_DATA: "bulkRetainedData",
});

export const RUNTIME_DIAGNOSTIC_OPERATOR_PLANES = Object.freeze([
  RUNTIME_DIAGNOSTIC_PLANES.AUTHORITY,
  RUNTIME_DIAGNOSTIC_PLANES.ROUTE,
  RUNTIME_DIAGNOSTIC_PLANES.ACTIVATION,
  RUNTIME_DIAGNOSTIC_PLANES.PROJECTION_REPAIR,
  RUNTIME_DIAGNOSTIC_PLANES.RETENTION,
  RUNTIME_DIAGNOSTIC_PLANES.DIAGNOSTIC,
]);

const WINDOW_LOCAL_COMMANDS = new Set([
  "dumpRecentEvents",
  "routeExplain",
  "projectionExplain",
  "hardRefresh",
  "reloadRuntimeAttachment",
]);

const RUNTIME_FORWARDED_COMMANDS = new Set([
  "activationExplain",
  "requestProjectionRepair",
  "resourceSample",
  "flushDiagnosticsToLogging",
  "openTestActivation",
  "closeTestActivation",
]);

function safeWindow(candidate) {
  return candidate && typeof candidate === "object" ? candidate : globalThis;
}

function randomOpaqueId(prefix) {
  try {
    const bytes = new Uint8Array(8);
    globalThis.crypto?.getRandomValues?.(bytes);
    const token = Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
    if (token) return `${prefix}-${token}`;
  } catch {}
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function debugSearchEnabled(win) {
  try {
    const params = new URLSearchParams(win.location?.search || "");
    const value = String(params.get("debug") || "").trim().toLowerCase();
    if (value === "1" || value === "true" || value === "yes") return true;
    if (value === "0" || value === "false" || value === "no") return false;
  } catch {}
  return null;
}

export function runtimeDiagnosticsEnabled(win = globalThis) {
  const target = safeWindow(win);
  const search = debugSearchEnabled(target);
  if (search === true) {
    try { target.localStorage?.setItem(RUNTIME_DIAGNOSTICS_STORAGE_KEY, "1"); } catch {}
    return true;
  }
  if (search === false) {
    try { target.localStorage?.removeItem(RUNTIME_DIAGNOSTICS_STORAGE_KEY); } catch {}
    return false;
  }
  try {
    if (target.localStorage?.getItem(RUNTIME_DIAGNOSTICS_STORAGE_KEY) === "1") return true;
  } catch {}
  return target.__constituteOperatorProfile?.debug === true;
}

function ensureRing(win) {
  const target = safeWindow(win);
  if (!Array.isArray(target.__constituteRuntimeDiagnostics)) {
    target.__constituteRuntimeDiagnostics = [];
  }
  return target.__constituteRuntimeDiagnostics;
}

function pushRing(win, event) {
  const ring = ensureRing(win);
  ring.push(event);
  while (ring.length > RUNTIME_DIAGNOSTICS_RING_LIMIT) ring.shift();
  return ring;
}

function localRingConsumerFloor(ring, clientId, sampledAt = Date.now()) {
  const last = ring[ring.length - 1] || null;
  const lagging = ring.length >= RUNTIME_DIAGNOSTICS_RING_LIMIT;
  const eventTimeFloor = Number(last?.observedAt || 0) || undefined;
  return assertConsumerFloor({
    kind: SWARM.RECORD_KIND.CONSUMER_FLOOR,
    floorId: `floor:runtime-diagnostics-agent:${clientId}`,
    consumerRef: clientId,
    materializationId: `runtime-diagnostics-agent:${clientId}`,
    subjectRef: "runtime.diagnostics.agent-ring",
    cursor: String(last?.eventId || "").trim() || undefined,
    ackFloor: String(ring.length),
    witnessFloor: String(ring.length),
    compactionFloor: String(Math.max(0, ring.length - RUNTIME_DIAGNOSTICS_RING_LIMIT)),
    eventTimeFloor,
    observedTimeFloor: Math.max(sampledAt, Number(eventTimeFloor || 0)),
    lagState: lagging
      ? SWARM.MATERIALIZATION_LAG_STATE.LAGGING
      : SWARM.MATERIALIZATION_LAG_STATE.CAUGHT_UP,
    reason: lagging ? "runtime diagnostics ring reached local materialization limit" : undefined,
    replay: { mode: "debug-ring", replayLimit: RUNTIME_DIAGNOSTICS_RING_LIMIT },
    redelivery: { mode: "replace-oldest", duplicatePolicy: "eventId" },
    sampledAt,
    expiresAt: sampledAt + 60_000,
  });
}

function localRingMaterializationBudget(ring, surface, clientId, sampledAt = Date.now()) {
  const pressure = ring.length >= RUNTIME_DIAGNOSTICS_RING_LIMIT;
  return assertMaterializationBudget({
    kind: SWARM.RECORD_KIND.MATERIALIZATION_BUDGET,
    budgetId: `runtime-diagnostics-agent:${clientId}`,
    sourceAuthority: "runtime.diagnostics.agent",
    consumerRef: clientId,
    payloadClass: SWARM.MATERIALIZATION_PAYLOAD_CLASS.EVIDENCE,
    copyRole: SWARM.MATERIALIZATION_COPY_ROLE.DEBUG,
    transferMode: SWARM.MATERIALIZATION_TRANSFER_MODE.CLONE,
    privacyTier: SWARM.MATERIALIZATION_PRIVACY_TIER.SAFE_FACTS,
    state: pressure ? SWARM.RESOURCE_POSTURE_STATE.PRESSURE : SWARM.RESOURCE_POSTURE_STATE.WITHIN_BUDGET,
    limits: {
      maxRingEvents: RUNTIME_DIAGNOSTICS_RING_LIMIT,
      ringCount: ring.length,
      surface,
    },
    snapshotPolicy: { mode: "bounded-debug-ring" },
    deltaPolicy: { mode: "push-append-drop-oldest" },
    coalescing: { key: "eventId" },
    cardinality: { maxEventIds: RUNTIME_DIAGNOSTICS_RING_LIMIT },
    schema: {
      state: SWARM.MATERIALIZATION_SCHEMA_STATE.CURRENT,
      version: "runtime-diagnostics-agent.v1",
    },
    consumerFloor: localRingConsumerFloor(ring, clientId, sampledAt),
    blockedReasons: pressure ? ["runtimeDiagnosticsAgentRingPressure"] : [],
    retentionClass: "ephemeral.debug-ring",
    issuedAt: sampledAt,
    releaseAfter: sampledAt,
    expiresAt: sampledAt + 60_000,
  });
}

function summarizeEvent(event) {
  const facts = event?.safeFacts && typeof event.safeFacts === "object" ? event.safeFacts : {};
  const error = facts.error && typeof facts.error === "object" ? facts.error : {};
  const parts = [
    event?.kind,
    event?.frameId ? `frame=${String(event.frameId).slice(0, 12)}` : "",
    event?.correlationId ? `corr=${String(event.correlationId).slice(0, 12)}` : "",
    event?.channelRef ? `channel=${event.channelRef}` : "",
    event?.capabilityRef ? `cap=${event.capabilityRef}` : "",
    facts.state ? `state=${facts.state}` : "",
    facts.message || error.message || error.code || "",
  ].map((part) => String(part || "").trim()).filter(Boolean);
  return parts.join(" ");
}

function logEvent(log, event) {
  const level = String(event?.level || "info").toLowerCase();
  const method = level === "error" ? "error" : level === "warn" ? "warn" : "debug";
  const line = `[runtime diagnostic] ${level} ${summarizeEvent(event)}`.trim();
  try {
    (log?.[method] || log?.debug || log?.log)?.call(log, line, event);
  } catch {}
}

function normalizeDiagnosticSubscription(options, surface, clientId) {
  const explicit = options.subscription && typeof options.subscription === "object" ? options.subscription : {};
  const planes = Array.isArray(explicit.planes)
    ? explicit.planes
    : Array.isArray(options.planes)
      ? options.planes
      : Object.values(RUNTIME_DIAGNOSTIC_PLANES);
  const cost = {
    ...(explicit.cost && typeof explicit.cost === "object" ? explicit.cost : {}),
  };
  if (options.minLevel !== undefined && cost.minLevel === undefined) cost.minLevel = options.minLevel;
  if (options.minLevelByPlane !== undefined && cost.minLevelByPlane === undefined) {
    cost.minLevelByPlane = options.minLevelByPlane;
  }
  if (options.denyKinds !== undefined && cost.denyKinds === undefined) cost.denyKinds = options.denyKinds;
  const issuedAt = Date.now();
  return {
    ...explicit,
    kind: "subscription.contract",
    subscriptionId: String(explicit.subscriptionId || `runtime-diagnostics:${clientId}`).trim(),
    subscriberRef: String(explicit.subscriberRef || clientId).trim(),
    publisherRef: String(explicit.publisherRef || "runtime:shared").trim(),
    planes,
    subjectSelector: {
      ...(explicit.subjectSelector && typeof explicit.subjectSelector === "object" ? explicit.subjectSelector : {}),
      recordKind: "runtime.diagnostic.event",
    },
    audience: {
      ...(explicit.audience && typeof explicit.audience === "object" ? explicit.audience : {}),
      surface,
      clientId,
    },
    cost,
    proof: {
      requirement: "none",
      ...(explicit.proof && typeof explicit.proof === "object" ? explicit.proof : {}),
    },
    delivery: {
      mode: "push",
      ...(explicit.delivery && typeof explicit.delivery === "object" ? explicit.delivery : {}),
    },
    backpressure: {
      behavior: "drop",
      ...(explicit.backpressure && typeof explicit.backpressure === "object" ? explicit.backpressure : {}),
    },
    window: {
      ...(explicit.window && typeof explicit.window === "object" ? explicit.window : {}),
      replayLimit: Math.max(0, Math.min(RUNTIME_DIAGNOSTICS_RING_LIMIT, Number(options.limit ?? explicit.window?.replayLimit ?? 80))),
    },
    issuedAt: Number(explicit.issuedAt || issuedAt),
    expiresAt: explicit.expiresAt,
  };
}

function localRouteExplain(ring, args = {}) {
  const target = String(args.frameId || args.correlationId || args.activationId || "").trim();
  const out = [];
  for (const event of ring) {
    if (String(event.kind || "") !== "route.observation") continue;
    if (!target) {
      out.push(event);
      continue;
    }
    if ([event.frameId, event.correlationId, event.activationId, event.routePromiseId]
      .map((value) => String(value || "").trim())
      .includes(target)) out.push(event);
  }
  return out;
}

function localProjectionExplain(ring, args = {}) {
  const target = String(args.projectionKey || args.channelId || "").trim();
  const out = [];
  for (const event of ring) {
    if (!String(event.kind || "").startsWith("projection.")) continue;
    if (!target) {
      out.push(event);
      continue;
    }
    if ([event.projectionKey, event.projectionId, event.channelRef]
      .map((value) => String(value || "").trim())
      .includes(target)) out.push(event);
  }
  return out;
}

async function clearDebugCaches(win) {
  const cachesApi = win.caches || globalThis.caches;
  if (!cachesApi?.keys || !cachesApi?.delete) return [];
  try {
    const keys = await cachesApi.keys();
    const deleted = [];
    for (const key of Array.isArray(keys) ? keys : []) {
      if (await cachesApi.delete(key)) deleted.push(String(key));
    }
    return deleted;
  } catch {
    return [];
  }
}

async function unregisterDebugServiceWorkers(win) {
  const serviceWorker = win.navigator?.serviceWorker || globalThis.navigator?.serviceWorker;
  if (!serviceWorker?.getRegistrations) return 0;
  try {
    const registrations = await serviceWorker.getRegistrations();
    let unregistered = 0;
    for (const registration of Array.isArray(registrations) ? registrations : []) {
      if (await registration?.unregister?.()) unregistered += 1;
    }
    return unregistered;
  } catch {
    return 0;
  }
}

function hardRefreshUrl(win) {
  const fallback = "http://localhost/?debug=1";
  let url;
  try {
    url = new URL(String(win.location?.href || fallback), fallback);
  } catch {
    url = new URL(fallback);
  }
  url.searchParams.set("__constituteHardRefresh", Date.now().toString(36));
  return url.toString();
}

async function hardRefreshWindow(win, args = {}) {
  const shouldClearCaches = args.clearCaches !== false;
  const shouldUnregisterServiceWorkers = args.unregisterServiceWorkers === true;
  const shouldCacheBust = args.cacheBust !== false;
  const cacheKeysDeleted = shouldClearCaches ? await clearDebugCaches(win) : [];
  const serviceWorkersUnregistered = shouldUnregisterServiceWorkers
    ? await unregisterDebugServiceWorkers(win)
    : 0;
  try {
    win.dispatchEvent?.(new CustomEvent("constitute:runtime-diagnostics:hardRefresh", {
      detail: { cacheKeysDeleted, serviceWorkersUnregistered },
    }));
  } catch {}
  if (shouldCacheBust) {
    const href = hardRefreshUrl(win);
    if (typeof win.location?.replace === "function") win.location.replace(href);
    else if (win.location) win.location.href = href;
    return {
      ok: true,
      command: "hardRefresh",
      cacheBust: true,
      cacheKeysDeleted,
      serviceWorkersUnregistered,
      href,
    };
  }
  win.location?.reload?.(true);
  return {
    ok: true,
    command: "hardRefresh",
    cacheBust: false,
    cacheKeysDeleted,
    serviceWorkersUnregistered,
  };
}

export function attachRuntimeDiagnostics(options = {}) {
  const win = safeWindow(options.window || globalThis);
  const port = options.port || null;
  const surface = String(options.surface || "browser").trim();
  const clientId = String(options.clientId || `${surface}-diagnostics`).trim();
  const log = options.console || console;
  const enabled = options.enabled !== undefined ? options.enabled === true : runtimeDiagnosticsEnabled(win);
  const pending = new Map();
  let runtimeSessionId = "";
  let materializationBudget = null;
  let consumerFloor = null;
  const ring = ensureRing(win);
  let localMaterializationBudget = localRingMaterializationBudget(ring, surface, clientId);
  const subscription = normalizeDiagnosticSubscription(options, surface, clientId);

  const agent = {
    enabled,
    surface,
    clientId,
    get runtimeSessionId() { return runtimeSessionId; },
    get materializationBudget() { return materializationBudget; },
    get consumerFloor() { return consumerFloor; },
    get localMaterializationBudget() { return localMaterializationBudget; },
    handleMessage(message) {
      const msg = message && typeof message === "object" ? message : {};
      if (msg.type === "runtime.diagnostics.events") {
        runtimeSessionId = String(msg.runtimeSessionId || msg.diagnostics?.runtimeSessionId || runtimeSessionId || "").trim();
        materializationBudget = msg.materializationBudget && typeof msg.materializationBudget === "object"
          ? msg.materializationBudget
          : materializationBudget;
        consumerFloor = msg.consumerFloor && typeof msg.consumerFloor === "object"
          ? msg.consumerFloor
          : materializationBudget?.consumerFloor || consumerFloor;
        const deliveryMode = String(msg.delivery?.mode || "").trim();
        const shouldLogReplay = deliveryMode !== "replay" || msg.delivery?.console === true;
        for (const event of Array.isArray(msg.events) ? msg.events : []) {
          pushRing(win, event);
          if (shouldLogReplay) logEvent(log, event);
        }
        localMaterializationBudget = localRingMaterializationBudget(ring, surface, clientId);
        return true;
      }
      if (msg.type === "runtime.diagnostic.event") {
        const event = msg.event && typeof msg.event === "object" ? msg.event : null;
        runtimeSessionId = String(msg.runtimeSessionId || msg.diagnostics?.runtimeSessionId || runtimeSessionId || "").trim();
        if (event) {
          pushRing(win, event);
          logEvent(log, event);
          localMaterializationBudget = localRingMaterializationBudget(ring, surface, clientId);
        }
        return true;
      }
      if (msg.type === "runtime.response" && msg.kind === RUNTIME_DIAGNOSTICS_COMMAND) {
        const requestId = String(msg.requestId || "").trim();
        const waiter = pending.get(requestId);
        if (!waiter) return true;
        pending.delete(requestId);
        win.clearTimeout?.(waiter.timer);
        if (msg.ok === false) waiter.reject(new Error(String(msg.error || "diagnostic command failed")));
        else waiter.resolve(msg.result);
        return true;
      }
      return false;
    },
    command(name, args = {}) {
      const command = String(name || "").trim();
      if (!enabled) return Promise.reject(new Error("runtime diagnostics are disabled"));
      if (!WINDOW_LOCAL_COMMANDS.has(command) && !RUNTIME_FORWARDED_COMMANDS.has(command)) {
        return Promise.reject(new Error(`diagnostic command not allowlisted: ${command}`));
      }
      if (command === "dumpRecentEvents") return Promise.resolve(ring.slice());
      if (command === "routeExplain") return Promise.resolve(localRouteExplain(ring, args));
      if (command === "projectionExplain") return Promise.resolve(localProjectionExplain(ring, args));
      if (command === "hardRefresh") {
        return hardRefreshWindow(win, args);
      }
      if (command === "reloadRuntimeAttachment") {
        try {
          win.dispatchEvent?.(new CustomEvent("constitute:runtime-diagnostics:reloadRuntimeAttachment", { detail: args }));
        } catch {}
        return Promise.resolve({ ok: true, command });
      }
      if (!port) return Promise.reject(new Error("runtime diagnostic command transport unavailable"));
      const requestId = randomOpaqueId("runtime-debug-command");
      const nonce = randomOpaqueId("runtime-debug-nonce");
      const expiresAt = Date.now() + 30_000;
      return new Promise((resolve, reject) => {
        const timer = win.setTimeout?.(() => {
          pending.delete(requestId);
          reject(new Error(`${command} timed out`));
        }, 10_000);
        pending.set(requestId, { resolve, reject, timer });
        port.postMessage({
          type: RUNTIME_DIAGNOSTICS_COMMAND,
          requestId,
          clientId,
          command,
          args,
          nonce,
          issuedAt: Date.now(),
          expiresAt,
          audienceRuntimeSessionId: runtimeSessionId,
          local: true,
        });
      });
    },
  };

  const forwardLogging = options.logging === true;

  if (enabled && port) {
    port.postMessage({
      type: RUNTIME_DIAGNOSTICS_SUBSCRIBE,
      requestId: randomOpaqueId("runtime-diagnostics-subscribe"),
      clientId,
      surface,
      limit: subscription.window.replayLimit,
      logging: forwardLogging,
      subscription,
    });
  }

  win.__constituteDebug = Object.freeze({
    surface,
    command: agent.command.bind(agent),
    dumpRecentEvents: () => ring.slice(),
    commandNames: Object.freeze([...WINDOW_LOCAL_COMMANDS, ...RUNTIME_FORWARDED_COMMANDS]),
  });

  return agent;
}
