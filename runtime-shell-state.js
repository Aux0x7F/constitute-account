const OFFLINE_TONE = "connStateText-offline";
const LIMITED_TONE = "connStateText-limited";
const CONNECTED_TONE = "connStateText-connected";

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  return String(value || "").trim();
}

function firstText(...values) {
  for (const value of values) {
    const normalized = text(value);
    if (normalized) return normalized;
  }
  return "";
}

function safeJson(raw) {
  try {
    return JSON.parse(String(raw || ""));
  } catch {
    return null;
  }
}

function storageValue(storage, key) {
  try {
    return storage && typeof storage.getItem === "function" ? storage.getItem(key) : null;
  } catch {
    return null;
  }
}

function storageRecordList(storage, key) {
  const value = safeJson(storageValue(storage, key));
  const records = value && typeof value === "object" && !Array.isArray(value)
    ? value.records
    : value;
  return Array.isArray(records)
    ? records.filter((entry) => entry && typeof entry === "object" && !Array.isArray(entry))
    : [];
}

function withoutAt(value) {
  return text(value).replace(/^@+/, "");
}

function objectCount(value) {
  return Object.keys(record(value)).length;
}

function projectionCount(snapshot) {
  return objectCount(record(snapshot).projections);
}

function serviceCount(snapshot) {
  const snap = record(snapshot);
  const catalogServices = Array.isArray(snap.serviceCatalog?.services) ? snap.serviceCatalog.services : [];
  if (catalogServices.length > 0) return catalogServices.length;
  const services = record(snap.services);
  if (Object.keys(services).length > 0) return Object.keys(services).length;
  const managed = record(snap.managedAppliances);
  let count = 0;
  for (const bucket of [managed.owned, managed.granted, managed.discoverable]) {
    const records = Array.isArray(bucket) ? bucket : [];
    for (const entry of records) {
      const hosted = Array.isArray(entry?.hostedServices) ? entry.hostedServices : [];
      count += hosted.length;
    }
  }
  return count;
}

function normalizeRole(value) {
  return text(value).toLowerCase();
}

function serviceRecord(recordValue) {
  const value = record(recordValue);
  const role = normalizeRole(value.role || value.type);
  const service = normalizeRole(value.service || value.slug || value.name);
  return service && service !== "gateway" && role !== "gateway";
}

function gatewayRecord(recordValue) {
  const value = record(recordValue);
  const role = normalizeRole(value.role || value.type || value.deviceKind || value.device_kind);
  const service = normalizeRole(value.service || value.slug || value.name);
  return role === "gateway" || service === "gateway";
}

function hostedServiceRecords(records) {
  const out = [];
  for (const entry of Array.isArray(records) ? records : []) {
    const source = record(entry);
    const gatewayPk = firstText(source.devicePk, source.pk, source.gatewayPk);
    const hosted = Array.isArray(source.hostedServices || source.hosted_services)
      ? (source.hostedServices || source.hosted_services)
      : [];
    for (const service of hosted) {
      const serviceRecord = record(service);
      const devicePk = firstText(serviceRecord.devicePk, serviceRecord.device_pk, serviceRecord.servicePk, serviceRecord.service_pk, serviceRecord.pk);
      if (!devicePk) continue;
      out.push({
        ...serviceRecord,
        devicePk,
        pk: devicePk,
        hostGatewayPk: firstText(serviceRecord.hostGatewayPk, serviceRecord.host_gateway_pk, gatewayPk),
        service: firstText(serviceRecord.service, serviceRecord.slug, serviceRecord.name),
        role: firstText(serviceRecord.role, serviceRecord.service, serviceRecord.slug, serviceRecord.name),
        deviceKind: firstText(serviceRecord.deviceKind, serviceRecord.device_kind, "service"),
      });
    }
  }
  return out;
}

function hostedServiceRecordsFromGatewaySnapshots(storage) {
  const value = safeJson(storageValue(storage, "constitute.gatewayHostedSnapshots"));
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  const gatewayRecords = [];
  for (const [gatewayPk, snapshot] of Object.entries(value)) {
    const source = record(snapshot);
    gatewayRecords.push({
      ...source,
      devicePk: firstText(source.devicePk, source.gatewayPk, gatewayPk),
      pk: firstText(source.devicePk, source.gatewayPk, gatewayPk),
      hostGatewayPk: firstText(source.hostGatewayPk, source.gatewayPk, gatewayPk),
      role: "gateway",
      service: "gateway",
      deviceKind: "gateway",
    });
  }
  return hostedServiceRecords(gatewayRecords);
}

export function browserStorageShellContext(storage = undefined) {
  const source = storage || (typeof globalThis !== "undefined" ? globalThis.localStorage : null);
  const identities = storageRecordList(source, "swarm.identityCache");
  const devices = storageRecordList(source, "swarm.deviceCache");
  const identity = identities.find((entry) => firstText(entry.identityId, entry.id)) || {};
  const browserDevice = devices.find((entry) => {
    const role = normalizeRole(entry.role || entry.deviceKind || entry.device_kind);
    return role === "browser" && firstText(entry.devicePk, entry.pk);
  }) || {};
  const gateway = devices.find((entry) => gatewayRecord(entry) && firstText(entry.devicePk, entry.pk)) || {};
  const services = [
    ...devices.filter(serviceRecord),
    ...hostedServiceRecords(devices),
    ...hostedServiceRecordsFromGatewaySnapshots(source),
  ];
  const serviceCount = new Set(
    services
      .map((entry) => firstText(entry.servicePk, entry.service_pk, entry.devicePk, entry.pk, entry.service))
      .filter(Boolean),
  ).size;

  return {
    identityId: firstText(identity.identityId, identity.id, browserDevice.identityId, browserDevice.ownerIdentityId),
    identityLabel: firstText(identity.label, identity.handle, identity.name),
    devicePk: firstText(browserDevice.devicePk, browserDevice.pk),
    gatewayPk: firstText(gateway.devicePk, gateway.pk),
    identityEvidence: Boolean(firstText(identity.identityId, identity.id, browserDevice.identityId, browserDevice.devicePk, browserDevice.pk)),
    evidenceSource: identities.length || devices.length ? "browserStorageCache" : "",
    serviceCount,
    serviceState: serviceCount > 0 ? "available" : "",
    gatewayState: firstText(gateway.status, gateway.state, gatewayRecord(gateway) ? "known" : ""),
  };
}

function normalizeConnectionCode(code) {
  const normalized = text(code).toLowerCase();
  if (normalized === "live") return "connected";
  return normalized;
}

export function runtimeShellConnectionToneClass(code) {
  const normalized = normalizeConnectionCode(code);
  if (["connected", "healthy", "online"].includes(normalized)) return CONNECTED_TONE;
  if (["connected-limited", "degraded", "connecting", "pending", "missing-authority", "routed", "accepted"].includes(normalized)) {
    return LIMITED_TONE;
  }
  return OFFLINE_TONE;
}

export function deriveRuntimeShellState(snapshot = {}, options = {}) {
  const snap = record(snapshot);
  const shell = record(snap.shell);
  const identity = record(shell.identity);
  const context = record(options.context);
  const resourceNames = record(snap.resourceNames);
  const connection = record(shell.connection);
  const relay = record(shell.relay);
  const gateway = record(shell.ownedGateway);
  const services = record(shell.services);
  const edge = record(snap.edge);
  const broker = record(snap.broker);
  const authority = record(snap.authority);
  const resource = record(snap.resource);
  const retention = record(snap.retention);

  const identityId = firstText(
    identity.identityId,
    identity.id,
    context.identityId,
    options.identityId,
  );
  const devicePk = firstText(
    identity.devicePk,
    identity.device_pk,
    authority.devicePk,
    context.devicePk,
    context.requesterRef,
    options.devicePk,
  );
  const identityEvidence = Boolean(identityId || devicePk || identity.linked === true || context.identityEvidence === true);
  const authorityMissing = identityEvidence && identity.linked === false;
  const explicitAuthorityState = Boolean(firstText(
    authority.state,
    identity.authorityPosture,
    context.authorityPosture,
  ));
  const rawLabel = withoutAt(firstText(
    identity.label,
    identity.handle,
    context.identityLabel,
    context.label,
    identityId ? resourceNames[identityId] : "",
  ));
  const handle = rawLabel ? `@${rawLabel}` : identityEvidence ? "@linked" : "@unlinked";
  const linked = identityEvidence;
  const resolvedType = firstText(
    context.resolvedType,
    rawLabel ? "friendly" : "",
    devicePk && !identityId ? "device" : "",
    identityId ? "publicKey" : "",
    "none",
  );
  const identityResolution = firstText(
    context.identityResolution,
    rawLabel ? "named" : "",
    identityEvidence ? "linked" : "unlinked",
  );
  const authorityPosture = firstText(
    authority.state,
    identity.authorityPosture,
    context.authorityPosture,
    authorityMissing ? "missingAuthority" : "",
    linked ? "present" : "absent",
  );
  const authorityReady = authority.ready === true || authorityPosture === "ready";

  const projectionMaterialized = projectionCount(snap) > 0;
  const activeInteraction = record(options.activeInteraction);
  const adapterLive = options.adapterLive === true || activeInteraction.adapterLive === true;
  const routeDelivered = options.routeDelivered === true || activeInteraction.routeDelivered === true;
  const serviceAccepted = options.serviceAccepted === true || activeInteraction.serviceAccepted === true;

  let connectionCode = normalizeConnectionCode(firstText(connection.code, connection.overall));
  let connectionLabel = firstText(connection.label, connection.overall);
  let connectionReason = firstText(connection.reason);
  if (adapterLive) {
    connectionCode = "live";
    connectionLabel = firstText(options.connectionLabel, "Live");
    connectionReason = firstText(options.connectionReason, "Live adapter attached.");
  } else if (serviceAccepted) {
    connectionCode = "accepted";
    connectionLabel = firstText(options.connectionLabel, "Accepted");
    connectionReason = firstText(options.connectionReason, "Service accepted; waiting for adapter.");
  } else if (routeDelivered) {
    connectionCode = "routed";
    connectionLabel = firstText(options.connectionLabel, "Routed");
    connectionReason = firstText(options.connectionReason, "Route delivered; waiting for adapter.");
  } else if (!connectionCode && (edge.connected === true || broker.available === true || snap.buildId)) {
    connectionCode = "connected-limited";
    connectionLabel = "Runtime attached";
    connectionReason = "Runtime is attached; product projections may still be materializing.";
  } else if (!connectionCode) {
    connectionCode = "offline";
    connectionLabel = "Offline";
    connectionReason = "Waiting for account runtime.";
  }
  if (!authorityReady && explicitAuthorityState && identityEvidence && ["offline", "connected-limited", ""].includes(connectionCode)) {
    connectionCode = "missing-authority";
    connectionLabel = "Missing authority";
    connectionReason = firstText(authority.reason, "Identity or device is present but authority is incomplete.");
  }

  const servicesAvailable = serviceCount(snap) || Number(context.serviceCount || context.servicesAvailable || 0) || (context.servicePk ? 1 : 0);
  const gatewayState = firstText(gateway.state, context.gatewayState, edge.connected === true ? "connected" : "", context.gatewayPk ? "known" : "");
  const serviceState = firstText(
    services.state,
    context.serviceState,
    servicesAvailable > 0 ? "available" : "",
    projectionMaterialized ? "projecting" : "",
    "unknown",
  );
  const productRunlevel = firstText(
    options.productRunlevel,
    adapterLive ? "live" : "",
    serviceAccepted ? "accepted" : "",
    routeDelivered ? "routed" : "",
    projectionMaterialized ? "stale" : "",
    servicesAvailable > 0 ? "linked" : "",
    linked ? "linked" : "",
    snap.buildId || broker.available ? "attached" : "",
    "offline",
  );

  return Object.freeze({
    runlevel: productRunlevel,
    identity: Object.freeze({
      linked,
      resolution: identityResolution,
      resolvedType,
      authorityPosture,
      evidenceSource: firstText(context.evidenceSource, identity.evidenceSource),
      authorityState: authorityReady ? "ready" : identityEvidence ? authorityPosture : "unlinked",
      identityId,
      devicePk,
      handle,
      label: handle,
      title: linked ? "Open account center" : "Identity not linked yet",
      ariaLabel: identityId ? `Identity ${identityId}` : linked ? "Identity present" : "Identity not linked",
      toneClass: linked ? "identityHandle-linked" : "identityHandle-unlinked",
      degraded: !authorityReady && explicitAuthorityState && identityEvidence,
    }),
    connection: Object.freeze({
      code: connectionCode,
      label: connectionLabel || "Offline",
      reason: connectionReason || "Waiting for account runtime.",
      toneClass: runtimeShellConnectionToneClass(connectionCode),
    }),
    relay: Object.freeze({
      state: firstText(relay.state, relay.label, broker.available === true ? "available" : "offline"),
      reason: firstText(relay.reason),
    }),
    gateway: Object.freeze({
      state: gatewayState || "unknown",
      reason: firstText(gateway.reason),
      gatewayPk: firstText(gateway.gatewayPk, context.gatewayPk),
    }),
    services: Object.freeze({
      state: serviceState,
      reason: firstText(services.reason),
      count: servicesAvailable,
    }),
    projections: Object.freeze({
      count: projectionCount(snap),
      materialized: projectionMaterialized,
      freshness: projectionMaterialized ? "materialized" : "missing",
    }),
    resource: Object.freeze({
      state: firstText(resource.state, "unknown"),
      reason: firstText(resource.reason),
      profileId: firstText(resource.profileId),
      cleanupAllowed: resource.cleanupAllowed === true,
      cleanupReason: firstText(resource.cleanupReason),
    }),
    retention: Object.freeze({
      state: firstText(retention.state, "unknown"),
      reason: firstText(retention.reason),
      releaseRequired: retention.releaseRequired !== false,
      destructiveAction: retention.destructiveAction === true,
    }),
    interaction: Object.freeze({
      routeDelivered,
      adapterLive,
      serviceAccepted,
      projectionMaterialized,
    }),
  });
}
