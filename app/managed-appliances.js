function normalizeRole(value) {
  return String(value || '').trim().toLowerCase();
}

function shortPk(value, head = 12, tail = 6) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw.length <= head + tail + 1) return raw;
  return `${raw.slice(0, head)}…${raw.slice(-tail)}`;
}

function titleCaseWords(value) {
  return String(value || '')
    .trim()
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function createManagedApplianceModel({
  applyHostedSnapshot,
  getSwarmSeen,
  applianceDiscoveryMaxAgeMs,
}) {
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

  function ownedPkSet(identityDevices) {
    return new Set(
      (Array.isArray(identityDevices) ? identityDevices : [])
        .map((d) => String(d?.pk || d?.devicePk || '').trim())
        .filter(Boolean),
    );
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
    const swarmSeen = pk ? Number(getSwarmSeen(pk) || 0) : 0;
    const hostedSeen = Array.isArray(rec?.hostedServices || rec?.hosted_services)
      ? (rec.hostedServices || rec.hosted_services).reduce((latest, hosted) => {
          const hostedUpdatedAt = Number(hosted?.updatedAt || hosted?.updated_at || 0);
          return Math.max(latest, hostedUpdatedAt);
        }, 0)
      : 0;
    return Math.max(0, nostrSeen, swarmSeen, hostedSeen);
  }

  function summarizeAppliance(rec, owned) {
    const pk = String(rec?.devicePk || rec?.pk || '').trim();
    const label = String(rec?.deviceLabel || rec?.label || '').trim();
    const deviceKind = normalizeRole(rec?.deviceKind || rec?.device_kind || '') || 'user';
    const role = normalizeRole(rec?.role || rec?.type || '') || 'unknown';
    const service = normalizeRole(rec?.service || '') || 'none';
    const version = String(rec?.serviceVersion || rec?.service_version || '').trim();
    const hostPlatformRaw = normalizeRole(rec?.hostPlatform || rec?.host_platform || rec?.platform || '');
    const hostPlatform = hostPlatformRaw === 'unknown' ? '' : hostPlatformRaw;
    const releaseChannel = String(rec?.releaseChannel || rec?.release_channel || '').trim();
    const releaseTrack = String(rec?.releaseTrack || rec?.release_track || '').trim();
    const releaseBranch = String(rec?.releaseBranch || rec?.release_branch || '').trim();
    const hostGatewayPk = String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim();
    const hostedServices = Array.isArray(rec?.hostedServices || rec?.hosted_services)
      ? (rec.hostedServices || rec.hosted_services)
      : [];
    const updatedAt = Number(rec?.updatedAt || rec?.updated_at || rec?.ts || rec?.lastSeen || 0);
    return {
      pk,
      title: label || [titleCaseWords(service && service !== 'none' ? service : role), shortPk(pk, 10, 4)].filter(Boolean).join(' • '),
      deviceKind,
      role,
      service,
      version,
      hostPlatform,
      releaseChannel,
      releaseTrack,
      releaseBranch,
      hostGatewayPk,
      hostedServices,
      updatedAt,
      owned,
    };
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

  function formatAgeShort(ts) {
    const at = Number(ts || 0);
    if (!at) return 'unknown';
    const ageSec = Math.max(0, Math.floor((Date.now() - at) / 1000));
    if (ageSec < 60) return `${ageSec}s ago`;
    const ageMin = Math.floor(ageSec / 60);
    if (ageMin < 60) return `${ageMin}m ago`;
    const ageHr = Math.floor(ageMin / 60);
    if (ageHr < 24) return `${ageHr}h ago`;
    const ageDay = Math.floor(ageHr / 24);
    return `${ageDay}d ago`;
  }

  function applianceFreshness(updatedAt) {
    const at = Number(updatedAt || 0);
    if (!at) return { label: 'unknown', css: 'freshness-unknown' };
    const ageMs = Math.max(0, Date.now() - at);
    if (ageMs <= 2 * 60 * 1000) return { label: 'live', css: 'freshness-live' };
    if (ageMs <= 15 * 60 * 1000) return { label: 'recent', css: 'freshness-recent' };
    if (ageMs <= 2 * 60 * 60 * 1000) return { label: 'stale', css: 'freshness-stale' };
    return { label: 'offline', css: 'freshness-offline' };
  }

  function formatReleaseMeta(channel, track, branch) {
    const ch = String(channel || '').trim();
    const tr = String(track || '').trim();
    const br = String(branch || '').trim();
    if (!ch && !tr && !br) return '';
    const left = [ch, tr].filter(Boolean).join('/');
    return br ? `${left || 'release'} @ ${br}` : (left || 'release');
  }

  function managedGatewayPkForRecord(record) {
    const gatewayPk = String(record?.hostGatewayPk || record?.host_gateway_pk || '').trim();
    if (gatewayPk) return gatewayPk;
    if (isGatewayRecord(record)) return String(record?.devicePk || record?.pk || '').trim();
    return '';
  }

  function managedServicePkForRecord(record) {
    return String(record?.devicePk || record?.pk || '').trim();
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
      const rec = applyHostedSnapshot(rawRec);
      const pk = String(rec?.devicePk || rec?.pk || '').trim();
      if (!pk || seen.has(pk)) continue;
      if (!(isGatewayRecord(rec) || isNvrRecord(rec))) continue;
      const ownedRec = owned.has(pk) || owned.has(String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim());
      const seenAt = applianceSeenAt(rec);
      const ageMs = seenAt ? Math.max(0, Date.now() - seenAt) : Number.POSITIVE_INFINITY;
      if (!ownedRec && ageMs > applianceDiscoveryMaxAgeMs) continue;
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

  function findGatewayHostedServiceRecord(gatewayPk, applianceRecords, serviceName = 'nvr') {
    const targetGatewayPk = String(gatewayPk || '').trim();
    const targetService = normalizeRole(serviceName || '');
    if (!targetGatewayPk || !targetService) return null;
    const records = Array.isArray(applianceRecords) ? applianceRecords : [];
    const matches = records.filter((rec) => {
      const service = normalizeRole(rec?.service || '');
      const hostGatewayPk = String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim();
      return service === targetService && hostGatewayPk === targetGatewayPk;
    });
    if (!matches.length) return null;
    return matches.sort((a, b) => {
      const aAuthority = String(a?.managedAvailabilityAuthority || '').trim().toLowerCase() === 'gateway' ? 1 : 0;
      const bAuthority = String(b?.managedAvailabilityAuthority || '').trim().toLowerCase() === 'gateway' ? 1 : 0;
      if (aAuthority !== bAuthority) return bAuthority - aAuthority;
      return applianceSeenAt(b) - applianceSeenAt(a);
    })[0] || null;
  }

  function isGatewayAuthoritativeManagedRecord(record) {
    return String(record?.managedAvailabilityAuthority || '').trim().toLowerCase() === 'gateway';
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

  return {
    applianceFreshness,
    applianceSeenAt,
    effectiveApplianceSeenAt,
    buildApplianceRecords,
    findGatewayHostedServiceRecord,
    formatAgeShort,
    formatReleaseMeta,
    isGatewayRecord,
    isNvrRecord,
    managedGatewayPkForRecord,
    mergeGatewayHostedServiceRecord,
    managedServicePkForRecord,
    ownedPkSet,
    partitionApplianceRecords,
    summarizeAppliance,
  };
}
