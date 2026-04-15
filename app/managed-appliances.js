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
    const role = normalizeRole(rec?.role || rec?.nodeType || rec?.type || '') || 'unknown';
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

  function appendSection(applianceList, title, hint, records, renderRecord) {
    if (!records.length) return;
    const section = document.createElement("section");
    section.className = "listSection";

    const header = document.createElement("div");
    header.className = "listSectionHeader";
    header.innerHTML = `<div class="itemTitle">${title}</div><div class="itemMeta">${hint}</div>`;
    section.appendChild(header);

    const body = document.createElement("div");
    body.className = "listSectionBody";
    for (const record of records) {
      body.appendChild(renderRecord(record));
    }
    section.appendChild(body);
    applianceList.appendChild(section);
  }

  function createActionIconButton(label, glyph, onClick, title = label, options = {}) {
    const button = document.createElement('button');
    button.type = 'button';
    const primary = Object.prototype.hasOwnProperty.call(options, 'primary') ? Boolean(options.primary) : glyph === '↗';
    button.className = 'actionIconButton';
    if (primary) button.classList.add('actionIconButton-primary');
    if (options.pending) button.classList.add('actionIconButton-pending');
    if (options.busy) button.classList.add('actionIconButton-busy');
    button.setAttribute('aria-label', label);
    button.title = title;
    button.textContent = glyph;
    if (typeof onClick === 'function') {
      button.onclick = onClick;
    } else {
      button.disabled = true;
    }
    return button;
  }

  function renderApplianceList({
    applianceList,
    identityDevices,
    swarmDevices,
    gatewayInventoryStableAt,
    showActivity,
    setSettingsTab,
    setPairCodeStatus,
    gatewayExtraZonesForPk,
    parseZoneKeyList,
    setGatewayExtraZonesForPk,
    requestGatewayZoneSync,
    setGatewayInstallStatus,
    requestRemoteNvrInstall,
    launchNvrControlPanel,
    openGatewayBasicsModal,
    escapeHtml,
    getManagedServiceActionState = () => null,
    grantedRecords: providedGrantedRecords = [],
    getGrantInventoryForService = () => null,
    requestGatewayGrantAction = null,
    isGrantedRecord = () => false,
  }) {
    if (!applianceList) return;
    while (applianceList.firstChild) applianceList.firstChild.remove();

    const owned = ownedPkSet(identityDevices);
    const recs = buildApplianceRecords(identityDevices, swarmDevices, providedGrantedRecords);
    const { ownedRecords, grantedRecords, discoverableRecords } = partitionApplianceRecords(
      recs,
      owned,
      isGrantedRecord,
    );

    if (ownedRecords.length === 0 && grantedRecords.length === 0 && discoverableRecords.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'item';
      empty.textContent = 'No owned, shared, or discoverable resources found yet.';
      applianceList.appendChild(empty);
      return;
    }

    const renderRecord = (rec) => {
      const ownedRec = owned.has(String(rec?.devicePk || rec?.pk || '').trim())
        || owned.has(String(rec?.hostGatewayPk || rec?.host_gateway_pk || '').trim());
      const info = summarizeAppliance(rec, ownedRec);
      const seenAt = effectiveApplianceSeenAt(rec, recs);
      const hostedNvr = isGatewayRecord(rec) ? findGatewayHostedServiceRecord(info.pk, recs, 'nvr') : null;
      const canSeeHostedServiceDetail = info.owned || isGrantedRecord(rec);
      if (isGatewayRecord(rec) && hostedNvr && info.owned) {
        gatewayInventoryStableAt.set(info.pk, Date.now());
      }

      const item = document.createElement('div');
      item.className = 'item';

      const meta = document.createElement('div');
      const freshness = applianceFreshness(seenAt);
      const last = seenAt ? new Date(seenAt).toLocaleString() : 'n/a';
      const suffix = info.version ? ` (${info.version})` : '';
      const host = info.hostPlatform ? ` • host ${info.hostPlatform}` : '';
      const kind = info.deviceKind ? `type ${info.deviceKind}` : '';
      const releaseMeta = formatReleaseMeta(info.releaseChannel, info.releaseTrack, info.releaseBranch);
      const releaseLine = releaseMeta ? `<div class="itemMeta">release ${escapeHtml(releaseMeta)}</div>` : '';
      const hostGatewayLine = info.hostGatewayPk
        ? `<div class="itemMeta">host gateway ${escapeHtml(info.hostGatewayPk.slice(0, 16))}…</div>`
        : '';
      const hostedServicesLine = canSeeHostedServiceDetail && Array.isArray(info.hostedServices) && info.hostedServices.length > 0
        ? `<div class="itemMeta">hosted services ${escapeHtml(info.hostedServices.map((svc) => String(svc?.service || 'service')).join(', '))}</div>`
        : '';
      const grantInventory = getGrantInventoryForService(hostedNvr || rec);
      const actionState = isNvrRecord(rec) ? getManagedServiceActionState(rec) : null;
      const audienceLabel = info.owned
        ? 'owned by this identity'
        : (isGrantedRecord(rec) ? 'shared with this identity' : 'discovered in zone');
      const scopeLabel = info.owned ? 'Owned' : (isGrantedRecord(rec) ? 'Shared' : 'Discoverable');
      const scopeCss = info.owned
        ? 'resourceScopeOwned'
        : (isGrantedRecord(rec) ? 'resourceScopeShared' : 'resourceScopeDiscoverable');
      const grantedScope = rec?.grantedScope && typeof rec.grantedScope === 'object' ? rec.grantedScope : null;
      const grantedCamerasLine = grantedScope && Array.isArray(grantedScope.viewSources) && grantedScope.viewSources.length > 0
        ? `<div class="itemMeta">granted cameras ${escapeHtml(grantedScope.viewSources.join(', '))}</div>`
        : '';
      meta.innerHTML = `
        <div class="itemTitle">${escapeHtml(info.title)}</div>
        <div class="itemMeta">pk ${escapeHtml(info.pk.slice(0, 16))}…</div>
        <div class="itemMeta">${escapeHtml([kind, `role ${info.role}`, `service ${info.service}${suffix}`, host ? host.replace(/^ • /, '') : ''].filter(Boolean).join(' • '))}</div>
        ${hostGatewayLine}
        ${hostedServicesLine}
        ${grantedCamerasLine}
        ${releaseLine}
        <div class="itemMeta"><span class="freshnessDot ${escapeHtml(freshness.css)}" title="${escapeHtml(last)}"></span>${escapeHtml(freshness.label)} • ${escapeHtml(formatAgeShort(seenAt))}</div>
        <div class="itemMeta">${escapeHtml(audienceLabel)} • updated ${escapeHtml(last)}</div>
        <div class="resourceScope ${escapeHtml(scopeCss)}">${escapeHtml(scopeLabel)}</div>
      `;

      const actions = document.createElement('div');
      actions.className = 'resourceActionBar';

      if (isGatewayRecord(rec)) {
        if (!info.owned) {
          const pair = document.createElement('button');
          pair.type = 'button';
          pair.className = 'actionTextButton';
          pair.textContent = 'Pair Existing Gateway';
          pair.onclick = () => {
            showActivity('settings');
            setSettingsTab('devices');
            setPairCodeStatus('Enter the pairing code shown by the gateway installer utility.');
          };
          actions.appendChild(pair);
        }

        const gatewaySupportsServices = hostedNvr
          || !info.hostPlatform
          || info.hostPlatform === 'linux'
          || info.hostPlatform === 'fcos';
        if (info.owned && typeof openGatewayBasicsModal === 'function') {
          actions.appendChild(createActionIconButton(
            'Gateway settings',
            '⚙',
            () => openGatewayBasicsModal({
              record: rec,
              info,
              freshness,
              seenAt,
              last,
              hostedNvr,
              grantInventory,
              onConfigureZones: async () => {
                const currentExtra = gatewayExtraZonesForPk(info.pk);
                const entered = window.prompt(
                  'Extra gateway zone keys (comma or space separated). Identity zones are always synced automatically.',
                  currentExtra.join(', '),
                );
                if (entered === null) return;
                const extras = parseZoneKeyList(entered);
                setGatewayExtraZonesForPk(info.pk, extras);
                setGatewayInstallStatus('Submitting gateway zone sync request...');
                const submitted = await requestGatewayZoneSync(rec, extras);
                if (submitted?.requestId) {
                  setGatewayInstallStatus(`Gateway zone sync requested (${submitted.requestId.slice(0, 12)}...).`, false);
                } else {
                  setGatewayInstallStatus('Gateway zone sync request submitted.', false);
                }
              },
              onOpenNvr: hostedNvr
                ? async () => {
                    await launchNvrControlPanel(hostedNvr);
                  }
                : null,
              onInstallNvr: (!hostedNvr && gatewaySupportsServices)
                ? async () => {
                    setGatewayInstallStatus('Submitting NVR install request to gateway...');
                    const submitted = await requestRemoteNvrInstall(rec);
                    const until = submitted?.enrollment?.expiresAt
                      ? new Date(submitted.enrollment.expiresAt).toLocaleTimeString()
                      : '';
                    if (submitted?.requestId) {
                      setGatewayInstallStatus(
                        until
                          ? `NVR install requested (request ${submitted.requestId.slice(0, 12)}...). Pair claim armed until ${until}.`
                          : `NVR install requested (request ${submitted.requestId.slice(0, 12)}...).`,
                        false,
                      );
                    } else {
                      setGatewayInstallStatus('NVR install request submitted.', false);
                    }
                  }
                : null,
            }),
            'Gateway basics',
          ));
        }
        if (!gatewaySupportsServices) {
          const unsupported = document.createElement('div');
          unsupported.className = 'itemMeta';
          unsupported.textContent = 'Gateway-hosted services are currently supported on Linux hosts only.';
          item.appendChild(unsupported);
        }
      }

      if (isNvrRecord(rec)) {
        const canUseService = info.owned || isGrantedRecord(rec);
        const authoritativeRecord = info.hostGatewayPk
          ? findGatewayHostedServiceRecord(info.hostGatewayPk, recs, 'nvr')
          : null;
        const launchRecord = isGatewayAuthoritativeManagedRecord(rec)
          ? rec
          : (isGatewayAuthoritativeManagedRecord(authoritativeRecord) ? authoritativeRecord : null);
        const gatewayLaunchReady = Boolean(launchRecord);
        const freshEnough = freshness.label === 'live' || freshness.label === 'recent';
        const pendingLaunch = freshness.label === 'stale' || freshness.label === 'offline' || freshness.label === 'unknown';
        const isBusy = actionState?.state === 'resolving' || actionState?.state === 'launching';
        const isErrorState = actionState?.state === 'error';
        const open = createActionIconButton(
          'Open Security Cameras',
          '↗',
          null,
          'Open Security Cameras',
          {
            primary: canUseService && gatewayLaunchReady && freshEnough && !isBusy && !isErrorState,
            pending: canUseService && gatewayLaunchReady && (pendingLaunch || isErrorState) && !isBusy,
            busy: isBusy,
          },
        );
        if (!canUseService) {
          open.disabled = true;
          open.title = 'Only owned or granted services can be launched.';
        } else if (!gatewayLaunchReady) {
          open.disabled = true;
          open.title = 'Gateway-hosted Security Cameras configuration is not published yet.';
        } else if (isBusy) {
          open.disabled = true;
          open.title = String(actionState?.message || 'Resolving Security Cameras availability...');
        } else {
          if (isErrorState) {
            open.title = String(actionState?.message || 'Security Cameras is not available right now.');
          } else if (pendingLaunch) {
            open.title = 'Resolve current Security Cameras availability before launch.';
          }
          open.onclick = () => {
            launchNvrControlPanel(launchRecord || rec).catch((err) => {
              setGatewayInstallStatus(`NVR launch failed: ${String(err?.message || err)}`, true);
            });
          };
        }
        actions.appendChild(open);

        actions.appendChild(createActionIconButton(
          'Security Cameras settings',
          '⚙',
          canUseService && gatewayLaunchReady && !isBusy
            ? () => {
                launchNvrControlPanel(launchRecord || rec, { activity: 'settings' }).catch((err) => {
                  setGatewayInstallStatus(`NVR settings failed: ${String(err?.message || err)}`, true);
                });
              }
            : null,
          isBusy
            ? String(actionState?.message || 'Resolving Security Cameras availability...')
            : 'Open Security Cameras settings',
        ));
      }

      item.appendChild(meta);
      if (actions.childNodes.length > 0) item.appendChild(actions);

      if (actionState?.message) {
        const stateLine = document.createElement('div');
        stateLine.className = 'resourceActionState';
        stateLine.dataset.state = String(actionState.state || 'idle');
        stateLine.textContent = String(actionState.message || '').trim();
        item.appendChild(stateLine);
      }

      if (grantInventory && info.owned && Array.isArray(grantInventory.grants) && grantInventory.grants.length > 0) {
        const sharePanel = document.createElement('div');
        sharePanel.className = 'resourceGrantSummary';

        const title = document.createElement('div');
        title.className = 'resourceGrantSummaryTitle';
        title.textContent = 'Current shared access';
        sharePanel.appendChild(title);

        for (const grant of grantInventory.grants) {
          const row = document.createElement('div');
          row.className = 'resourceGrantRow';

          const text = document.createElement('span');
          const viewSources = Array.isArray(grant?.viewSources) ? grant.viewSources.join(', ') : '';
          const controlSources = Array.isArray(grant?.controlSources) ? grant.controlSources.join(', ') : '';
          text.textContent = `${String(grant?.granteeIdentityId || '').trim() || 'unknown identity'} • view ${viewSources || 'none'}${controlSources ? ` • ptz ${controlSources}` : ''}`;
          row.appendChild(text);
          sharePanel.appendChild(row);
        }

        item.appendChild(sharePanel);
      }
      return item;
    };

    appendSection(
      applianceList,
      "Owned",
      "Gateways and services owned by this identity.",
      ownedRecords,
      renderRecord,
    );
    appendSection(
      applianceList,
      "Shared With You",
      "Access granted by another owner.",
      grantedRecords,
      renderRecord,
    );
    appendSection(
      applianceList,
      "Discoverable / Pairable",
      "Generic zone discovery only. Pair or grant access before using hosted services.",
      discoverableRecords,
      renderRecord,
    );
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
    renderApplianceList,
    summarizeAppliance,
  };
}
