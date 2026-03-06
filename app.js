import { IdentityClient } from './identity/client.js';

const panePathEl = document.getElementById('panePath');

const connWrap = document.getElementById('connWrap');
const connDot = document.getElementById('connDot');
const connStateText = document.getElementById('connStateText');
const connLog = document.getElementById('connLog');
const connPopover = document.getElementById('connPopover');
const popRelay = document.getElementById('popRelay');
const popDaemon = document.getElementById('popDaemon');
const popSwarm = document.getElementById('popSwarm');
const popSwarmCache = document.getElementById('popSwarmCache');

const btnMenu = document.getElementById('btnMenu');
const drawer = document.getElementById('drawer');
const drawerBackdrop = document.getElementById('drawerBackdrop');
const btnDrawerClose = document.getElementById('btnDrawerClose');

const btnBell = document.getElementById('btnBell');
const notifMenu = document.getElementById('notifMenu');
const notifList = document.getElementById('notifList');
const btnNotifClear = document.getElementById('btnNotifClear');

const viewHome = document.getElementById('viewHome');
const viewSettings = document.getElementById('viewSettings');
const viewOnboard = document.getElementById('viewOnboard');

const tabButtons = Array.from(viewSettings.querySelectorAll('.tab'));
const tabPanes = {
  profile: document.getElementById('tab_profile'),
  devices: document.getElementById('tab_devices'),
  appliances: document.getElementById('tab_appliances'),
  peers: document.getElementById('tab_peers'),
  pairing: document.getElementById('tab_pairing'),
  apps: document.getElementById('tab_apps'),
  identity: document.getElementById('tab_identity'),
};

const profileName = document.getElementById('profileName');
const profileAbout = document.getElementById('profileAbout');
const btnSaveProfile = document.getElementById('btnSaveProfile');

const deviceDid = document.getElementById('deviceDid');
const deviceLabel = document.getElementById('deviceLabel');
const btnSaveDeviceLabel = document.getElementById('btnSaveDeviceLabel');
const deviceList = document.getElementById('deviceList');
const blockedList = document.getElementById('blockedList');

const pairingList = document.getElementById('pairingList');
const pairingEmpty = document.getElementById('pairingEmpty');
const pairCodeInput = document.getElementById('pairCodeInput');
const btnClaimPairCode = document.getElementById('btnClaimPairCode');
const pairCodeStatus = document.getElementById('pairCodeStatus');

const identityLabelEl = document.getElementById('identityLabel');
const identityIdEl = document.getElementById('identityId');
const identityLinkedEl = document.getElementById('identityLinked');

// Optional app repo settings
const appRepoInput = document.getElementById('appRepoInput');
const btnAddAppRepo = document.getElementById('btnAddAppRepo');
const appRepoStatus = document.getElementById('appRepoStatus');
const appCapabilityList = document.getElementById('appCapabilityList');
const homeAppsList = document.getElementById('homeAppsList');

const gatewayInstallPlatform = document.getElementById('gatewayInstallPlatform');
const gatewayInstallIncludeNvr = document.getElementById('gatewayInstallIncludeNvr');
const gatewayInstallDevSource = document.getElementById('gatewayInstallDevSource');
const gatewayInstallDevBranch = document.getElementById('gatewayInstallDevBranch');
const gatewayInstallDevBranchRow = document.getElementById('gatewayInstallDevBranchRow');
const btnGatewayInstallCopy = document.getElementById('btnGatewayInstallCopy');
const btnGatewayInstallOpen = document.getElementById('btnGatewayInstallOpen');
const gatewayInstallStatus = document.getElementById('gatewayInstallStatus');
const gatewayInstallDetectedPlatform = document.getElementById('gatewayInstallDetectedPlatform');
const gatewayInstallHint = document.getElementById('gatewayInstallHint');
const gatewayInstallCommandPreview = document.getElementById('gatewayInstallCommandPreview');
const applianceList = document.getElementById('applianceList');

const joinDeviceLabelEl = document.getElementById('joinDeviceLabel');

const deviceDidSummary = document.getElementById('deviceDidSummary');
const deviceSecuritySummary = document.getElementById('deviceSecuritySummary');
const identityLinkedSummary = document.getElementById('identityLinkedSummary');

// Peers UI
const zoneNameInput = document.getElementById('zoneName');
const btnCreateZone = document.getElementById('btnCreateZone');
const zonesList = document.getElementById('zonesList');
const zoneLink = document.getElementById('zoneLink');
const btnCopyZoneLink = document.getElementById('btnCopyZoneLink');
const zoneJoinKey = document.getElementById('zoneJoinKey');
const btnJoinZone = document.getElementById('btnJoinZone');
const peersCount = document.getElementById('peersCount');
const peersList = document.getElementById('peersList');

// Onboarding elements
const obStepDevice = document.getElementById('obStepDevice');
const obStepIdentity = document.getElementById('obStepIdentity');
const btnObDeviceContinue = document.getElementById('btnObDeviceContinue');
const obDeviceLabel = document.getElementById('obDeviceLabel');
const obIdentityLabel = document.getElementById('obIdentityLabel');
const obPairCodeWrap = document.getElementById('obPairCodeWrap');
const obPairCode = document.getElementById('obPairCode');
const btnObIdentityContinue = document.getElementById('btnObIdentityContinue');
const existingInfo = document.getElementById('existingInfo');
const obModeTabs = Array.from(obStepIdentity.querySelectorAll('.tab'));

const btnSecWebAuthn = document.getElementById('btnSecWebAuthn');
const btnSecSkip = document.getElementById('btnSecSkip');
const obDeviceStatus = document.getElementById('obDeviceStatus');

const SWARM_ICE_SERVERS = [
  { urls: 'stun:stun.l.google.com:19302' },
  // TODO: Add TURN here for NATs that block P2P.
  // { urls: 'turn:turn.example.com:3478', username: 'user', credential: 'pass' },
];

let relayState = 'offline';
let daemonState = 'unknown';
let swarmState = 'offline';
let connDerived = 'offline';
const connStateLog = []; // newest first

let lastDeviceState = null;
let lastIdentity = null;
let lastDirectory = [];
let lastZones = [];
let activeZoneKey = '';
let pendingZoneNav = false;
let swarm = null;
let clientReady = false;
let swarmBootRequested = false;

const APPS_REPOS_KEY = 'constitute.apps.repos';
const APPS_ENABLED_KEY = 'constitute.apps.enabled';
const DEFAULT_APP_REPOS = ['https://github.com/Aux0x7F/constitute-nvr-ui'];
const ROLE_APP_REPO_MAP = Object.freeze({
  nvr: ['https://github.com/Aux0x7F/constitute-nvr-ui'],
});
const SERVICE_APP_REPO_MAP = Object.freeze({
  nvr: ['https://github.com/Aux0x7F/constitute-nvr-ui'],
});
let appRepoCatalog = [];
let appEnabledIds = new Set();
let appLaunchHints = new Map();
window.__constituteEnabledApps = [];

const GATEWAY_INSTALL_TARGETS = Object.freeze({
  'linux-image': {
    label: 'Linux image (FCOS metal)',
    supportsServices: true,
  },
  'windows-service': {
    label: 'Windows service',
    supportsServices: false,
  },
});

function detectOperatorPlatform() {
  const uaDataPlatform = String(navigator?.userAgentData?.platform || '').toLowerCase();
  const platform = String(navigator?.platform || '').toLowerCase();
  const ua = String(navigator?.userAgent || '').toLowerCase();
  const sample = `${uaDataPlatform} ${platform} ${ua}`;
  if (sample.includes('win')) return 'windows';
  if (sample.includes('mac')) return 'mac';
  if (sample.includes('linux') || sample.includes('x11')) return 'linux';
  return 'unknown';
}

const OPERATOR_PLATFORM = detectOperatorPlatform();

const GATEWAY_INSTALLERS = Object.freeze({
  'linux-image': Object.freeze({
    windows: {
      commandBase: 'powershell -NoProfile -ExecutionPolicy Bypass -Command "$sb=[ScriptBlock]::Create((irm https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/windows/prepare-auto-update-image.ps1 -UseBasicParsing).Content); & $sb @args"',
      scriptUrl: 'https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/windows/prepare-auto-update-image.ps1',
      downloadName: 'constitute-gateway-linux-image.ps1',
      note: 'Linux image target supports gateway-hosted services (including NVR).',
    },
    linux: {
      commandBase: 'curl -fsSL https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/fcos/prepare-auto-update-image.sh | bash',
      scriptUrl: 'https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/fcos/prepare-auto-update-image.sh',
      downloadName: 'constitute-gateway-linux-image.sh',
      note: 'Linux image target supports gateway-hosted services (including NVR).',
    },
    mac: {
      commandBase: 'curl -fsSL https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/fcos/prepare-auto-update-image.sh | bash',
      scriptUrl: 'https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/fcos/prepare-auto-update-image.sh',
      downloadName: 'constitute-gateway-linux-image.sh',
      note: 'Linux image target supports gateway-hosted services (including NVR).',
    },
  }),
  'windows-service': Object.freeze({
    windows: {
      commandBase: 'powershell -NoProfile -ExecutionPolicy Bypass -Command "$sb=[ScriptBlock]::Create((irm https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/windows/install-latest.ps1 -UseBasicParsing).Content); & $sb @args"',
      scriptUrl: 'https://raw.githubusercontent.com/Aux0x7F/constitute-gateway/main/scripts/windows/install-latest.ps1',
      downloadName: 'constitute-gateway-windows-service.ps1',
      note: 'Windows service target does not support gateway-hosted NVR service installation.',
    },
  }),
});

const NVR_INSTALLERS = Object.freeze({
  linux: {
    commandBase: "curl -fsSL https://raw.githubusercontent.com/Aux0x7F/constitute-nvr/main/scripts/linux/install-latest.sh | bash -s --",
    scriptUrl: "https://raw.githubusercontent.com/Aux0x7F/constitute-nvr/main/scripts/linux/install-latest.sh",
  },
});

let lastSwarmDevices = [];
const pendingGatewayServiceInstalls = new Map();

class SwarmTransport {
  constructor({ client, onState }) {
    this.client = client;
    this.onState = onState || (() => {});
    this.localPk = '';
    this.peers = new Map(); // pk -> RTCPeerConnection
    this.channels = new Map(); // pk -> RTCDataChannel
    this.connecting = new Set();
    this.backoff = new Map(); // pk -> nextAt
    this.maxPeers = 6;
    this.lastKnown = [];
    this.lastPeerKey = '';
    this.success = new Map(); // pk -> lastOkMs
    this.identityCache = [];
    this.deviceCache = [];
    this.identityCacheTs = 0;
    this.deviceCacheTs = 0;
    this.swarmSeen = new Map(); // pk -> lastSeenMs
    this.cacheTtlMs = 24 * 60 * 60 * 1000;
  }

  setLocalPk(pk) {
    this.localPk = String(pk || '').trim();
  }

  async setPeers(list) {
    if (typeof RTCPeerConnection === 'undefined') {
      this.onState('error');
      return;
    }
    const peers = this._selectPeers(list);
    const key = peers.slice().sort().join(',');
    if (key === this.lastPeerKey) {
      this._updateState();
      return;
    }
    this.lastPeerKey = key;
    console.log('[swarm] peers selected', peers);
    this.lastKnown = peers;
    this._persistLastKnown(peers);
    for (const pk of peers) {
      if (!this.peers.has(pk) && !this.connecting.has(pk) && this._canDial(pk)) {
        this.connecting.add(pk);
        console.log('[swarm] dialing', pk);
        this._connectTo(pk).catch(() => {});
      }
    }
    this._updateState();
  }

  _updateState() {
    const openCount = Array.from(this.channels.values()).filter(c => c && c.readyState === 'open').length;
    if (openCount > 0) this.onState('open');
    else if (this.connecting.size > 0) this.onState('connecting');
    else this.onState('offline');
  }

  async _connectTo(pk) {
    const pc = new RTCPeerConnection({ iceServers: SWARM_ICE_SERVERS });
    this.peers.set(pk, pc);

    pc.onicecandidate = (e) => {
      if (!e.candidate) return;
      console.log('[swarm] ice', pk);
      this._sendSignal(pk, 'ice', e.candidate).catch(() => {});
    };

    pc.onconnectionstatechange = () => {
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed' || pc.connectionState === 'disconnected') {
        this._cleanup(pk);
      }
      this._updateState();
    };

    const dc = pc.createDataChannel('swarm');
    this._attachChannel(pk, dc);

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    console.log('[swarm] offer', pk);
    await this._sendSignal(pk, 'offer', offer);
  }

  async _acceptFrom(pk, offer) {
    let pc = this.peers.get(pk);
    if (!pc) {
      pc = new RTCPeerConnection({ iceServers: SWARM_ICE_SERVERS });
      this.peers.set(pk, pc);
      pc.onicecandidate = (e) => {
        if (!e.candidate) return;
        this._sendSignal(pk, 'ice', e.candidate).catch(() => {});
      };
      pc.onconnectionstatechange = () => {
        if (pc.connectionState === 'failed' || pc.connectionState === 'closed' || pc.connectionState === 'disconnected') {
          this._cleanup(pk);
        }
        this._updateState();
      };
      pc.ondatachannel = (e) => this._attachChannel(pk, e.channel);
    }
    await pc.setRemoteDescription(new RTCSessionDescription(offer));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    console.log('[swarm] answer', pk);
    await this._sendSignal(pk, 'answer', answer);
  }

  async _handleAnswer(pk, answer) {
    const pc = this.peers.get(pk);
    if (!pc) return;
    await pc.setRemoteDescription(new RTCSessionDescription(answer));
  }

  async _handleIce(pk, cand) {
    const pc = this.peers.get(pk);
    if (!pc) return;
    try { await pc.addIceCandidate(new RTCIceCandidate(cand)); } catch {}
  }

  _attachChannel(pk, dc) {
    this.channels.set(pk, dc);
    dc.onopen = async () => {
      this.connecting.delete(pk);
      this.backoff.delete(pk);
      this._recordSuccess(pk);
      this._markSeen(pk);
      console.log('[swarm] channel open', pk);
      await this._sendRecords(dc);
      this._updateState();
    };
    dc.onclose = () => {
      this._cleanup(pk);
      this._updateState();
    };
    dc.onmessage = (e) => this._onChannelMessage(pk, e.data);
  }

  async _sendRecords(dc) {
    const irec = await this.client.call('swarm.identity.record', {}, { timeoutMs: 20000 }).catch(() => null);
    const drec = await this.client.call('swarm.device.record', {}, { timeoutMs: 20000 }).catch(() => null);
    dc.send(JSON.stringify({ type: 'swarm_records', identityRecord: irec, deviceRecord: drec }));
  }

  async _onChannelMessage(pk, data) {
    let msg;
    try { msg = JSON.parse(String(data || '')); } catch { return; }
    this._markSeen(pk);
    console.log('[swarm] msg', pk, msg?.type || 'unknown');
    if (msg.type === 'swarm_records') {
      if (msg.identityRecord) await this.client.call('swarm.identity.put', { record: msg.identityRecord }, { timeoutMs: 20000 }).catch(() => {});
      if (msg.deviceRecord) await this.client.call('swarm.device.put', { record: msg.deviceRecord }, { timeoutMs: 20000 }).catch(() => {});
      return;
    }
    if (msg.type === 'swarm_request') {
      await this._sendRecords(this.channels.get(pk));
      return;
    }
  }

  async _sendSignal(toPk, signalType, data) {
    return await this.client.call('swarm.signal.send', { toPk, signalType, data }, { timeoutMs: 20000 });
  }

  async onSignal(evt) {
    const from = String(evt?.from || '').trim();
    if (!from || from === this.localPk) return;
    const t = evt?.signalType;
    console.log('[swarm] signal', t, from);
    if (t === 'offer') return await this._acceptFrom(from, evt.data);
    if (t === 'answer') return await this._handleAnswer(from, evt.data);
    if (t === 'ice') return await this._handleIce(from, evt.data);
  }

  _cleanup(pk) {
    this.connecting.delete(pk);
    this._backoff(pk);
    const ch = this.channels.get(pk);
    if (ch) try { ch.close(); } catch {}
    this.channels.delete(pk);
    const pc = this.peers.get(pk);
    if (pc) try { pc.close(); } catch {}
    this.peers.delete(pk);
  }

  _selectPeers(list) {
    const arr = (Array.isArray(list) ? list : [])
      .map(r => String(r?.devicePk || '').trim())
      .filter(Boolean)
      .filter(pk => pk !== this.localPk);
    if (arr.length <= this.maxPeers) return arr;
    const scored = arr.map(pk => ({
      pk,
      score: this.success.get(pk) || 0,
    }));
    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, Math.min(this.maxPeers, Math.max(2, Math.floor(this.maxPeers / 2)))).map(x => x.pk);
    const rest = scored.slice(top.length).map(x => x.pk);
    const shuffled = rest.sort(() => Math.random() - 0.5);
    const pick = top.concat(shuffled.slice(0, this.maxPeers - top.length));
    return pick;
  }

  _backoff(pk) {
    const now = Date.now();
    const next = now + 30_000 + Math.floor(Math.random() * 20_000);
    this.backoff.set(pk, next);
  }

  _canDial(pk) {
    const nextAt = this.backoff.get(pk) || 0;
    return Date.now() >= nextAt;
  }

  _persistLastKnown(peers) {
    try { localStorage.setItem('swarm.lastPeers', JSON.stringify(peers)); } catch {}
  }

  _recordSuccess(pk) {
    this.success.set(pk, Date.now());
    this._persistSuccess();
  }

  _persistSuccess() {
    try {
      const arr = Array.from(this.success.entries());
      localStorage.setItem('swarm.peerSuccess', JSON.stringify(arr));
    } catch {}
  }

  loadSuccess() {
    try {
      const raw = localStorage.getItem('swarm.peerSuccess');
      const arr = JSON.parse(raw || '[]');
      if (Array.isArray(arr)) {
        this.success = new Map(arr.filter(x => Array.isArray(x) && x.length === 2));
      }
    } catch {}
  }

  cacheIdentityRecords(list) {
    const arr = Array.isArray(list) ? list : [];
    this.identityCache = arr;
    this.identityCacheTs = Date.now();
    try {
      localStorage.setItem('swarm.identityCache', JSON.stringify({ ts: this.identityCacheTs, records: arr }));
    } catch {}
  }

  loadIdentityCache() {
    try {
      const raw = localStorage.getItem('swarm.identityCache');
      const obj = JSON.parse(raw || '{}');
      const arr = Array.isArray(obj?.records) ? obj.records : [];
      const ts = Number(obj?.ts || 0);
      if (ts && Date.now() - ts < this.cacheTtlMs) {
        this.identityCache = arr;
        this.identityCacheTs = ts;
      }
    } catch {}
    return this.identityCache;
  }

  cacheDeviceRecords(list) {
    const arr = Array.isArray(list) ? list : [];
    this.deviceCache = arr;
    this.deviceCacheTs = Date.now();
    try {
      localStorage.setItem('swarm.deviceCache', JSON.stringify({ ts: this.deviceCacheTs, records: arr }));
    } catch {}
  }

  loadDeviceCache() {
    try {
      const raw = localStorage.getItem('swarm.deviceCache');
      const obj = JSON.parse(raw || '{}');
      const arr = Array.isArray(obj?.records) ? obj.records : [];
      const ts = Number(obj?.ts || 0);
      if (ts && Date.now() - ts < this.cacheTtlMs) {
        this.deviceCache = arr;
        this.deviceCacheTs = ts;
      }
    } catch {}
    return this.deviceCache;
  }

  loadLastKnown() {
    try {
      const raw = localStorage.getItem('swarm.lastPeers');
      const arr = JSON.parse(raw || '[]');
      if (Array.isArray(arr)) this.lastKnown = arr;
    } catch {}
    return this.lastKnown;
  }

  getFallbackPeers() {
    const preferred = this.lastKnown && this.lastKnown.length > 0
      ? this.lastKnown
      : (this.deviceCache || []).map(d => String(d?.devicePk || '').trim()).filter(Boolean);
    return preferred;
  }

  _markSeen(pk) {
    const k = String(pk || '').trim();
    if (!k) return;
    this.swarmSeen.set(k, Date.now());
  }

  getSwarmSeen(pk) {
    return this.swarmSeen.get(pk) || 0;
  }

  getCacheAges() {
    return {
      identity: this.identityCacheTs ? Date.now() - this.identityCacheTs : 0,
      device: this.deviceCacheTs ? Date.now() - this.deviceCacheTs : 0,
    };
  }
}

function _deriveConnState() {
  const r = relayState;
  const d = daemonState;
  const s = swarmState;

  if (d !== 'online') {
    if (r === 'connecting') return 'connecting';
    return 'disconnected';
  }

  if (r === 'open' && s === 'open') return 'connected';
  if (r === 'open' && (s === 'offline' || s === 'error' || s === 'disabled')) return 'degraded';
  if (s === 'open' && (r === 'offline' || r === 'error' || r === 'closed')) return 'swarm-only';
  if (r === 'connecting' || s === 'connecting') return 'connecting';
  if (r === 'error' || r === 'closed') return 'error';
  return 'disconnected';
}

function _deriveConnLabel() {
  const r = relayState;
  const d = daemonState;
  const s = swarmState;

  if (d !== 'online') {
    if (r === 'connecting') return 'Connecting to nostr...';
    return 'Disconnected';
  }

  if (r === 'connecting') return 'Connecting to nostr...';
  if (r === 'open' && s === 'connecting') return 'Connecting to swarm...';
  if (r === 'open' && s === 'open') return 'Fully connected';
  if (r === 'open' && (s === 'offline' || s === 'error' || s === 'disabled')) return 'Partially connected';
  if (s === 'open' && (r === 'offline' || r === 'error' || r === 'closed')) return 'Swarm only';
  if (r === 'error' || r === 'closed') return 'Disconnected';
  return 'Disconnected';
}

function _pushConnLog(reason = '') {
  const state = _deriveConnState();
  const label = _deriveConnLabel();
  connStateText.textContent = label;
  if (state === connDerived && connStateLog.length > 0) return;

  connDerived = state;

  connStateLog.unshift({ ts: Date.now(), state, reason: String(reason || '') });
  while (connStateLog.length > 25) connStateLog.pop();

  renderConnLog();
}

function renderConnLog() {
  connLog.innerHTML = '';
  for (const e of connStateLog) {
    const row = document.createElement('div');
    row.className = 'connLogItem';
    const t = new Date(e.ts);
    const hh = String(t.getHours()).padStart(2, '0');
    const mm = String(t.getMinutes()).padStart(2, '0');
    const ss = String(t.getSeconds()).padStart(2, '0');
    row.innerHTML = `
      <span>${hh}:${mm}:${ss}</span>
      <span class="connLogState">${escapeHtml(e.state)}</span>
    `;
    connLog.appendChild(row);
  }
}

function setRelayState(s, reason = '') {
  relayState = String(s || 'offline');
  popRelay.textContent = relayState;

  _setConnDot();
  _pushConnLog(reason);
}

function formatAge(ms) {
  if (!ms || ms < 0) return 'n/a';
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h`;
  const d = Math.floor(h / 24);
  return `${d}d`;
}

function setDaemonState(s, reason = '') {
  daemonState = String(s || 'unknown');
  popDaemon.textContent = daemonState;
  _pushConnLog(reason);
}

function setSwarmState(s, reason = '') {
  swarmState = String(s || 'offline');
  if (popSwarm) popSwarm.textContent = swarmState;
  if (popSwarmCache && swarm) {
    const ages = swarm.getCacheAges();
    const oldest = Math.max(ages.identity || 0, ages.device || 0);
    popSwarmCache.textContent = oldest ? `${formatAge(oldest)} old` : 'n/a';
  }
  _setConnDot();
  _pushConnLog(reason);
}

function _setConnDot() {
  connDot.classList.remove('conn-off', 'conn-open', 'conn-err', 'conn-conn');
  if (relayState === 'open' || swarmState === 'open') connDot.classList.add('conn-open');
  else if (relayState === 'connecting' || swarmState === 'connecting') connDot.classList.add('conn-conn');
  else if (relayState === 'error' || relayState === 'closed') connDot.classList.add('conn-err');
  else connDot.classList.add('conn-off');
}

function showConnPopover() { connPopover.classList.remove('hidden'); }
function hideConnPopover() { connPopover.classList.add('hidden'); }
connWrap.addEventListener('mouseenter', showConnPopover);
connWrap.addEventListener('mouseleave', hideConnPopover);
connWrap.addEventListener('focusin', showConnPopover);
connWrap.addEventListener('focusout', hideConnPopover);

// drawer
function openDrawer() {
  drawer.classList.remove('hidden');
  drawerBackdrop.classList.remove('hidden');
}
function closeDrawer() {
  drawer.classList.add('hidden');
  drawerBackdrop.classList.add('hidden');
}
btnMenu.addEventListener('click', openDrawer);
btnDrawerClose.addEventListener('click', closeDrawer);
drawerBackdrop.addEventListener('click', closeDrawer);

// notifications
function toggleNotifMenu() {
  notifMenu.classList.toggle('hidden');
}
btnBell.addEventListener('click', toggleNotifMenu);
document.addEventListener('click', (e) => {
  const t = e.target;
  if (!notifMenu.contains(t) && !btnBell.contains(t)) notifMenu.classList.add('hidden');
});

// activities
function showActivity(name) {
  viewHome.classList.toggle('hidden', name !== 'home');
  viewSettings.classList.toggle('hidden', name !== 'settings');
  viewOnboard.classList.toggle('hidden', name !== 'onboarding');
  panePathEl.textContent = name === 'home' ? '' : name;
}

function setSettingsTab(name) {
  for (const b of tabButtons) b.classList.toggle('active', b.dataset.tab === name);
  for (const [k, el] of Object.entries(tabPanes)) el.classList.toggle('hidden', k !== name);
}


function setAppStatus(msg, error = false) {
  if (!appRepoStatus) return;
  appRepoStatus.textContent = String(msg || '');
  appRepoStatus.classList.toggle('warn', !!error);
}

function setPairCodeStatus(msg, error = false) {
  if (!pairCodeStatus) return;
  pairCodeStatus.textContent = String(msg || '');
  pairCodeStatus.classList.toggle('warn', !!error);
}

function setGatewayInstallStatus(msg, error = false) {
  if (!gatewayInstallStatus) return;
  gatewayInstallStatus.textContent = String(msg || '');
  gatewayInstallStatus.classList.toggle('warn', !!error);
}

function setGatewayInstallHint(msg, error = false) {
  if (!gatewayInstallHint) return;
  gatewayInstallHint.textContent = String(msg || '');
  gatewayInstallHint.classList.toggle('warn', !!error);
}

function setGatewayInstallCommandPreview(msg = '') {
  if (!gatewayInstallCommandPreview) return;
  gatewayInstallCommandPreview.textContent = String(msg || '');
}

function summarizeInstallDetail(detail) {
  const raw = String(detail || '').trim();
  if (!raw) return '';
  return raw.length > 240 ? `${raw.slice(0, 240)}...` : raw;
}

function handleGatewayServiceInstallStatusEvent(evt) {
  const requestId = String(evt?.requestId || '').trim();
  const status = String(evt?.status || '').trim().toLowerCase();
  const gatewayPk = String(evt?.gatewayPk || '').trim();
  const reason = String(evt?.reason || '').trim();
  const detail = summarizeInstallDetail(evt?.detail || '');

  const meta = pendingGatewayServiceInstalls.get(requestId) || null;
  const target = meta?.gatewayPk || gatewayPk || 'gateway';

  const base = `NVR install ${status || 'update'} for ${target.slice(0, 12)}...`;
  const reasonPart = reason ? ` (${reason})` : '';
  const detailPart = detail ? ` — ${detail}` : '';

  const isError = status === 'failed' || status === 'rejected';
  setGatewayInstallStatus(`${base}${reasonPart}${detailPart}`, isError);

  if (status === 'complete' || status === 'failed' || status === 'rejected') {
    if (requestId) pendingGatewayServiceInstalls.delete(requestId);
  }

  if (status === 'complete') {
    refreshAll().catch(() => {});
  }
}

function operatorPlatformLabel() {
  if (OPERATOR_PLATFORM === 'windows') return 'Windows';
  if (OPERATOR_PLATFORM === 'linux') return 'Linux';
  if (OPERATOR_PLATFORM === 'mac') return 'macOS';
  return 'Unknown';
}

function selectedGatewayInstallTarget() {
  const raw = String(gatewayInstallPlatform?.value || 'linux-image').trim().toLowerCase();
  return raw === 'windows-service' ? 'windows-service' : 'linux-image';
}

function preferredGatewayInstallTarget() {
  return 'linux-image';
}

function currentGatewayTargetInfo() {
  return GATEWAY_INSTALL_TARGETS[selectedGatewayInstallTarget()] || GATEWAY_INSTALL_TARGETS['linux-image'];
}

function targetSupportsServices(target = selectedGatewayInstallTarget()) {
  return Boolean(GATEWAY_INSTALL_TARGETS[target]?.supportsServices);
}

function currentGatewayInstaller() {
  const target = selectedGatewayInstallTarget();
  const byTarget = GATEWAY_INSTALLERS[target];
  if (!byTarget) return null;
  return byTarget[OPERATOR_PLATFORM] || null;
}

function currentNvrInstaller() {
  return NVR_INSTALLERS.linux;
}

function gatewayInstallIncludesNvr() {
  return Boolean(gatewayInstallIncludeNvr?.checked);
}

function gatewayInstallUsesDevSource() {
  const target = selectedGatewayInstallTarget();
  if (target !== 'linux-image') return false;
  return Boolean(gatewayInstallDevSource?.checked);
}

function gatewayInstallDevBranchValue() {
  const raw = String(gatewayInstallDevBranch?.value || 'main').trim();
  const safe = raw.replace(/[^a-zA-Z0-9._\/-]/g, '');
  return safe || 'main';
}

function shellSingleQuote(value) {
  const v = String(value ?? '');
  return `'${v.replace(/'/g, `'"'"'`)}'`;
}

function collectAuthorizedIdentityDevicePks() {
  const authorized = new Set();
  const ownPk = String(lastDeviceState?.pk || '').trim();
  if (ownPk) authorized.add(ownPk);
  const identityDevices = Array.isArray(lastIdentity?.devices) ? lastIdentity.devices : [];
  for (const d of identityDevices) {
    const pk = String(d?.pk || d?.devicePk || '').trim();
    if (pk) authorized.add(pk);
  }
  return Array.from(authorized.values());
}

async function prepareInstallEnrollment(target) {
  const identityLabel = String(lastIdentity?.label || '').trim();
  if (!identityLabel) throw new Error('link an identity before preparing install enrollment');

  const enrollment = await client.call('pairing.prepareInstall', {
    autoApprove: true,
    ttlMs: 6 * 60 * 1000,
    target: String(target || 'gateway'),
  }, { timeoutMs: 20000 });

  const pairCode = String(enrollment?.code || '').trim();
  const pairCodeHash = String(enrollment?.codeHash || '').trim();
  const pairIdentity = String(enrollment?.identityLabel || identityLabel).trim();
  if (!pairIdentity || !pairCode || !pairCodeHash) {
    throw new Error('invalid install enrollment payload');
  }

  return {
    identityLabel: pairIdentity,
    code: pairCode,
    codeHash: pairCodeHash,
    expiresAt: Number(enrollment?.expiresAt || 0),
  };
}

function buildNvrInstallCommand({ identityId, enrollment, zoneKeys = [], swarmPeers = [], publicWsUrl = '' }) {
  const installer = currentNvrInstaller();
  if (!installer?.commandBase) throw new Error('no nvr installer configured');

  const parts = [
    installer.commandBase,
    '--non-interactive',
    '--identity-id', shellSingleQuote(identityId),
    '--allow-unsigned-hello-mvp',
    '--pair-identity', shellSingleQuote(enrollment.identityLabel),
    '--pair-code', shellSingleQuote(enrollment.code),
    '--pair-code-hash', shellSingleQuote(enrollment.codeHash),
    '--enable-reolink-autoprovision',
  ];

  const ws = String(publicWsUrl || '').trim();
  if (ws) {
    parts.push('--public-ws-url', shellSingleQuote(ws));
  }

  for (const peer of swarmPeers) {
    const v = String(peer || '').trim();
    if (v) parts.push('--swarm-peer', shellSingleQuote(v));
  }

  for (const zone of zoneKeys) {
    const v = String(zone || '').trim();
    if (v) parts.push('--zone-key', shellSingleQuote(v));
  }

  for (const pk of collectAuthorizedIdentityDevicePks()) {
    parts.push('--authorized-device-pk', shellSingleQuote(pk));
  }

  return parts.join(' ');
}

async function prepareGatewayInstallContext() {
  const target = selectedGatewayInstallTarget();
  const targetInfo = currentGatewayTargetInfo();
  const installer = currentGatewayInstaller();
  if (!installer) {
    throw new Error(`Target ${targetInfo.label} is not supported from ${operatorPlatformLabel()} operators`);
  }

  const noteParts = [];
  if (installer.note) noteParts.push(String(installer.note));

  let command = installer.commandBase;
  let enrollment = null;

  const identityId = String(lastIdentity?.id || '').trim();
  const hasIdentity = Boolean(identityId && String(lastIdentity?.label || '').trim());

  if (target === 'linux-image' && hasIdentity) {
    const gwEnrollment = await prepareInstallEnrollment('gateway');
    command = `${command} --pair-identity ${shellSingleQuote(gwEnrollment.identityLabel)} --pair-code ${shellSingleQuote(gwEnrollment.code)} --pair-code-hash ${shellSingleQuote(gwEnrollment.codeHash)}`;
    enrollment = { gateway: gwEnrollment };
    noteParts.push('Gateway auto-pair enrollment embedded.');
  } else if (target === 'linux-image' && !hasIdentity) {
    noteParts.push('Link an identity to embed gateway auto-pair enrollment.');
  }

  if (gatewayInstallUsesDevSource()) {
    const branch = gatewayInstallDevBranchValue();
    command = `${command} --dev-source --dev-branch ${shellSingleQuote(branch)} --dev-poll`;
    noteParts.push(`Development source image enabled (branch: ${branch}).`);
  }

  if (gatewayInstallIncludesNvr()) {
    if (!targetInfo.supportsServices || target !== 'linux-image') {
      throw new Error('NVR bootstrap is only available for Linux image target');
    }
    if (!hasIdentity) {
      throw new Error('link an identity before including NVR bootstrap');
    }

    const zoneKeys = installZoneKeys();
    if (zoneKeys.length === 0) {
      throw new Error('join a zone before including NVR bootstrap');
    }

    const nvrEnrollment = await prepareInstallEnrollment('nvr');
    const nvrCommand = buildNvrInstallCommand({
      identityId,
      enrollment: nvrEnrollment,
      zoneKeys,
      swarmPeers: ['127.0.0.1:4040'],
      publicWsUrl: 'ws://127.0.0.1:8456/session',
    });

    command = `${command}

# After gateway first boot (run on the gateway host)
${nvrCommand}`;
    enrollment = {
      ...(enrollment || {}),
      nvr: nvrEnrollment,
    };
    noteParts.push('NVR install + pairing bootstrap appended (post-boot host command).');
  }

  return {
    target,
    targetInfo,
    installer,
    command,
    enrollment,
    note: noteParts.join(' '),
  };
}


async function downloadGatewayInstallerScript(prepared) {
  const installer = prepared?.installer || null;
  const command = String(prepared?.command || '').trim();
  if (!installer || !command) throw new Error('missing installer command context');

  const isWin = OPERATOR_PLATFORM === 'windows';
  const header = isWin
    ? [
      '# generated by Constitute web shell',
      '# run in elevated PowerShell when writing image to device',
      '',
    ]
    : [
      '#!/usr/bin/env bash',
      '# generated by Constitute web shell',
      'set -euo pipefail',
      '',
    ];

  const body = [
    ...header,
    command,
    '',
  ].join('\n');

  const blob = new Blob([body], { type: 'text/plain;charset=utf-8' });
  const blobUrl = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = blobUrl;
  const target = selectedGatewayInstallTarget();
  const ext = isWin ? 'ps1' : 'sh';
  a.download = `constitute-gateway-${target}-install.${ext}`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(blobUrl), 1000);
}

function parseUdpPeer(endpoint) {
  const raw = String(endpoint || '').trim();
  if (!raw) return '';
  let candidate = raw;
  if (candidate.startsWith('udp://')) {
    candidate = candidate.slice('udp://'.length);
  }
  if (!candidate.includes(':')) return '';
  return candidate;
}

function gatewaySwarmPeerForRecord(record) {
  const pk = String(record?.devicePk || record?.pk || '').trim();
  if (!pk) return '';
  const directory = Array.isArray(lastDirectory) ? lastDirectory : [];
  const hit = directory.find((entry) => String(entry?.devicePk || '').trim() === pk);
  if (!hit) return '';
  return parseUdpPeer(hit?.swarm || '');
}

function installZoneKeys() {
  const preferred = String(activeZoneKey || '').trim();
  const zones = Array.isArray(lastZones) ? lastZones : [];
  const keys = [];
  if (preferred) keys.push(preferred);
  for (const zone of zones) {
    const key = String(zone?.key || '').trim();
    if (key && !keys.includes(key)) keys.push(key);
  }
  return keys;
}

async function prepareNvrInstallContext(record) {
  const installer = currentNvrInstaller();
  if (!installer?.commandBase) throw new Error('no nvr installer configured');

  const identityId = String(lastIdentity?.id || '').trim();
  if (!identityId || !String(lastIdentity?.label || '').trim()) {
    throw new Error('link an identity before installing NVR');
  }

  const gatewayHostPlatform = normalizeRole(record?.hostPlatform || record?.host_platform || record?.platform || '');
  if (gatewayHostPlatform && gatewayHostPlatform !== 'linux' && gatewayHostPlatform !== 'fcos') {
    throw new Error('selected gateway host platform does not support NVR service installation');
  }

  const gatewayPeer = gatewaySwarmPeerForRecord(record);
  if (!gatewayPeer) throw new Error('gateway endpoint not discovered yet (wait for zone presence)');

  const zones = installZoneKeys();
  if (zones.length === 0) throw new Error('join a zone before installing NVR');

  const enrollment = await prepareInstallEnrollment('nvr');

  const gatewayHost = gatewayPeer.split(':')[0];
  const defaultWs = gatewayHost ? `ws://${gatewayHost}:8456/session` : 'ws://127.0.0.1:8456/session';
  const swarmPeers = [gatewayPeer, '127.0.0.1:4040'].filter((v, i, arr) => v && arr.indexOf(v) === i);

  const command = buildNvrInstallCommand({
    identityId,
    enrollment,
    zoneKeys: zones,
    swarmPeers,
    publicWsUrl: defaultWs,
  });

  return {
    installer,
    command,
    enrollment,
    gatewayPeer,
    zones,
  };
}


async function requestRemoteNvrInstall(record) {
  const gatewayPk = String(record?.devicePk || record?.pk || '').trim();
  if (!gatewayPk) throw new Error('gateway device pk missing');

  const prepared = await prepareNvrInstallContext(record);
  const identityId = String(lastIdentity?.id || '').trim();
  if (!identityId) throw new Error('identity is not linked');

  const gatewayHost = String(prepared.gatewayPeer || '').split(':')[0] || '';
  const publicWsUrl = gatewayHost ? `ws://${gatewayHost}:8456/session` : 'ws://127.0.0.1:8456/session';

  const payload = {
    gatewayDevicePk: gatewayPk,
    service: 'nvr',
    action: 'install',
    identityId,
    pairIdentity: prepared.enrollment.identityLabel,
    pairCode: prepared.enrollment.code,
    pairCodeHash: prepared.enrollment.codeHash,
    zone: String(prepared.zones?.[0] || ''),
    zoneKeys: prepared.zones,
    authorizedDevicePks: collectAuthorizedIdentityDevicePks(),
    swarmPeers: [prepared.gatewayPeer, '127.0.0.1:4040'].filter((v, i, arr) => v && arr.indexOf(v) === i),
    publicWsUrl,
    allowUnsignedHelloMvp: true,
    reolinkAutoprovision: true,
    timeoutSecs: 900,
  };

  const response = await client.call('gateway.service.install', payload, { timeoutMs: 20000 });
  const requestId = String(response?.requestId || '').trim();
  if (requestId) {
    pendingGatewayServiceInstalls.set(requestId, {
      gatewayPk,
      requestedAt: Date.now(),
    });
  }

  return {
    requestId,
    enrollment: prepared.enrollment,
  };
}


function updateGatewayInstallHint() {
  const target = selectedGatewayInstallTarget();
  const targetInfo = currentGatewayTargetInfo();
  const installer = currentGatewayInstaller();

  if (gatewayInstallDetectedPlatform) {
    gatewayInstallDetectedPlatform.textContent = `Detected operator platform: ${operatorPlatformLabel()}.`;
  }

  const supportsServices = targetInfo.supportsServices;
  if (gatewayInstallIncludeNvr) {
    gatewayInstallIncludeNvr.disabled = !supportsServices;
    if (!supportsServices) gatewayInstallIncludeNvr.checked = false;
  }

  const devEligible = target === 'linux-image';
  if (gatewayInstallDevSource) {
    gatewayInstallDevSource.disabled = !devEligible;
    if (!devEligible) gatewayInstallDevSource.checked = false;
  }
  if (gatewayInstallDevBranchRow) {
    gatewayInstallDevBranchRow.classList.toggle('hidden', !gatewayInstallUsesDevSource());
  }

  if (!installer) {
    setGatewayInstallHint(
      `No ${targetInfo.label} installer script is available for ${operatorPlatformLabel()} operators.`,
      true,
    );
    setGatewayInstallCommandPreview('');
    setGatewayInstallStatus('');
    return;
  }

  const supportNote = supportsServices
    ? 'Target supports gateway-hosted services (including NVR).'
    : 'Target does not support gateway-hosted NVR services.';

  const includeNote = gatewayInstallIncludesNvr()
    ? 'NVR install+pair bootstrap will be appended.'
    : 'Enable NVR bootstrap if you want first-boot NVR install wiring.';

  const devNote = gatewayInstallUsesDevSource()
    ? `Development source image enabled (branch: ${gatewayInstallDevBranchValue()}).`
    : 'Release image mode (updates from releases/latest).';

  const installerNote = installer.note ? `${installer.note} ` : '';
  setGatewayInstallHint(`${installerNote}${supportNote} ${includeNote} ${devNote}`.trim());

  let preview = installer.commandBase;
  if (gatewayInstallUsesDevSource()) {
    preview = `${preview} --dev-source --dev-branch ${shellSingleQuote(gatewayInstallDevBranchValue())} --dev-poll`;
  }
  setGatewayInstallCommandPreview(preview);
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
      .filter(Boolean)
  );
}

function summarizeAppliance(rec, owned) {
  const pk = String(rec?.devicePk || rec?.pk || '').trim();
  const label = String(rec?.deviceLabel || rec?.label || '').trim();
  const role = normalizeRole(rec?.role || rec?.nodeType || rec?.type || '') || 'unknown';
  const service = normalizeRole(rec?.service || '') || 'none';
  const version = String(rec?.serviceVersion || rec?.service_version || '').trim();
  const hostPlatform = normalizeRole(rec?.hostPlatform || rec?.host_platform || rec?.platform || '');
  const releaseChannel = String(rec?.releaseChannel || rec?.release_channel || '').trim();
  const releaseTrack = String(rec?.releaseTrack || rec?.release_track || '').trim();
  const releaseBranch = String(rec?.releaseBranch || rec?.release_branch || '').trim();
  const updatedAt = Number(rec?.updatedAt || rec?.updated_at || rec?.ts || rec?.lastSeen || 0);
  return {
    pk,
    title: label || `${role}:${pk.slice(0, 12)}`,
    role,
    service,
    version,
    hostPlatform,
    releaseChannel,
    releaseTrack,
    releaseBranch,
    updatedAt,
    owned,
  };
}

function formatReleaseMeta(channel, track, branch) {
  const ch = String(channel || '').trim();
  const tr = String(track || '').trim();
  const br = String(branch || '').trim();
  if (!ch && !tr && !br) return '';
  const left = [ch, tr].filter(Boolean).join('/');
  return br ? `${left || 'release'} @ ${br}` : (left || 'release');
}

async function ensureNvrAppEnabledFromRecord(record) {
  const hint = serviceModuleHints(record) || {
    owner: 'Aux0x7F',
    repo: 'constitute-nvr-ui',
    repoUrl: 'https://github.com/Aux0x7F/constitute-nvr-ui',
    manifestUrl: '',
    sessionWsUrl: String(record?.sessionWsUrl || record?.session_ws_url || record?.publicWsUrl || '').trim(),
    allowUnsignedHelloMvp: Boolean(record?.allowUnsignedHelloMvp || record?.allow_unsigned_hello_mvp || false),
  };

  const changed = await ensureAppRepoEnabledByUrl(hint.repoUrl, {
    manifestUrl: hint.manifestUrl,
  });

  if (hint.sessionWsUrl || hint.allowUnsignedHelloMvp) {
    const key = repoKey(hint.owner, hint.repo);
    const launchHint = {};
    if (hint.sessionWsUrl) launchHint.ws = hint.sessionWsUrl;
    if (hint.allowUnsignedHelloMvp) launchHint.insecure = true;
    appLaunchHints.set(key, launchHint);
  }

  if (changed) {
    saveAppPrefs();
    publishEnabledApps();
    renderAppCatalog();
  }

  return appRepoCatalog.find((entry) =>
    String(entry?.owner || '').toLowerCase() === String(hint.owner || '').toLowerCase() &&
    String(entry?.repo || '').toLowerCase() === String(hint.repo || '').toLowerCase()
  ) || null;
}

function launchAppWindow(app) {
  const base = appLaunchUrl(app);
  if (!base) {
    setAppStatus('No launch URL available for this app.', true);
    return;
  }
  try {
    const target = new URL(base);
    const ctx = appLaunchContextQuery(app);
    if (ctx) {
      const combined = target.search ? `${target.search}&${ctx}` : `?${ctx}`;
      target.search = combined;
    }
    window.open(target.toString(), '_blank', 'noopener,noreferrer');
  } catch {
    setAppStatus('App launch URL is invalid.', true);
  }
}

async function launchNvrControlPanel(record) {
  try {
    const app = await ensureNvrAppEnabledFromRecord(record);
    if (!app) {
      setGatewayInstallStatus('NVR UI manifest unavailable for this device.', true);
      return;
    }
    launchAppWindow(app);
  } catch (err) {
    setGatewayInstallStatus(`NVR control panel failed: ${String(err?.message || err)}`, true);
  }
}

function renderApplianceList(identityDevices, swarmDevices) {
  if (!applianceList) return;
  clear(applianceList);

  const owned = ownedPkSet(identityDevices);
  const recs = [];
  const seen = new Set();
  for (const rec of (Array.isArray(swarmDevices) ? swarmDevices : [])) {
    const pk = String(rec?.devicePk || rec?.pk || '').trim();
    if (!pk || seen.has(pk)) continue;
    if (!(isGatewayRecord(rec) || isNvrRecord(rec))) continue;
    seen.add(pk);
    recs.push(rec);
  }

  if (recs.length === 0) {
    const empty = document.createElement('div');
    empty.className = 'item';
    empty.textContent = 'No gateway or NVR appliances discovered yet.';
    applianceList.appendChild(empty);
    return;
  }

  recs.sort((a, b) => {
    const aa = summarizeAppliance(a, owned.has(String(a?.devicePk || a?.pk || '').trim()));
    const bb = summarizeAppliance(b, owned.has(String(b?.devicePk || b?.pk || '').trim()));
    return Number(bb.updatedAt || 0) - Number(aa.updatedAt || 0);
  });

  for (const rec of recs) {
    const info = summarizeAppliance(rec, owned.has(String(rec?.devicePk || rec?.pk || '').trim()));

    const item = document.createElement('div');
    item.className = 'item';

    const meta = document.createElement('div');
    const last = info.updatedAt ? new Date(info.updatedAt).toLocaleString() : 'n/a';
    const suffix = info.version ? ` (${info.version})` : '';
    const host = info.hostPlatform ? ` • host ${info.hostPlatform}` : '';
    const releaseMeta = formatReleaseMeta(info.releaseChannel, info.releaseTrack, info.releaseBranch);
    const releaseLine = releaseMeta ? `<div class="itemMeta">release ${escapeHtml(releaseMeta)}</div>` : '';
    meta.innerHTML = `
      <div class="itemTitle">${escapeHtml(info.title)}</div>
      <div class="itemMeta">pk ${escapeHtml(info.pk.slice(0, 16))}…</div>
      <div class="itemMeta">role ${escapeHtml(info.role)} • service ${escapeHtml(info.service)}${escapeHtml(suffix)}${escapeHtml(host)}</div>
      ${releaseLine}
      <div class="itemMeta">${info.owned ? 'owned by this identity' : 'discovered in zone'} • updated ${escapeHtml(last)}</div>
    `;

    const actions = document.createElement('div');
    actions.className = 'itemActions';

    if (isGatewayRecord(rec)) {
      const pair = document.createElement('button');
      pair.type = 'button';
      pair.textContent = 'Pair Existing Gateway';
      pair.onclick = () => {
        showActivity('settings');
        setSettingsTab('pairing');
        setPairCodeStatus('Enter pairing code shown by the gateway installer.');
      };
      actions.appendChild(pair);

      const gatewaySupportsServices = info.hostPlatform
        ? (info.hostPlatform === 'linux' || info.hostPlatform === 'fcos')
        : true;
      if (gatewaySupportsServices) {
        const installNvr = document.createElement('button');
        installNvr.type = 'button';
        installNvr.textContent = 'Install NVR Service';
        installNvr.onclick = async () => {
          try {
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
          } catch (err) {
            setGatewayInstallStatus(`Could not submit NVR install request: ${String(err?.message || err)}`, true);
          }
        };
        actions.appendChild(installNvr);
      } else {
        const unsupported = document.createElement('button');
        unsupported.type = 'button';
        unsupported.disabled = true;
        unsupported.textContent = 'NVR Unsupported';
        unsupported.title = 'Gateway-hosted services are currently supported on Linux image hosts only.';
        actions.appendChild(unsupported);
      }
    }

    if (isNvrRecord(rec)) {
      const open = document.createElement('button');
      open.type = 'button';
      open.textContent = 'Open Security Cameras';
      open.onclick = () => {
        launchNvrControlPanel(rec).catch((err) => {
          setGatewayInstallStatus(`NVR launch failed: ${String(err?.message || err)}`, true);
        });
      };
      actions.appendChild(open);
    }

    item.appendChild(meta);
    item.appendChild(actions);
    applianceList.appendChild(item);
  }
}

function loadAppPrefs() {
  try {
    const rawRepos = localStorage.getItem(APPS_REPOS_KEY);
    const parsedRepos = rawRepos ? JSON.parse(rawRepos) : [];
    if (Array.isArray(parsedRepos)) appRepoCatalog = parsedRepos;
  } catch {}

  if (!Array.isArray(appRepoCatalog) || appRepoCatalog.length === 0) {
    appRepoCatalog = DEFAULT_APP_REPOS.map((url) => ({ url }));
  }

  try {
    const rawEnabled = localStorage.getItem(APPS_ENABLED_KEY);
    const arr = rawEnabled ? JSON.parse(rawEnabled) : [];
    appEnabledIds = new Set(Array.isArray(arr) ? arr.map(String) : []);
  } catch {
    appEnabledIds = new Set();
  }
}

function saveAppPrefs() {
  try {
    localStorage.setItem(APPS_REPOS_KEY, JSON.stringify(appRepoCatalog));
    localStorage.setItem(APPS_ENABLED_KEY, JSON.stringify(Array.from(appEnabledIds)));
  } catch {}
}

function enabledAppManifests() {
  return appRepoCatalog
    .filter((app) => !app.unresolved)
    .filter((app) => appEnabledIds.has(String(app.id || `${app.owner}/${app.repo}`)));
}

function publishEnabledApps() {
  const enabled = enabledAppManifests().map((app) => ({
    id: String(app.id || ''),
    label: String(app.label || ''),
    owner: String(app.owner || ''),
    repo: String(app.repo || ''),
    ref: String(app.ref || ''),
    url: String(app.url || ''),
    entry: String(app.entry || 'index.html'),
    capabilities: Array.isArray(app.capabilities) ? app.capabilities.map(String) : [],
    version: String(app.version || ''),
    description: String(app.description || ''),
    manifestUrl: String(app.manifestUrl || ''),
    launchUrl: String(app.launchUrl || ''),
  }));
  window.__constituteEnabledApps = enabled;
  window.dispatchEvent(new CustomEvent('constitute.apps.updated', { detail: { enabled } }));
  renderHomeApps();
}

function normalizeRole(value) {
  return String(value || '').trim().toLowerCase();
}

function repoKey(owner, repo) {
  return `${String(owner || '').trim().toLowerCase()}/${String(repo || '').trim().toLowerCase()}`;
}

function findAppIndexByRepo(owner, repo) {
  const o = String(owner || '').trim().toLowerCase();
  const r = String(repo || '').trim().toLowerCase();
  return appRepoCatalog.findIndex((entry) =>
    String(entry?.owner || '').trim().toLowerCase() === o &&
    String(entry?.repo || '').trim().toLowerCase() === r
  );
}

function findAppIndexByManifestUrl(url) {
  const target = String(url || '').trim();
  if (!target) return -1;
  return appRepoCatalog.findIndex((entry) => String(entry?.manifestUrl || '').trim() === target);
}

function parseServiceRepoHint(value, repoRef = 'main') {
  const raw = String(value || '').trim();
  if (!raw) return null;

  if (/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(raw)) {
    const [owner, repo] = raw.split('/');
    const ref = String(repoRef || 'main').trim() || 'main';
    return {
      owner,
      repo,
      ref,
      url: `https://github.com/${owner}/${repo}/tree/${ref}`,
    };
  }

  const parsed = parseGitHubRepoInput(raw);
  if (!parsed) return null;
  if (repoRef && parsed.ref === 'main') {
    parsed.ref = String(repoRef).trim() || parsed.ref;
    parsed.url = `https://github.com/${parsed.owner}/${parsed.repo}/tree/${parsed.ref}`;
  }
  return parsed;
}

async function ensureAppRepoEnabledByUrl(url, opts = {}) {
  const parsed = parseGitHubRepoInput(url);
  if (!parsed) return false;

  const existingIdx = findAppIndexByRepo(parsed.owner, parsed.repo);
  const manifestUrl = String(opts?.manifestUrl || '').trim();

  if (existingIdx >= 0) {
    const existing = appRepoCatalog[existingIdx];
    const existingId = String(existing?.id || `${existing?.owner}/${existing?.repo}`);
    if (existing?.unresolved) {
      try {
        const manifest = await fetchAppManifest(parsed, { manifestUrl });
        appRepoCatalog[existingIdx] = manifest;
        appEnabledIds.delete(existingId);
        appEnabledIds.add(String(manifest.id || `${manifest.owner}/${manifest.repo}`));
        return true;
      } catch {
        return false;
      }
    }
    let changed = false;
    if (manifestUrl && String(existing?.manifestUrl || '').trim() !== manifestUrl) {
      existing.manifestUrl = manifestUrl;
      changed = true;
    }
    if (!appEnabledIds.has(existingId)) {
      appEnabledIds.add(existingId);
      changed = true;
    }
    return changed;
  }

  if (manifestUrl) {
    const byManifestIdx = findAppIndexByManifestUrl(manifestUrl);
    if (byManifestIdx >= 0) {
      const existing = appRepoCatalog[byManifestIdx];
      const existingId = String(existing?.id || `${existing?.owner}/${existing?.repo}`);
      if (!appEnabledIds.has(existingId)) {
        appEnabledIds.add(existingId);
        return true;
      }
      return false;
    }
  }

  try {
    const manifest = await fetchAppManifest(parsed, { manifestUrl });
    appRepoCatalog.push(manifest);
    appEnabledIds.add(String(manifest.id || `${manifest.owner}/${manifest.repo}`));
    return true;
  } catch {
    return false;
  }
}

function serviceModuleHints(rec) {
  const uiRepo = String(rec?.uiRepo || rec?.ui_repo || '').trim();
  const uiRef = String(rec?.uiRef || rec?.ui_ref || rec?.['ref'] || '').trim() || 'main';
  const uiManifestUrl = String(rec?.uiManifestUrl || rec?.ui_manifest_url || '').trim();

  if (!uiRepo && !uiManifestUrl) return null;

  const parsedRepo = parseServiceRepoHint(uiRepo, uiRef);
  if (!parsedRepo) return null;

  const sessionWsUrl = String(rec?.sessionWsUrl || rec?.session_ws_url || rec?.publicWsUrl || '').trim();
  const allowUnsignedHelloMvp = Boolean(
    rec?.allowUnsignedHelloMvp || rec?.allow_unsigned_hello_mvp || false
  );

  return {
    owner: parsedRepo.owner,
    repo: parsedRepo.repo,
    repoUrl: parsedRepo.url,
    manifestUrl: uiManifestUrl,
    sessionWsUrl,
    allowUnsignedHelloMvp,
  };
}

async function autoEnableAppsForIdentityDeviceRoles(identityDevices, swarmDevices) {
  const identityPks = new Set(
    (Array.isArray(identityDevices) ? identityDevices : [])
      .map((d) => String(d?.pk || d?.devicePk || '').trim())
      .filter(Boolean)
  );
  if (identityPks.size === 0) return;

  const roles = new Set();
  const services = new Set();
  const directHints = [];
  for (const rec of (Array.isArray(swarmDevices) ? swarmDevices : [])) {
    const pk = String(rec?.devicePk || rec?.pk || '').trim();
    if (!identityPks.has(pk)) continue;
    const role = normalizeRole(rec?.role || rec?.nodeType || rec?.type || '');
    if (role) roles.add(role);
    const service = normalizeRole(rec?.service || '');
    if (service) services.add(service);

    const hint = serviceModuleHints(rec);
    if (hint) directHints.push(hint);
  }

  const repoTargets = [];
  const launchHints = new Map();
  if (directHints.length > 0) {
    for (const hint of directHints) {
      repoTargets.push(hint);
      const key = repoKey(hint.owner, hint.repo);
      if (key !== '/') {
        const launchHint = {};
        if (hint.sessionWsUrl) launchHint.ws = hint.sessionWsUrl;
        if (hint.allowUnsignedHelloMvp) launchHint.insecure = true;
        if (Object.keys(launchHint).length > 0) {
          launchHints.set(key, launchHint);
        }
      }
    }
  } else {
    const fallbackRepoUrls = new Set();
    for (const role of roles) {
      const mapped = ROLE_APP_REPO_MAP[role] || [];
      for (const u of mapped) fallbackRepoUrls.add(u);
    }
    for (const service of services) {
      const mapped = SERVICE_APP_REPO_MAP[service] || [];
      for (const u of mapped) fallbackRepoUrls.add(u);
    }
    for (const repoUrl of fallbackRepoUrls) {
      repoTargets.push({ repoUrl, manifestUrl: '' });
    }
  }

  if (repoTargets.length === 0) return;

  if (launchHints.size > 0) {
    appLaunchHints = launchHints;
  }

  const dedupe = new Set();
  let changed = false;
  for (const target of repoTargets) {
    const key = `${target.repoUrl}@@${target.manifestUrl || ''}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    const didChange = await ensureAppRepoEnabledByUrl(target.repoUrl, {
      manifestUrl: target.manifestUrl,
    });
    changed = changed || didChange;
  }

  if (changed) {
    saveAppPrefs();
    publishEnabledApps();
    renderAppCatalog();
    renderHomeApps();
    setAppStatus('Auto-enabled app repos from detected service records.');
  }
}
function renderAppCatalog() {
  clear(appCapabilityList);
  if (!appCapabilityList) return;

  if (!Array.isArray(appRepoCatalog) || appRepoCatalog.length === 0) {
    const d = document.createElement('div');
    d.className = 'small muted';
    d.textContent = 'No app repos configured.';
    appCapabilityList.appendChild(d);
    return;
  }

  for (const app of appRepoCatalog) {
    const id = String(app.id || `${app.owner}/${app.repo}`);
    const row = document.createElement('div');
    row.className = 'item appItem';

    const label = document.createElement('label');
    label.className = 'appItemLabel';

    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = appEnabledIds.has(id);
    cb.disabled = !!app.unresolved;
    cb.onchange = () => {
      if (cb.checked) appEnabledIds.add(id);
      else appEnabledIds.delete(id);
      saveAppPrefs();
      publishEnabledApps();
      setAppStatus(`${app.label} ${cb.checked ? 'enabled' : 'disabled'}.`);
    };

    const text = document.createElement('div');
    const title = document.createElement('strong');
    title.textContent = app.unresolved ? `${app.label} (manifest unavailable)` : app.label;
    const meta = document.createElement('div');
    meta.className = 'small muted';
    const parts = [`${app.owner}/${app.repo}@${app.ref}`];
    if (app.description) parts.push(app.description);
    meta.textContent = parts.join(' - ');
    text.appendChild(title);
    text.appendChild(meta);

    label.appendChild(cb);
    label.appendChild(text);

    const btnRemove = document.createElement('button');
    btnRemove.type = 'button';
    btnRemove.textContent = 'Remove';
    btnRemove.onclick = () => {
      appRepoCatalog = appRepoCatalog.filter((x) => String(x.id || `${x.owner}/${x.repo}`) !== id);
      appEnabledIds.delete(id);
      saveAppPrefs();
      publishEnabledApps();
      renderAppCatalog();
      setAppStatus(`Removed ${app.label}.`);
    };

    row.appendChild(label);
    row.appendChild(btnRemove);
    appCapabilityList.appendChild(row);
  }
}

function appLaunchUrl(app) {
  const explicit = String(app?.launchUrl || '').trim();
  if (explicit && /^https:\/\//.test(explicit)) return explicit;

  const owner = String(app?.owner || '').trim();
  const repo = String(app?.repo || '').trim();
  const ref = String(app?.ref || 'main').trim() || 'main';
  const entry = String(app?.entry || 'index.html').replace(/^\/+/, '');
  if (!owner || !repo || !entry) return '';

  return `https://cdn.jsdelivr.net/gh/${owner}/${repo}@${ref}/${entry}`;
}

function appLaunchContextQuery(app) {
  const q = new URLSearchParams();
  const identityId = String(lastIdentity?.id || '').trim();
  const devicePk = String(lastDeviceState?.pk || '').trim();
  if (identityId) q.set('identityId', identityId);
  if (devicePk) q.set('devicePk', devicePk);

  const key = repoKey(app?.owner, app?.repo);
  const hint = appLaunchHints.get(key);
  if (hint?.ws) q.set('ws', String(hint.ws));
  if (hint?.insecure) q.set('insecure', '1');
  q.set('autoconnect', '1');

  return q.toString();
}

function renderHomeApps() {
  clear(homeAppsList);
  if (!homeAppsList) return;

  const apps = enabledAppManifests();
  if (!apps.length) {
    const d = document.createElement('div');
    d.className = 'small muted';
    d.textContent = 'No apps enabled.';
    homeAppsList.appendChild(d);
    return;
  }

  for (const app of apps) {
    const row = document.createElement('div');
    row.className = 'item appItem';

    const left = document.createElement('div');
    left.className = 'appItemLabel';

    const text = document.createElement('div');
    const title = document.createElement('strong');
    title.textContent = app.label || app.id || 'App';

    const meta = document.createElement('div');
    meta.className = 'small muted';
    const parts = [`${app.owner}/${app.repo}@${app.ref}`];
    if (app.description) parts.push(app.description);
    meta.textContent = parts.join(' - ');

    text.appendChild(title);
    text.appendChild(meta);
    left.appendChild(text);

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = 'Launch';
    btn.onclick = () => {
      launchAppWindow(app);
    };

    row.appendChild(left);
    row.appendChild(btn);
    homeAppsList.appendChild(row);
  }
}

async function addAppRepoFromInput() {
  const parsed = parseGitHubRepoInput(appRepoInput?.value);
  if (!parsed) {
    setAppStatus('Paste a valid GitHub repo URL (or owner/repo).', true);
    return;
  }

  setAppStatus('Fetching app manifest…');
  try {
    const manifest = await fetchAppManifest(parsed);
    const id = String(manifest.id || `${manifest.owner}/${manifest.repo}`);
    const existingIdx = appRepoCatalog.findIndex((x) => String(x.id || `${x.owner}/${x.repo}`) === id);
    if (existingIdx >= 0) appRepoCatalog[existingIdx] = manifest;
    else appRepoCatalog.push(manifest);
    appEnabledIds.add(id);
    saveAppPrefs();
    publishEnabledApps();
    renderAppCatalog();
    if (appRepoInput) appRepoInput.value = '';
    setAppStatus(`Added ${manifest.label}.`);
  } catch (err) {
    setAppStatus(`Failed to fetch manifest: ${String(err?.message || err)}`, true);
  }
}

async function hydrateAppCatalog() {
  loadAppPrefs();
  const hydrated = [];
  for (const entry of appRepoCatalog) {
    const parsed = entry.owner && entry.repo
      ? { owner: entry.owner, repo: entry.repo, ref: entry.ref || 'main', url: entry.url || `https://github.com/${entry.owner}/${entry.repo}` }
      : parseGitHubRepoInput(entry.url || '');
    if (!parsed) continue;
    try {
      const manifest = await fetchAppManifest(parsed);
      hydrated.push(manifest);
      if (!appEnabledIds.has(String(manifest.id))) appEnabledIds.add(String(manifest.id));
    } catch {
      const unresolvedId = String(entry.id || `${parsed.owner}/${parsed.repo}`);
      hydrated.push({
        id: unresolvedId,
        label: String(entry.label || parsed.repo),
        entry: String(entry.entry || 'index.html'),
        capabilities: Array.isArray(entry.capabilities) ? entry.capabilities.map(String) : [],
        description: String(entry.description || 'Manifest unavailable'),
        version: String(entry.version || ''),
        manifestUrl: String(entry.manifestUrl || ''),
        launchUrl: String(entry.launchUrl || ''),
        owner: parsed.owner,
        repo: parsed.repo,
        ref: parsed.ref || 'main',
        url: parsed.url,
        unresolved: true,
      });
      appEnabledIds.delete(unresolvedId);
    }
  }
  appRepoCatalog = hydrated;
  saveAppPrefs();
  publishEnabledApps();
  renderAppCatalog();
  const okCount = hydrated.filter((x) => !x.unresolved).length;
  const badCount = hydrated.length - okCount;
  if (hydrated.length === 0) {
    setAppStatus('No app manifests loaded.', true);
  } else if (badCount > 0) {
    setAppStatus(`${okCount} loaded, ${badCount} unavailable.`, true);
  } else {
    setAppStatus(`${okCount} app manifest(s) loaded.`);
  }
}

function clear(el) {
  if (!el) return;
  while (el.firstChild) el.removeChild(el.firstChild);
}

function escapeHtml(s) {
  return String(s ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function b64url(bytes) {
  let s = '';
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replaceAll('+', '-').replaceAll('/', '_').replaceAll('=', '');
}

function renderNotifications(notifs) {
  clear(notifList);
  const unread = (notifs || []).filter(n => !n.read);
  btnBell.classList.toggle('has-unread', unread.length !== 0);

  if (!notifs || notifs.length === 0) {
    const d = document.createElement('div');
    d.className = 'item';
    d.textContent = 'No notifications.';
    notifList.appendChild(d);
    return;
  }

  for (const n of notifs) {
    const it = document.createElement('div');
    it.className = 'item';
    it.innerHTML = `
      <div class="itemTitle">${escapeHtml(n.title || '')}${n.read ? '' : ' •'}</div>
      <div class="itemMeta">${escapeHtml(n.body || '')}</div>
      <div class="itemMeta">${new Date(n.ts || Date.now()).toLocaleString()}</div>
    `;
    it.onclick = async () => {
      try { await client.call('notifications.remove', { id: n.id }, { timeoutMs: 12000 }); } catch {}
      if (n.kind === 'pairing') {
        notifMenu.classList.add('hidden');
        showActivity('settings');
        setSettingsTab('pairing');
      }
      await refreshAll();
      if (n.kind === 'pairing') {
        showActivity('settings');
        setSettingsTab('pairing');
      }
    };
    notifList.appendChild(it);
  }
}

function filterPendingPairRequests(reqs, identityDevices) {
  const knownPks = new Set((identityDevices || []).map(d => d.pk).filter(Boolean));
  const knownDids = new Set((identityDevices || []).map(d => d.did).filter(Boolean));

  return (reqs || []).filter(r => {
    if (r.status && r.status !== 'pending') return false;
    if (r.state && r.state !== 'pending') return false;
    if (r.resolved === true) return false;
    if (r.approved === true || r.rejected === true) return false;
    if (r.devicePk && knownPks.has(r.devicePk)) return false;
    if (r.deviceDid && knownDids.has(r.deviceDid)) return false;
    return true;
  });
}

function renderPairRequests(reqs, identityDevices) {
  const pending = filterPendingPairRequests(reqs, identityDevices);

  clear(pairingList);
  pairingEmpty.classList.toggle('hidden', pending.length !== 0);

  for (const r of pending) {
    const item = document.createElement('div');
    item.className = 'item';

    const top = document.createElement('div');
    top.className = 'itemTop';

    const left = document.createElement('div');
    const codeMeta = r.code
      ? `code ${r.code}`
      : (r.codeHash ? `code# ${String(r.codeHash).slice(0, 12)}…` : 'code n/a');
    left.innerHTML = `
      <div class="itemTitle">${escapeHtml(r.identityLabel || '(no identity label)')}</div>
      <div class="itemMeta">Device: ${escapeHtml(r.deviceLabel || '(no label)')} • pk ${escapeHtml((r.devicePk || '').slice(0, 12))}… • ${escapeHtml(codeMeta)}</div>
    `;

    const actions = document.createElement('div');
    actions.className = 'itemActions';

    const btnApprove = document.createElement('button');
    btnApprove.className = 'ok';
    btnApprove.textContent = 'Approve';

    const btnReject = document.createElement('button');
    btnReject.className = 'danger';
    btnReject.textContent = 'Reject';

    btnApprove.onclick = async () => {
      try {
        btnApprove.disabled = true;
        btnApprove.textContent = 'Approving…';
        await client.call('pairing.approve', { requestId: r.id }, { timeoutMs: 20000 });
        await refreshAll();
      } catch (e) {
        console.error(e);
        pairingEmpty.textContent = `Approve failed: ${String(e?.message || e)}`;
        pairingEmpty.classList.remove('hidden');
      } finally {
        btnApprove.disabled = false;
        btnApprove.textContent = 'Approve';
      }
    };

    btnReject.onclick = async () => {
      try {
        btnReject.disabled = true;
        btnReject.textContent = 'Rejecting…';
        await client.call('pairing.reject', { requestId: r.id }, { timeoutMs: 20000 });
        await refreshAll();
      } catch (e) {
        console.error(e);
        pairingEmpty.textContent = `Reject failed: ${String(e?.message || e)}`;
        pairingEmpty.classList.remove('hidden');
      } finally {
        btnReject.disabled = false;
        btnReject.textContent = 'Reject';
      }
    };

    actions.append(btnApprove, btnReject);
    top.append(left, actions);
    item.append(top);
    pairingList.appendChild(item);
  }
}

function renderDeviceList(devs) {
  clear(deviceList);
  if (!devs || devs.length === 0) {
    const d = document.createElement('div');
    d.className = 'item';
    d.textContent = 'No devices yet.';
    deviceList.appendChild(d);
    return;
  }
  for (const d0 of devs) {
    const d = document.createElement('div');
    d.className = 'item';
    const top = document.createElement('div');
    top.className = 'itemTop';

    const info = document.createElement('div');
    info.innerHTML = `
      <div class="itemTitle">${escapeHtml(d0.label || '(no label)')}</div>
      <div class="itemMeta">${escapeHtml((d0.did || '').slice(0, 42))}${(d0.did||'').length>42?'…':''}</div>
      <div class="itemMeta">pk ${escapeHtml((d0.pk || '').slice(0, 12))}…</div>
    `;

    const actions = document.createElement('div');
    actions.className = 'itemActions';

    if (d0?.pk) {
      const btnRevoke = document.createElement('button');
      btnRevoke.className = 'danger';
      btnRevoke.textContent = 'X';
      btnRevoke.title = 'Revoke device';
      btnRevoke.onclick = async () => {
        try { await client.call('device.revoke', { pk: d0.pk }, { timeoutMs: 20000 }); } catch (e) { console.error(e); }
        await refreshAll();
      };
      actions.appendChild(btnRevoke);
    }

    top.append(info, actions);
    d.appendChild(top);
    deviceList.appendChild(d);
  }
}

function renderBlockedList(list) {
  clear(blockedList);
  if (!list || list.length === 0) {
    const d = document.createElement('div');
    d.className = 'item';
    d.textContent = 'No blocked devices.';
    blockedList.appendChild(d);
    return;
  }
  for (const b of list) {
    const item = document.createElement('div');
    item.className = 'item';
    const top = document.createElement('div');
    top.className = 'itemTop';

    const info = document.createElement('div');
    const ts = b?.ts ? new Date(b.ts).toLocaleString() : '';
    info.innerHTML = `
      <div class="itemTitle">${escapeHtml(b?.reason || 'blocked')}</div>
      <div class="itemMeta">pk ${escapeHtml((b?.pk || '').slice(0, 12))}…</div>
      <div class="itemMeta">${escapeHtml(b?.did || '')}</div>
      <div class="itemMeta">${escapeHtml(ts)}</div>
    `;

    const actions = document.createElement('div');
    actions.className = 'itemActions';
    if (b?.pk || b?.did) {
      const btnUnblock = document.createElement('button');
      btnUnblock.className = 'danger';
      btnUnblock.textContent = 'X';
      btnUnblock.title = 'Unblock device';
      btnUnblock.onclick = async () => {
        try { await client.call('blocked.remove', { pk: b?.pk || '', did: b?.did || '' }, { timeoutMs: 20000 }); } catch (e) { console.error(e); }
        await refreshAll();
      };
      actions.appendChild(btnUnblock);
    }

    top.append(info, actions);
    item.appendChild(top);
    blockedList.appendChild(item);
  }
}

function renderZones(list) {
  clear(zonesList);
  const arr = Array.isArray(list) ? list : [];
  if (arr.length === 0) {
    const d = document.createElement('div');
    d.className = 'item';
    d.textContent = 'No zones yet.';
    zonesList.appendChild(d);
    return;
  }
  for (const z of arr) {
    const item = document.createElement('div');
    item.className = 'card';
    const name = z.name || '(unnamed)';
    const key = z.key || '';
    const zoneMembers = (Array.isArray(lastDirectory) ? lastDirectory : []).filter(e => e.devicePk && e.zone === key);
    item.innerHTML = `
      <div class="cardTitle">${escapeHtml(name)}</div>
      <div class="itemMeta">${escapeHtml(key)}</div>
      <div class="small muted" style="margin-top:.35rem;">Peers: ${zoneMembers.length}</div>
      <div class="list" data-zone-list="${escapeHtml(key)}"></div>
    `;
    const listEl = item.querySelector(`[data-zone-list="${escapeHtml(key)}"]`);
    if (zoneMembers.length === 0) {
      const d = document.createElement('div');
      d.className = 'item';
      d.textContent = 'No devices discovered yet.';
      listEl.appendChild(d);
    } else {
      for (const e of zoneMembers) {
        const row = document.createElement('div');
        row.className = 'item';
        const nostrSeen = Number(e.lastSeen || 0);
        const swarmSeen = swarm ? swarm.getSwarmSeen(e.devicePk) : 0;
        const sources = [
          nostrSeen ? 'nostr' : '',
          swarmSeen ? 'swarm' : '',
        ].filter(Boolean).join(' + ') || 'unknown';
        const role = String(e.role || '').trim();
        const serviceVersion = String(e.serviceVersion || '').trim();
        const roleLine = serviceVersion ? `${role || 'unknown'} (${serviceVersion})` : (role || 'unknown');
        const releaseMeta = formatReleaseMeta(e.releaseChannel, e.releaseTrack, e.releaseBranch);
        const releaseLine = releaseMeta ? `<div class="itemMeta">Release: ${escapeHtml(releaseMeta)}</div>` : '';
        row.innerHTML = `
          <div class="itemTitle">${escapeHtml(e.devicePk || '')}</div>
          <div class="itemMeta">Source: ${escapeHtml(sources)}</div>
          <div class="itemMeta">Role: ${escapeHtml(roleLine)}</div>
          ${releaseLine}
          <div class="itemMeta">Nostr seen ${nostrSeen ? new Date(nostrSeen).toLocaleString() : 'n/a'}</div>
          <div class="itemMeta">Swarm seen ${swarmSeen ? new Date(swarmSeen).toLocaleString() : 'n/a'}</div>
        `;
        listEl.appendChild(row);
      }
    }
    item.onclick = () => {
      activeZoneKey = key;
      setZoneLink(key);
      renderPeers(lastDirectory);
    };
    zonesList.appendChild(item);
  }
  if (!activeZoneKey && arr[0]?.key) {
    activeZoneKey = arr[0].key;
    setZoneLink(activeZoneKey);
  }
}

function setZoneLink(key) {
  if (!zoneLink) return;
  if (!key) { zoneLink.textContent = ''; return; }
  const base = `${window.location.origin}${window.location.pathname}`;
  const url = `${base}?zone=${encodeURIComponent(key)}`;
  zoneLink.textContent = url;
}

function renderPeers(list) {
  if (!peersList) return;
  clear(peersList);
  const arr = Array.isArray(list) ? list : [];
  const deviceEntries = arr.filter(e => e.devicePk && (!activeZoneKey || e.zone === activeZoneKey));
  if (peersCount) peersCount.textContent = `${deviceEntries.length} devices`;
  if (deviceEntries.length === 0) {
    const d = document.createElement('div');
    d.className = 'item';
    d.textContent = 'No devices discovered yet.';
    peersList.appendChild(d);
    return;
  }
  for (const e of deviceEntries) {
    const item = document.createElement('div');
    item.className = 'item';
    const nostrSeen = Number(e.lastSeen || 0);
    const swarmSeen = swarm ? swarm.getSwarmSeen(e.devicePk) : 0;
    const sources = [
      nostrSeen ? 'nostr' : '',
      swarmSeen ? 'swarm' : '',
    ].filter(Boolean).join(' + ') || 'unknown';
    const role = String(e.role || '').trim();
    const serviceVersion = String(e.serviceVersion || '').trim();
    const roleLine = serviceVersion ? `${role || 'unknown'} (${serviceVersion})` : (role || 'unknown');
    const releaseMeta = formatReleaseMeta(e.releaseChannel, e.releaseTrack, e.releaseBranch);
    const releaseLine = releaseMeta ? `<div class="itemMeta">Release: ${escapeHtml(releaseMeta)}</div>` : '';
    item.innerHTML = `
      <div class="itemTitle">${escapeHtml(e.devicePk || '')}</div>
      <div class="itemMeta">Source: ${escapeHtml(sources)}</div>
      <div class="itemMeta">Role: ${escapeHtml(roleLine)}</div>
      ${releaseLine}
      <div class="itemMeta">Nostr seen ${nostrSeen ? new Date(nostrSeen).toLocaleString() : 'n/a'}</div>
      <div class="itemMeta">Swarm seen ${swarmSeen ? new Date(swarmSeen).toLocaleString() : 'n/a'}</div>
    `;
    peersList.appendChild(item);
  }
}

// onboarding: security radio choice
let onboardingSecurityChoice = null; // 'webauthn' | 'skip'

function setSecurityChoice(which) {
  onboardingSecurityChoice = which;
  btnSecWebAuthn.classList.toggle('selected', which === 'webauthn');
  btnSecSkip.classList.toggle('selected', which === 'skip');
  btnSecWebAuthn.setAttribute('aria-checked', which === 'webauthn' ? 'true' : 'false');
  btnSecSkip.setAttribute('aria-checked', which === 'skip' ? 'true' : 'false');
}

btnSecWebAuthn?.addEventListener('click', () => setSecurityChoice('webauthn'));
btnSecSkip?.addEventListener('click', () => setSecurityChoice('skip'));

let client;

function ensureOnboardingState(ident) {
  if (!ident?.linked) {
    showActivity('onboarding');
    setOnboardStep(1);
  }
}

async function refreshAll() {
  // SEQUENTIAL (not Promise.all) to avoid SW starvation/timeouts.
  const st = await client.call('device.getState', {}, { timeoutMs: 20000 });
  const ident = await client.call('identity.get', {}, { timeoutMs: 20000 });
  const prof = await client.call('profile.get', {}, { timeoutMs: 20000 });
  const reqs = await client.call('pairing.list', {}, { timeoutMs: 20000 });
  const blocked = await client.call('blocked.list', {}, { timeoutMs: 20000 });
  const directory = await client.call('directory.list', {}, { timeoutMs: 20000 });
  const zones = await client.call('zones.list', {}, { timeoutMs: 20000 });
  const swarmDevices = await client.call('swarm.device.list', {}, { timeoutMs: 20000 }).catch(() => []);
  const swarmIdentities = await client.call('swarm.identity.list', {}, { timeoutMs: 20000 }).catch(() => []);
  const notifs = await client.call('notifications.list', {}, { timeoutMs: 20000 });
  const myLabel = await client.call('device.getLabel', {}, { timeoutMs: 20000 });

  lastDeviceState = st;
  lastIdentity = ident;
  lastDirectory = directory || [];
  lastZones = zones || [];
  lastSwarmDevices = swarmDevices || [];

  // If we only have a key, try to resolve the human name via peers.
  for (const z of (lastZones || [])) {
    const n = String(z?.name || '').trim();
    if (!n || n === 'Joined' || n.startsWith('Zone ')) {
      client.call('zones.meta.request', { key: z.key }, { timeoutMs: 20000 }).catch(() => {});
      client.call('zones.list.request', { key: z.key }, { timeoutMs: 20000 }).catch(() => {});
    }
  }

  setDaemonState('online', 'rpc ok');

  deviceDid.textContent = st.did || '';
  deviceDidSummary.textContent = st.did || '(none)';
  deviceSecuritySummary.textContent = st.didMethod === 'webauthn' ? 'platform-backed' : 'software-only';
  identityLinkedSummary.textContent = ident?.linked ? 'yes' : 'no';

  identityLabelEl.textContent = ident?.label || '';
  identityIdEl.textContent = ident?.id || '';
  identityLinkedEl.textContent = ident?.linked ? 'yes' : 'no';

  profileName.value = ident?.label || '';
  profileAbout.value = prof?.about || '';

  deviceLabel.value = myLabel?.label || '';

  renderDeviceList(ident?.devices || []);
  renderBlockedList(blocked || []);
  renderZones(lastZones);
  renderPeers(lastDirectory);
  renderApplianceList(ident?.devices || [], lastSwarmDevices);
  renderPairRequests(reqs || [], ident?.devices || []);
  renderNotifications(notifs || []);
  ensureOnboardingState(ident);
  await autoEnableAppsForIdentityDeviceRoles(ident?.devices || [], swarmDevices || []).catch(() => {});
  if (swarm && ident?.linked) {
    swarm.setLocalPk(st.pk || '');
    swarm.setPeers(swarmDevices || []);
    swarm.cacheIdentityRecords(swarmIdentities || []);
    swarm.cacheDeviceRecords(swarmDevices || []);
    if (!swarmBootRequested) {
      swarmBootRequested = true;
      client.call('swarm.record.request', { want: ['identity', 'device'] }, { timeoutMs: 20000 })
        .catch(() => client.call('swarm.discovery.request', {}, { timeoutMs: 20000 }))
        .catch(() => {});
    }
  } else {
    setSwarmState('disabled');
  }

  return { st, ident };
}

function setOnboardStep(n) {
  obStepDevice.classList.toggle('hidden', n !== 1);
  obStepIdentity.classList.toggle('hidden', n !== 2);
}

async function waitForPairAcceptance({ identityLabel, myDevicePk, timeoutMs = 90000 }) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const ident = await client.call('identity.get', {}, { timeoutMs: 20000 }).catch(() => null);
    if (ident?.linked && ident?.label === identityLabel) {
      const list = Array.isArray(ident.devices) ? ident.devices : [];
      if (!myDevicePk) return true;
      if (list.some(d => d?.pk === myDevicePk)) return true;
    }
    await new Promise(r => setTimeout(r, 800));
  }
  return false;
}

async function ensureOnboardingFlow() {
  const ident = await client.call('identity.get', {}, { timeoutMs: 20000 }).catch(() => null);
  if (ident?.linked) return true;
  showActivity('onboarding');
  setOnboardStep(1);
  return false;
}

async function applyPendingZone() {
  if (!lastIdentity?.linked) return;
  const pending = await client.call('zones.pending.get', {}, { timeoutMs: 20000 }).catch(() => '');
  const key = normalizeZoneKey(pending);
  if (!key) return;
    try { await client.call('zones.join', { key }, { timeoutMs: 20000 }); } catch {}
  try { await client.call('zones.pending.clear', {}, { timeoutMs: 20000 }); } catch {}
  await refreshAll();
  if (pendingZoneNav) {
    showActivity('settings');
    setSettingsTab('peers');
    pendingZoneNav = false;
    return true;
  }
  return false;
}

function normalizeZoneKey(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';
  if (!raw.includes('://')) return raw;
  try {
    const u = new URL(raw);
    return u.searchParams.get('zone') || '';
  } catch {
    return '';
  }
}

async function applyUrlParams() {
  const params = new URLSearchParams(window.location.search || '');
  const zone = normalizeZoneKey(params.get('zone'));
  let didJoin = false;
  if (zone) {
    try {
      if (!lastIdentity?.linked) {
        await client.call('zones.pending.set', { key: zone }, { timeoutMs: 20000 });
        pendingZoneNav = true;
      } else {
        await client.call('zones.join', { key: zone }, { timeoutMs: 20000 });
        didJoin = true;
      }
    } catch {}
  }
  if (didJoin) {
    await refreshAll();
    try { await client.call('zones.pending.clear', {}, { timeoutMs: 20000 }); } catch {}
    showActivity('settings');
    setSettingsTab('peers');
    return true;
  }
  return false;
}

async function postIdentityLinkedFlow() {
  const didUrlNav = await applyUrlParams();
  const didPendingNav = await applyPendingZone();
  if (didUrlNav || didPendingNav || pendingZoneNav) return;
  showActivity('home');
}

async function runWebAuthnSetup() {
  obDeviceStatus.textContent = 'Starting WebAuthn…';
  const want = await client.call('device.wantWebAuthnUpgrade', {}, { timeoutMs: 20000 });
  if (!want?.ok) {
    obDeviceStatus.textContent = 'Already platform-backed.';
    return true;
  }

  const cred = await navigator.credentials.create({
    publicKey: {
      rp: { name: 'Constitute' },
      user: {
        id: new TextEncoder().encode(want.deviceIdHint || String(Date.now())),
        name: 'device',
        displayName: 'device',
      },
      challenge: crypto.getRandomValues(new Uint8Array(32)),
      pubKeyCredParams: [{ type: 'public-key', alg: -7 }],
      authenticatorSelection: { residentKey: 'preferred', userVerification: 'preferred' },
      timeout: 60000,
      attestation: 'none',
    }
  });

  const rawId = new Uint8Array(cred.rawId);
  const credIdB64 = b64url(rawId);
  await client.call('device.setWebAuthn', { credIdB64 }, { timeoutMs: 20000 });
  obDeviceStatus.textContent = 'WebAuthn set.';
  return true;
}

function wireUi() {
  // drawer nav
  for (const b of drawer.querySelectorAll('.navbtn')) {
    b.addEventListener('click', () => {
      showActivity(b.dataset.activity);
      closeDrawer();
    });
  }

  // settings tabs
  for (const b of tabButtons) b.addEventListener('click', () => setSettingsTab(b.dataset.tab));

  if (btnClaimPairCode) {
    btnClaimPairCode.onclick = async () => {
      const code = String(pairCodeInput?.value || '').trim();
      if (!code) {
        setPairCodeStatus('Enter a code from the new device.', true);
        return;
      }
      try {
        await client.call('pairing.claimCode', { code }, { timeoutMs: 20000 });
        setPairCodeStatus('Claim sent. Wait for the pairing request, then approve it below.');
      } catch (e) {
        setPairCodeStatus(`Claim failed: ${String(e?.message || e)}`, true);
      }
    };
  }

  if (btnAddAppRepo) {
    btnAddAppRepo.onclick = () => addAppRepoFromInput().catch((e) => console.error(e));
  }
  if (appRepoInput) {
    appRepoInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        addAppRepoFromInput().catch((err) => console.error(err));
      }
    });
  }

  if (gatewayInstallPlatform) {
    gatewayInstallPlatform.value = preferredGatewayInstallTarget();
    gatewayInstallPlatform.addEventListener('change', updateGatewayInstallHint);
  }
  if (gatewayInstallIncludeNvr) {
    gatewayInstallIncludeNvr.addEventListener('change', updateGatewayInstallHint);
  }
  if (gatewayInstallDevSource) {
    gatewayInstallDevSource.addEventListener('change', updateGatewayInstallHint);
  }
  if (gatewayInstallDevBranch) {
    gatewayInstallDevBranch.addEventListener('input', updateGatewayInstallHint);
  }
  updateGatewayInstallHint();

  if (btnGatewayInstallCopy) {
    btnGatewayInstallCopy.onclick = async () => {
      try {
        const prepared = await prepareGatewayInstallContext();
        await navigator.clipboard.writeText(prepared.command);
        const suffix = prepared.note ? ` ${prepared.note}` : '';
        setGatewayInstallStatus(`Copied ${prepared.targetInfo.label} command.${suffix}`);
      } catch (err) {
        setGatewayInstallStatus(`Could not prepare install command: ${String(err?.message || err)}`, true);
      }
    };
  }

  if (btnGatewayInstallOpen) {
    btnGatewayInstallOpen.onclick = async () => {
      try {
        const prepared = await prepareGatewayInstallContext();
        await downloadGatewayInstallerScript(prepared);
        const suffix = prepared.note ? ` ${prepared.note}` : '';
        setGatewayInstallStatus(`Downloaded generated installer script for ${prepared.targetInfo.label}.${suffix}`);
      } catch (err) {
        setGatewayInstallStatus(`Script download failed: ${String(err?.message || err)}`, true);
      }
    };
  }

  btnSaveProfile.onclick = async () => {
    try {
      const label = profileName.value.trim();
      if (label) {
        await client.call('identity.setLabel', { identityLabel: label }, { timeoutMs: 20000 });
      }
      await client.call('profile.set', { name: label, about: profileAbout.value }, { timeoutMs: 20000 });
      await refreshAll();
    } catch (e) { console.error(e); }
  };

  btnSaveDeviceLabel.onclick = async () => {
    try {
      await client.call('device.setLabel', { label: deviceLabel.value }, { timeoutMs: 20000 });
      await refreshAll();
    } catch (e) { console.error(e); }
  };

  btnNotifClear.onclick = async () => {
    try { await client.call('notifications.clear', {}, { timeoutMs: 20000 }); } catch {}
    await refreshAll();
  };

  btnCreateZone.onclick = async () => {
    const name = String(zoneNameInput.value || '').trim();
    if (!name) return;
    try { await client.call('zones.add', { name }, { timeoutMs: 20000 }); } catch (e) { console.error(e); }
    zoneNameInput.value = '';
    await refreshAll();
  };

  btnCopyZoneLink.onclick = async () => {
    const link = String(zoneLink.textContent || '').trim();
    if (!link) return;
    try { await navigator.clipboard.writeText(link); } catch {}
  };

  btnJoinZone.onclick = async () => {
    const key = normalizeZoneKey(zoneJoinKey.value);
    if (!key) return;
    try { await client.call('zones.join', { key }, { timeoutMs: 20000 }); } catch (e) { console.error(e); }
    zoneJoinKey.value = '';
    await refreshAll();
  };

  // onboarding mode tabs
  let mode = 'new';
  obModeTabs.forEach(t => {
    t.onclick = () => {
      obModeTabs.forEach(x => x.classList.remove('tab-active'));
      t.classList.add('tab-active');
      mode = t.dataset.mode === 'existing' ? 'existing' : 'new';
      existingInfo.classList.toggle('hidden', true);
      if (obPairCodeWrap) obPairCodeWrap.classList.toggle('hidden', mode !== 'existing');
    };
  });

  // onboarding step1: enforce radio selection
  btnObDeviceContinue.onclick = async () => {
    if (!onboardingSecurityChoice) {
      obDeviceStatus.textContent = 'Pick WebAuthn or Skip.';
      return;
    }

    try {
      if (onboardingSecurityChoice === 'webauthn') {
        await runWebAuthnSetup();
      } else {
        await client.call('device.noteWebAuthnSkipped', {}, { timeoutMs: 20000 });
        obDeviceStatus.textContent = 'Using software-only keys.';
      }
      setOnboardStep(2);
    } catch (e) {
      console.error(e);
      obDeviceStatus.textContent = `Failed: ${String(e?.message || e)}`;
    }
  };

  // onboarding step2: require both labels (always)
  btnObIdentityContinue.onclick = async () => {
    const dlabel = obDeviceLabel.value.trim();
    const ilabel = obIdentityLabel.value.trim();

    if (!dlabel) { obDeviceLabel.focus(); return; }
    if (!ilabel) { obIdentityLabel.focus(); return; }

    try {
      // Preflight: if already linked, block "create" but allow "join existing".
      const current = await client.call('identity.get', {}, { timeoutMs: 20000 }).catch(() => null);
      if (current?.linked && mode === 'new') {
        existingInfo.classList.remove('hidden');
        existingInfo.textContent = `Identity already exists on this device (${current.label || 'unknown'}).`;
        return;
      }
      if (current?.linked && mode === 'existing') {
        existingInfo.classList.remove('hidden');
        existingInfo.textContent = `Current identity will be replaced after approval (${current.label || 'unknown'}).`;
      }

        if (mode === 'new') {
          await client.call('identity.create', { identityLabel: ilabel, deviceLabel: dlabel }, { timeoutMs: 20000 });
          await refreshAll();
          await postIdentityLinkedFlow();
          return;
        }

      existingInfo.classList.remove('hidden');
      existingInfo.textContent = 'Requesting pairing…';

      const myDevicePk = lastDeviceState?.pk || null;
      const code = String(obPairCode?.value || '').trim();

      const res = await client.call('identity.requestPair', {
        identityLabel: ilabel,
        deviceLabel: dlabel,
        code: code || undefined,
      }, { timeoutMs: 20000 });

      const usedCode = String(res?.code || code || '').trim();
      existingInfo.textContent = usedCode
        ? `Pairing code: ${usedCode}. Ask the owner to enter this in Settings > Pairing > Add Device, then approve.`
        : 'Waiting for owner claim and approval…';
      const ok = await waitForPairAcceptance({ identityLabel: ilabel, myDevicePk, timeoutMs: 90000 });

        if (ok) {
          existingInfo.textContent = '';
          await refreshAll();
          await postIdentityLinkedFlow();
          return;
        }

      existingInfo.textContent = 'Timed out. Approve on the other device and try again.';
    } catch (e) {
      console.error(e);
      existingInfo.classList.remove('hidden');
      existingInfo.textContent = String(e?.message || e);
    }
  };
}

function startSharedRelayPipe(client, relayUrl) {
  const w = new SharedWorker('./relay.worker.js');
  const port = w.port;
  port.start();

  port.onmessage = async (ev) => {
    const msg = ev.data || {};
    if (msg.type === 'relay.status') {
      setRelayState(msg.state, msg.reason || '');
      if (clientReady) {
        client.call('relay.status', {
          state: msg.state,
          url: msg.url || '',
          code: msg.code ?? null,
          reason: msg.reason ?? ''
        }, { timeoutMs: 20000 }).catch((e) => console.error('relay.status rpc failed', e));
      }
      return;
    }
    if (msg.type === 'relay.rx' && typeof msg.data === 'string') {
      if (clientReady) {
        client.call('relay.rx', { data: msg.data, url: msg.url || '' }, { timeoutMs: 20000 })
          .catch((e) => console.error('relay.rx rpc failed', e));
      }
      return;
    }
  };

  navigator.serviceWorker.addEventListener('message', (e) => {
    const m = e.data || {};
    if (m.type === 'relay.tx' && typeof m.data === 'string') {
      port.postMessage({ type: 'relay.send', frame: m.data });
    }
  });

  port.postMessage({ type: 'relay.connect', url: relayUrl });
  return port;
}

(async function main() {
  // Default to onboarding until SW state is confirmed.
  showActivity('onboarding');
  setOnboardStep(1);

  client = new IdentityClient({
    onEvent: (evt) => {
      if (evt?.type === 'log') {
        const msg = String(evt.message || '');
        if (msg.startsWith('[zone_list] payload list: ')) {
          const raw = msg.replace('[zone_list] payload list: ', '');
          try { console.log('[sw][zone_list payload]', JSON.parse(raw)); }
          catch { console.log('[sw][zone_list payload]', raw); }
          return;
        }
        if (msg.startsWith('[zone_list] interpreted name: ')) {
          const raw = msg.replace('[zone_list] interpreted name: ', '');
          console.log('[sw][zone_list name]', raw);
          return;
        }
        console.log('[sw]', msg);
      }
      if (evt?.type === 'swarm_signal') {
        if (swarm) swarm.onSignal(evt).catch(() => {});
      }
      if (evt?.type === 'gateway_service_install_status') {
        handleGatewayServiceInstallStatusEvent(evt);
      }
      if (evt?.type === 'notify') refreshAll().catch(() => {});
    }
  });

  await client.ready().catch((e) => console.error(e));
  clientReady = client.isServiceWorkerAvailable();
  if (clientReady) {
    startSharedRelayPipe(client, 'wss://relay.snort.social');
  } else {
    setDaemonState('offline', 'sw unavailable');
  }

  swarm = new SwarmTransport({ client, onState: (s) => setSwarmState(s) });
  swarm.loadSuccess();
  swarm.loadIdentityCache();
  swarm.loadDeviceCache();
  swarm.loadLastKnown();
  const fallbackPeers = swarm.getFallbackPeers();
  if (fallbackPeers && fallbackPeers.length > 0) {
    swarm.setPeers(fallbackPeers.map(pk => ({ devicePk: pk })));
  }

  wireUi();
  await hydrateAppCatalog();
  _pushConnLog('init');

  // Default radio selection: webauthn if supported
  setSecurityChoice('webauthn');

  try {
    await refreshAll();
    const linked = await ensureOnboardingFlow();
    if (linked) {
      setSettingsTab('profile');
      await postIdentityLinkedFlow();
    } else {
      await applyUrlParams();
    }
  } catch (e) {
    console.error('refreshAll failed', e);
    setDaemonState('offline', 'rpc failed');
    showActivity('onboarding');
    setOnboardStep(1);
  }

  // Health check: if SW drops, fall back to onboarding.
  setInterval(async () => {
    try {
      await client.call('device.getState', {}, { timeoutMs: 5000 });
      setDaemonState('online', 'rpc ok');
    } catch {
      setDaemonState('offline', 'rpc fail');
      showActivity('onboarding');
      setOnboardStep(1);
    }
  }, 10000);
})();
