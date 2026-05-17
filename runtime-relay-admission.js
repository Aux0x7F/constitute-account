import { SWARM, assertEventAdmissionEnvelope, assertIngressLanePosture } from 'constitute-protocol';

export const RELAY_RX_FORWARD_TIMEOUT_MS = 5_000;
export const RELAY_RX_FORWARD_MAX_IN_FLIGHT = 2;
export const RELAY_RX_FORWARD_QUEUE_LIMIT = 32;

const RELAY_RX_ACCEPT_WINDOW_SEC = 10 * 60;
const RELAY_RX_FUTURE_SKEW_SEC = 2 * 60;
const RELAY_RX_APP_TAG = 'constitute';
const RELAY_RX_DISCOVERY_TAG = 'swarm_discovery';
const RELAY_RX_LANE_PRIORITY = Object.freeze({
  authority: 0,
  route: 10,
  activation: 20,
  projectionRepair: 30,
  projection: 40,
  directory: 45,
  account: 50,
  diagnostic: 70,
  loggingReplay: 80,
  bulkRetainedData: 90,
  invalid: 100,
  irrelevant: 100,
  staleReplay: 100,
  futureReplay: 100,
  gatewayControl: 100,
});
const RELAY_RX_APP_PAYLOAD_TYPES = new Set([
  'device_blocked',
  'device_unblocked',
  'zone_presence',
  'zone_list',
  'zone_list_request',
  'zone_meta',
  'zone_meta_request',
  'zone_joined',
  'zone_probe',
  'identity_label_update',
  'device_label_update',
  'pair_claim',
  'pair_request',
  'pair_approve',
  'pair_reject',
  'pair_resolved',
  'notifications_clear',
  'room_key_update',
  'device_revoked',
  'swarm_identity_record',
  'swarm_device_record',
  'swarm_dht_record',
  'swarm_record_request',
  'swarm_dht_get',
  'swarm_dht_put',
  'swarm_record_response',
  'gateway_service_install_status',
  'gateway_zone_sync_status',
  'swarm_signal',
]);
const RELAY_RX_SELF_ECHO_PAYLOAD_TYPES = new Set([
  'zone_presence',
  'zone_list',
  'zone_list_request',
  'zone_meta',
  'zone_meta_request',
  'zone_probe',
]);

function randomAdmissionId(prefix) {
  const bytes = new Uint8Array(8);
  const cryptoApi = globalThis.crypto;
  if (cryptoApi?.getRandomValues) {
    cryptoApi.getRandomValues(bytes);
    return `${prefix}-${Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')}`;
  }
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export function relayRxLanePriority(lane) {
  const priority = RELAY_RX_LANE_PRIORITY[String(lane || '')];
  return Number.isFinite(priority) ? priority : 90;
}

export function relayRxIngressLanePosture({
  lane,
  laneKind = 'relay',
  state = SWARM.RESOURCE_POSTURE_STATE.WITHIN_BUDGET,
  counts = {},
  limits = {},
  relevanceRefs = [],
  blockedReasons = [],
  sampledAt = Date.now(),
} = {}) {
  const laneKey = String(lane || 'unknown').replace(/^relay\./, '') || 'unknown';
  return assertIngressLanePosture({
    kind: SWARM.RECORD_KIND.INGRESS_LANE_POSTURE,
    laneId: `relay.${laneKey}`,
    laneKind,
    priority: relayRxLanePriority(laneKey),
    state,
    counts,
    limits,
    relevanceRefs,
    blockedReasons,
    sampledAt,
  });
}

function relayRxLanePlane(lane) {
  if (lane === 'authority') return 'authority';
  if (lane === 'route') return 'route';
  if (lane === 'activation') return 'activation';
  if (lane === 'projectionRepair') return 'projectionRepair';
  if (lane === 'projection' || lane === 'directory') return 'projection';
  if (lane === 'diagnostic') return 'diagnostic';
  if (lane === 'loggingReplay') return 'loggingReplay';
  if (lane === 'bulkRetainedData') return 'bulkRetainedData';
  return 'diagnostic';
}

function makeRelayRxAdmission({ accepted, lane, reason, event = null, payload = null, subject = null, context = {}, observedAt = Date.now() }) {
  const priority = relayRxLanePriority(lane);
  const eventId = String(event?.id || '').trim();
  const decision = accepted
    ? SWARM.EVENT_ADMISSION_DECISION.FORWARD
    : SWARM.EVENT_ADMISSION_DECISION.DROP;
  const claimedSeverity = String(payload?.severity || payload?.level || '').trim();
  const subscriberRef = String(context.subscriberRef || context.memberRef || context.ownerId || '').trim();
  const memberRef = String(context.memberRef || '').trim();
  const identity = String(context.identity || context.identityLabel || '').trim();
  const surface = String(context.surface || 'constitute-account').trim();
  const proofRequirement = accepted
    ? SWARM.EVENT_PROOF_REQUIREMENT.SIGNATURE
    : SWARM.EVENT_PROOF_REQUIREMENT.NONE;
  const proofState = accepted
    ? SWARM.EVENT_PROOF_STATE.PENDING
    : SWARM.EVENT_PROOF_STATE.NOT_REQUIRED;
  return {
    accepted: Boolean(accepted),
    lane,
    reason,
    priority,
    admission: assertEventAdmissionEnvelope({
      kind: SWARM.RECORD_KIND.EVENT_ADMISSION,
      admissionId: eventId ? `relay:${eventId}` : randomAdmissionId('relay-admission'),
      plane: relayRxLanePlane(lane),
      laneId: `relay.${lane}`,
      publisherRef: String(event?.pubkey || '').trim() || 'relay:unknown',
      subscriberRef,
      subject: subject || {
        relayEventId: eventId,
        relayKind: Number(event?.kind || 0) || undefined,
        payloadType: String(payload?.type || '').trim() || undefined,
      },
      audience: {
        memberRef: memberRef || undefined,
        identity: identity || undefined,
        surface,
      },
      claimedSeverity: claimedSeverity || undefined,
      effectivePriority: priority,
      decision,
      proofRequirement,
      proofState,
      reason,
      observedAt,
      expiresAt: observedAt + RELAY_RX_FORWARD_TIMEOUT_MS,
    }),
  };
}

function relayEventHasTag(event, tagValue) {
  const tags = Array.isArray(event?.tags) ? event.tags : [];
  return tags.some((tag) => Array.isArray(tag) && tag[0] === 't' && tag[1] === tagValue);
}

function relayEventTagValue(event, tagName) {
  const tags = Array.isArray(event?.tags) ? event.tags : [];
  const found = tags.find((tag) => Array.isArray(tag) && tag[0] === tagName);
  return String(found?.[1] || '').trim();
}

function relayPayloadTargetsSubscriber(payload, context = {}) {
  const memberRef = String(context.memberRef || '').trim();
  const identityLabel = String(context.identity || context.identityLabel || '').trim();
  const toPk = String(payload?.toPk || payload?.toDevicePk || payload?.targetPk || '').trim();
  if (toPk && memberRef && toPk !== memberRef) return false;
  const payloadIdentity = String(payload?.identity || payload?.identityLabel || '').trim();
  if (payloadIdentity && identityLabel && payloadIdentity !== identityLabel) return false;
  const zoneKeys = new Set([
    ...(Array.isArray(context.zoneKeys) ? context.zoneKeys : []),
    ...(Array.isArray(context.zones) ? context.zones : []),
  ].map((value) => String(value?.key || value || '').trim()).filter(Boolean));
  const payloadZones = [
    payload?.zone,
    ...(Array.isArray(payload?.zoneKeys) ? payload.zoneKeys : []),
  ].map((value) => String(value || '').trim()).filter(Boolean);
  if (zoneKeys.size > 0 && payloadZones.length > 0 && !payloadZones.some((zone) => zoneKeys.has(zone))) {
    return false;
  }
  return true;
}

export function relayRxIngressClassification(data, options = {}) {
  const observedAt = Number(options.observedAt || 0) || Date.now();
  const context = options.context && typeof options.context === 'object' ? options.context : {};
  let parsed = null;
  try {
    parsed = JSON.parse(String(data || ''));
  } catch {
    return makeRelayRxAdmission({ accepted: false, lane: 'invalid', reason: 'invalidJson', context, observedAt });
  }
  if (!Array.isArray(parsed) || parsed[0] !== 'EVENT') {
    return makeRelayRxAdmission({ accepted: false, lane: 'invalid', reason: 'notEvent', context, observedAt });
  }
  const event = parsed[2];
  if (!event || typeof event !== 'object') {
    return makeRelayRxAdmission({ accepted: false, lane: 'invalid', reason: 'missingEvent', context, observedAt });
  }
  const createdAt = Number(event.created_at || 0);
  if (!createdAt) return makeRelayRxAdmission({ accepted: false, lane: 'invalid', reason: 'missingCreatedAt', event, context, observedAt });
  const now = Math.floor(observedAt / 1000);
  if (createdAt < now - RELAY_RX_ACCEPT_WINDOW_SEC) {
    return makeRelayRxAdmission({ accepted: false, lane: 'staleReplay', reason: 'tooOld', event, context, observedAt });
  }
  if (createdAt > now + RELAY_RX_FUTURE_SKEW_SEC) {
    return makeRelayRxAdmission({ accepted: false, lane: 'futureReplay', reason: 'futureSkew', event, context, observedAt });
  }
  if (typeof options.isAppHandled === 'function' && options.isAppHandled(data, event)) {
    return makeRelayRxAdmission({ accepted: false, lane: 'gatewayControl', reason: 'handledByApp', event, context, observedAt });
  }
  const hasAppTag = relayEventHasTag(event, RELAY_RX_APP_TAG);
  const hasDiscoveryTag = relayEventHasTag(event, RELAY_RX_DISCOVERY_TAG);
  const eventKind = Number(event.kind || 0);
  if (eventKind === 30078 && hasDiscoveryTag) {
    const recType = relayEventTagValue(event, 'type');
    if (['identity', 'device', 'dht'].includes(recType)) {
      return makeRelayRxAdmission({
        accepted: true,
        lane: 'directory',
        reason: recType,
        event,
        subject: { relayEventId: String(event.id || '').trim(), relayKind: eventKind, discoveryType: recType },
        context,
        observedAt,
      });
    }
    return makeRelayRxAdmission({ accepted: false, lane: 'directory', reason: 'unsupportedDiscoveryType', event, context, observedAt });
  }
  if (!hasAppTag) return makeRelayRxAdmission({ accepted: false, lane: 'irrelevant', reason: 'missingAppTag', event, context, observedAt });
  let payload = null;
  try {
    payload = JSON.parse(String(event.content || ''));
  } catch {
    return makeRelayRxAdmission({ accepted: false, lane: 'invalid', reason: 'invalidPayloadJson', event, context, observedAt });
  }
  const type = String(payload?.type || '').trim();
  if (!RELAY_RX_APP_PAYLOAD_TYPES.has(type)) {
    return makeRelayRxAdmission({ accepted: false, lane: 'irrelevant', reason: 'unsupportedPayloadType', event, payload, context, observedAt });
  }
  const senderPk = String(event.pubkey || '').trim();
  const memberRef = String(context.memberRef || '').trim();
  if (senderPk && memberRef && senderPk === memberRef && RELAY_RX_SELF_ECHO_PAYLOAD_TYPES.has(type)) {
    return makeRelayRxAdmission({ accepted: false, lane: 'account', reason: 'selfEcho', event, payload, context, observedAt });
  }
  if (!relayPayloadTargetsSubscriber(payload, context)) {
    return makeRelayRxAdmission({ accepted: false, lane: 'irrelevant', reason: 'targetMismatch', event, payload, context, observedAt });
  }
  const lane = type.startsWith('pair_') || type === 'pair_claim'
    ? 'authority'
    : type.startsWith('gateway_') || type === 'swarm_signal'
      ? 'route'
      : type.startsWith('swarm_')
        ? 'projection'
        : 'account';
  return makeRelayRxAdmission({ accepted: true, lane, reason: type, event, payload, context, observedAt });
}
