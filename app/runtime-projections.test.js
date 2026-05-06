import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const source = readFileSync(resolve(here, '../app.js'), 'utf8');
const workerSource = readFileSync(resolve(here, '../runtime.worker.js'), 'utf8');

test('logging service projection can discover retained runtime service records before live identity refresh', () => {
  assert.match(source, /function runtimeOwnedDevicePks\(/);
  assert.match(source, /\.\.\.runtimeOwnedDevicePks\(\)/);
  assert.match(source, /findManagedServiceRecordForProjection/);
  assert.match(source, /managedServicePkForRecord\(rec\)/);
  assert.match(source, /function retainedProjectionServiceRecord\(/);
  assert.match(source, /async function discoverManagedServiceRecordForProjection\(/);
  assert.match(source, /await Promise\.all\(gatewayPks\.map\(\(gatewayPk\) => requestGatewayInventoryRefresh\(gatewayPk, \{ force: true \}\)/);
  assert.match(source, /runtimeStatusSnapshot\?\.projections/);
  assert.match(source, /servicePk: projectedService\.servicePk/);
  assert.match(source, /function withManagedServiceAccessKeys\(/);
  assert.match(source, /withManagedServiceAccessKeys\(\s+await resolveManagedServiceForAccess\(record, \{ serviceLabel, requireGatewayAuthority: false \}\),\s+record,\s+\)/);
});

test('logging service projection repair uses bounded timeouts', () => {
  assert.match(source, /PROJECTION_SERVICE_ACCESS_TIMEOUT_MS/);
  assert.match(source, /PROJECTION_SIGNAL_REQUEST_TIMEOUT_MS/);
  assert.match(source, /timeoutMs: PROJECTION_SERVICE_ACCESS_TIMEOUT_MS/);
  assert.match(source, /timeoutMs: PROJECTION_SIGNAL_REQUEST_TIMEOUT_MS/);
  assert.match(source, /policy: req\?\.policy && typeof req\.policy === 'object' \? req\.policy : \{\}/);
});

test('account inventory refresh tracks every managed service surface', () => {
  assert.match(source, /const MANAGED_APP_SURFACES = Object\.freeze\(\{\s+nvr: 'constitute-nvr-ui',\s+logging: 'constitute-logging-ui',\s+\}\)/);
  assert.match(source, /Object\.keys\(MANAGED_APP_SURFACES\)\.some\(\(service\) => !findGatewayHostedServiceRecord\(info\.pk, applianceRecords, service\)\)/);
  assert.doesNotMatch(source, /!findGatewayHostedServiceRecord\(info\.pk, applianceRecords, 'nvr'\)/);
});

test('account bridge and shared runtime use the same worker build id', () => {
  assert.match(source, /const PLATFORM_RUNTIME_VERSION = Object\.freeze\(\{ major: 2, minor: 12 \}\)/);
  assert.match(workerSource, /const RUNTIME_VERSION = Object\.freeze\(\{ major: 2, minor: 12 \}\)/);
  assert.match(source, /target\.searchParams\.set\('v', PLATFORM_RUNTIME_BUILD_ID\)/);
  assert.match(source, /name: `constitute-account-runtime-\$\{PLATFORM_RUNTIME_BUILD_ID\}`/);
});

test('runtime worker does not block local projection and status messages on hydration', () => {
  assert.doesNotMatch(workerSource, /await ensureHydrated\(\);/);
  assert.match(workerSource, /A slow gateway\/relay path should\s+\/\/ never make local runtime put\/get messages time out\./);
  assert.match(workerSource, /case 'runtime\.status\.put':/);
  assert.match(workerSource, /case 'managedAppliances\.sourceSnapshot\.put':/);
  assert.match(workerSource, /case BROKER\.PROJECTION_PUT:/);
  assert.match(workerSource, /case BROKER\.SERVICE_PROJECTION_REQUEST:/);
});

test('runtime snapshots expose broker availability without leaking request payloads', () => {
  assert.match(workerSource, /broker: \{\s+available: Boolean\(brokerEndpoint\),\s+surface: String\(brokerEndpoint\?\.surface \|\| ''\)\.trim\(\),\s+\}/);
  assert.match(workerSource, /const brokerWasAvailable = Boolean\(brokerClientId && endpoints\.get\(brokerClientId\)\)/);
  assert.match(workerSource, /if \(brokerWasAvailable !== brokerIsAvailable \|\| entry\?\.broker\) \{\s+broadcastSnapshot\(\);\s+scheduleProjectionSync\(0\);\s+\}/);
  assert.doesNotMatch(workerSource, /broker: \{[^}]*payload/s);
});

test('runtime worker persists retained projections under service identity keys', () => {
  assert.match(workerSource, /const key = projectionStoreKey\(stored\) \|\| stored\.channelId/);
  assert.match(workerSource, /function projectionPolicyId\(/);
  assert.match(workerSource, /\[servicePk \|\| service, channelId, policyId\]/);
  assert.match(workerSource, /retainedProjections\.set\(key,/);
  assert.match(workerSource, /function retainedProjectionObject\(/);
  assert.match(workerSource, /function retainedProjectionStoreObject\(/);
  assert.match(workerSource, /function retainedProjectionCoverageObject\(/);
  assert.match(workerSource, /projections: retainedProjectionStoreObject\(\)/);
  assert.match(workerSource, /projectionCoverage: retainedProjectionCoverageObject\(\)/);
  assert.match(workerSource, /function mergeProjectionRecord\(/);
  assert.match(workerSource, /function mergeProjectionEvents\(/);
  assert.match(workerSource, /function projectionReplacesEventSet\(/);
  assert.match(workerSource, /replacesEventSet\s+\?\s+nextEvents\.slice\(\)\.sort/);
  assert.match(workerSource, /materializedCount: mergedEvents\.length/);
  assert.match(workerSource, /function projectionSemanticallyEqual\(/);
  assert.match(workerSource, /if \(existing && projectionSemanticallyEqual\(existing, storedProjection\)\) \{/);
  assert.match(workerSource, /projection\.observer\.update/);
  assert.match(workerSource, /result: \{\s+\.\.\.\(message\.result && typeof message\.result === 'object' \? message\.result : \{\}\),\s+projection: stored,/);
  assert.match(workerSource, /if \(channelId && !out\[channelId\]\) out\[channelId\] = cloned/);
});

test('runtime worker owns projection policy sync instead of UI request assembly', () => {
  assert.match(workerSource, /const PROJECTION_POLICY_PUT = 'projection\.policy\.put'/);
  assert.match(workerSource, /const projectionPolicies = new Map\(\)/);
  assert.match(workerSource, /const pendingProjectionSyncRequests = new Map\(\)/);
  assert.match(workerSource, /function handleProjectionPolicyPut\(/);
  assert.match(workerSource, /function startProjectionSync\(/);
  assert.match(workerSource, /function queueProjectionSyncRequest\(/);
  assert.match(workerSource, /sourceClientId: 'runtime-projection-sync'/);
  assert.match(workerSource, /case PROJECTION_POLICY_PUT:/);
  assert.match(workerSource, /function handleRuntimeProjectionSyncResponse\(/);
  assert.match(workerSource, /projection\.sync\.diagnostic/);
});
