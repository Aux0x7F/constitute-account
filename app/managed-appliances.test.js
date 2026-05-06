import test from 'node:test';
import assert from 'node:assert/strict';

import { createManagedApplianceModel } from './managed-appliances.js';

function makeModel(getSwarmSeen = () => 0) {
  return createManagedApplianceModel({
    applyHostedSnapshot: (record) => record,
    getSwarmSeen,
    applianceDiscoveryMaxAgeMs: 24 * 60 * 60 * 1000,
  });
}

function gatewayRecord({ updatedAt, hostedServices }) {
  return {
    devicePk: 'gateway-pk',
    deviceLabel: 'DevGateway',
    role: 'gateway',
    service: 'none',
    updatedAt,
    hostedServices,
  };
}

function directNvrRecord(updatedAt) {
  return {
    devicePk: 'nvr-pk',
    deviceLabel: 'Constitute NVR',
    deviceKind: 'service',
    role: 'native',
    service: 'nvr',
    hostGatewayPk: 'gateway-pk',
    updatedAt,
    uiRepo: 'constitute-nvr-ui',
  };
}

function hostedNvrRecord(updatedAt) {
  return {
    devicePk: 'nvr-pk',
    servicePk: 'nvr-pk',
    deviceLabel: 'Constitute NVR',
    deviceKind: 'service',
    service: 'nvr',
    hostGatewayPk: 'gateway-pk',
    serviceVersion: '0.1.0',
    updatedAt,
    status: 'online',
    cameraCount: 2,
  };
}

function directLoggingRecord(updatedAt) {
  return {
    devicePk: 'logging-device-pk',
    servicePk: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    deviceLabel: 'Constitute Logging',
    deviceKind: 'service',
    role: 'native',
    service: 'logging',
    hostGatewayPk: 'gateway-pk',
    updatedAt,
  };
}

test('gateway-hosted service projection replaces stale direct nvr freshness', () => {
  const now = Date.now();
  const model = makeModel();
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [
      gatewayRecord({ updatedAt: now - 2 * 60 * 1000, hostedServices: [hostedNvrRecord(now - 2 * 60 * 1000)] }),
      directNvrRecord(now - 6 * 60 * 60 * 1000),
    ],
  );

  const nvr = recs.find((record) => record.devicePk === 'nvr-pk');
  assert.ok(nvr, 'expected merged nvr record');
  assert.equal(recs.filter((record) => record.devicePk === 'nvr-pk').length, 1);
  assert.equal(nvr.managedAvailabilityAuthority, 'gateway');
  assert.equal(nvr.updatedAt, now - 2 * 60 * 1000);
  assert.equal(nvr.status, 'online');
  assert.equal(model.applianceSeenAt(nvr), now - 2 * 60 * 1000);
});

test('gateway authority wins even when direct nvr presence looks fresher', () => {
  const now = Date.now();
  const model = makeModel(() => now);
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [
      gatewayRecord({ updatedAt: now - 35 * 60 * 1000, hostedServices: [hostedNvrRecord(now - 35 * 60 * 1000)] }),
      directNvrRecord(now - 30 * 1000),
    ],
  );

  const nvr = recs.find((record) => record.devicePk === 'nvr-pk');
  assert.ok(nvr, 'expected merged nvr record');
  assert.equal(nvr.managedAvailabilityAuthority, 'gateway');
  assert.equal(model.applianceSeenAt(nvr), now - 35 * 60 * 1000);
});

test('direct nvr record remains usable when no gateway-hosted projection exists', () => {
  const now = Date.now();
  const model = makeModel();
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [directNvrRecord(now - 45 * 1000)],
  );

  const nvr = recs.find((record) => record.devicePk === 'nvr-pk');
  assert.ok(nvr, 'expected direct nvr record');
  assert.equal(nvr.managedAvailabilityAuthority, undefined);
  assert.equal(model.applianceSeenAt(nvr), now - 45 * 1000);
});

test('direct generic hosted service record remains usable when no gateway-hosted projection exists', () => {
  const now = Date.now();
  const model = makeModel();
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [directLoggingRecord(now - 30 * 1000)],
  );

  const logging = recs.find((record) => record.service === 'logging');
  assert.ok(logging, 'expected direct logging service record');
  assert.equal(model.isManagedServiceRecord(logging), true);
  assert.equal(model.managedGatewayPkForRecord(logging), 'gateway-pk');
  assert.equal(model.managedServicePkForRecord(logging), 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  assert.equal(model.applianceSeenAt(logging), now - 30 * 1000);
});

test('gateway freshness considers every hosted service, not only nvr', () => {
  const now = Date.now();
  const model = makeModel();
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [
      gatewayRecord({ updatedAt: now - 30 * 60 * 1000, hostedServices: [] }),
      directLoggingRecord(now - 20 * 1000),
    ],
  );

  const gateway = recs.find((record) => record.devicePk === 'gateway-pk');
  assert.ok(gateway, 'expected gateway record');
  assert.equal(model.effectiveApplianceSeenAt(gateway, recs), now - 20 * 1000);
});

test('hosted service keeps list identity separate from cryptographic service identity', () => {
  const now = Date.now();
  const model = makeModel();
  const hostedLogging = {
    devicePk: 'logging:gateway-pk',
    servicePk: '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
    deviceLabel: 'Constitute Logging',
    deviceKind: 'service',
    service: 'logging',
    hostGatewayPk: 'gateway-pk',
    updatedAt: now,
    status: 'online',
  };
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [gatewayRecord({ updatedAt: now, hostedServices: [hostedLogging] })],
  );

  const logging = recs.find((record) => record.service === 'logging');
  assert.ok(logging, 'expected hosted logging record');
  assert.equal(logging.devicePk, 'logging:gateway-pk');
  assert.equal(model.managedGatewayPkForRecord(logging), 'gateway-pk');
  assert.equal(model.managedServicePkForRecord(logging), hostedLogging.servicePk);
});

test('hosted service without service public key does not use synthetic list identity for access', () => {
  const now = Date.now();
  const model = makeModel();
  const hostedLogging = {
    devicePk: 'logging:gateway-pk',
    deviceLabel: 'Constitute Logging',
    deviceKind: 'service',
    service: 'logging',
    hostGatewayPk: 'gateway-pk',
    updatedAt: now,
    status: 'online',
  };
  const recs = model.buildApplianceRecords(
    [{ pk: 'gateway-pk' }],
    [gatewayRecord({ updatedAt: now, hostedServices: [hostedLogging] })],
  );

  const logging = recs.find((record) => record.service === 'logging');
  assert.ok(logging, 'expected hosted logging record');
  assert.equal(logging.devicePk, 'logging:gateway-pk');
  assert.equal(model.managedServicePkForRecord(logging), '');
});
