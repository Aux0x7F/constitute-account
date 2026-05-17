import test from 'node:test';
import assert from 'node:assert/strict';
import {
  RUNTIME_DIAGNOSTICS_COMMAND,
  RUNTIME_DIAGNOSTIC_OPERATOR_PLANES,
  RUNTIME_DIAGNOSTICS_STORAGE_KEY,
  RUNTIME_DIAGNOSTICS_SUBSCRIBE,
  attachRuntimeDiagnostics,
  runtimeDiagnosticsEnabled,
} from '../runtime-diagnostics.js';

function makeWindow(url = 'http://localhost/?debug=1') {
  const storage = new Map();
  const sessionStorage = new Map();
  const cacheDeletes = [];
  const registrations = [
    { unregistered: false, async unregister() { this.unregistered = true; return true; } },
  ];
  let indexedDbTouched = false;
  return {
    indexedDB: {
      open() { indexedDbTouched = true; throw new Error('hardRefresh must not touch IndexedDB'); },
      deleteDatabase() { indexedDbTouched = true; throw new Error('hardRefresh must not delete IndexedDB'); },
    },
    indexedDbTouched() { return indexedDbTouched; },
    location: {
      href: url,
      search: new URL(url).search,
      reloadCount: 0,
      replacedWith: '',
      reload() { this.reloadCount += 1; },
      replace(nextUrl) {
        this.replacedWith = String(nextUrl);
        this.href = String(nextUrl);
        this.search = new URL(nextUrl).search;
      },
    },
    caches: {
      deleted: cacheDeletes,
      async keys() { return ['constitute-shell-cache', 'constitute-runtime-cache']; },
      async delete(key) { cacheDeletes.push(String(key)); return true; },
    },
    navigator: {
      serviceWorker: {
        registrations,
        async getRegistrations() { return registrations; },
      },
    },
    localStorage: {
      getItem(key) { return storage.has(key) ? storage.get(key) : null; },
      setItem(key, value) { storage.set(key, String(value)); },
      removeItem(key) { storage.delete(key); },
    },
    sessionStorage: {
      getItem(key) { return sessionStorage.has(key) ? sessionStorage.get(key) : null; },
      setItem(key, value) { sessionStorage.set(key, String(value)); },
      removeItem(key) { sessionStorage.delete(key); },
    },
    setTimeout,
    clearTimeout,
    dispatches: [],
    dispatchEvent(event) { this.dispatches.push(event); },
  };
}

function makePort() {
  return {
    messages: [],
    postMessage(message) {
      this.messages.push(message);
    },
  };
}

test('runtime diagnostics agent enables from debug query and subscribes to shared worker events', async () => {
  const win = makeWindow();
  const port = makePort();
  const lines = [];
  const agent = attachRuntimeDiagnostics({
    window: win,
    port,
    surface: 'test-ui',
    clientId: 'test-ui',
    console: { debug: (...args) => lines.push(args), warn: (...args) => lines.push(args), error: (...args) => lines.push(args) },
  });
  assert.equal(agent.enabled, true);
  assert.equal(win.localStorage.getItem(RUNTIME_DIAGNOSTICS_STORAGE_KEY), '1');
  assert.equal(port.messages[0].type, RUNTIME_DIAGNOSTICS_SUBSCRIBE);
  assert.equal(port.messages[0].logging, false);
  assert.equal(port.messages[0].subscription.kind, 'subscription.contract');
  assert.equal(port.messages[0].subscription.window.replayLimit, 80);
  const handled = agent.handleMessage({
    type: 'runtime.diagnostic.event',
    diagnostics: { runtimeSessionId: 'runtime-session-test' },
    event: {
      recordKind: 'runtime.diagnostic.event',
      eventId: 'event-1',
      kind: 'route.observation',
      level: 'warn',
      observedAt: Date.now(),
      buildId: 'runtime-2.25',
      runtimeSessionId: 'runtime-session-test',
      safeFacts: { state: 'observingUnreachable' },
    },
  });
  assert.equal(handled, true);
  assert.equal(win.__constituteRuntimeDiagnostics.length, 1);
  assert.match(lines[0][0], /runtime diagnostic/);
  assert.deepEqual(await win.__constituteDebug.command('routeExplain'), win.__constituteRuntimeDiagnostics);
});

test('runtime diagnostics forwards activation posture command', async () => {
  const win = makeWindow();
  const port = makePort();
  const agent = attachRuntimeDiagnostics({
    window: win,
    port,
    surface: 'test-ui',
    clientId: 'test-ui',
    console: { debug() {}, warn() {}, error() {} },
  });

  agent.handleMessage({
    type: 'runtime.diagnostics.events',
    diagnostics: { runtimeSessionId: 'runtime-session-test' },
    events: [],
  });
  const resultPromise = win.__constituteDebug.command('activationExplain', { activationId: 'activation-test' });
  const command = port.messages.find((entry) => entry.type === RUNTIME_DIAGNOSTICS_COMMAND && entry.command === 'activationExplain');
  assert.ok(command);
  assert.equal(command.audienceRuntimeSessionId, 'runtime-session-test');
  agent.handleMessage({
    type: 'runtime.response',
    kind: RUNTIME_DIAGNOSTICS_COMMAND,
    requestId: command.requestId,
    ok: true,
    result: {
      'activation-test': {
        state: 'answerMaterialized',
        serviceAccepted: true,
      },
    },
  });
  assert.deepEqual(await resultPromise, {
    'activation-test': {
      state: 'answerMaterialized',
      serviceAccepted: true,
    },
  });
});

test('runtime diagnostics logging sink requires explicit opt-in', () => {
  const win = makeWindow();
  const port = makePort();
  const agent = attachRuntimeDiagnostics({
    window: win,
    port,
    surface: 'test-ui',
    clientId: 'test-ui',
    logging: true,
    console: { debug() {}, warn() {}, error() {} },
  });
  assert.equal(agent.enabled, true);
  assert.equal(port.messages[0].type, RUNTIME_DIAGNOSTICS_SUBSCRIBE);
  assert.equal(port.messages[0].logging, true);
});

test('runtime diagnostics replay populates ring without fresh console warnings', () => {
  const win = makeWindow();
  const port = makePort();
  const lines = [];
  const agent = attachRuntimeDiagnostics({
    window: win,
    port,
    surface: 'test-ui',
    clientId: 'test-ui',
    console: { debug: (...args) => lines.push(args), warn: (...args) => lines.push(args), error: (...args) => lines.push(args) },
  });
  agent.handleMessage({
    type: 'runtime.diagnostics.events',
    runtimeSessionId: 'runtime-session-test',
    delivery: { mode: 'replay', replayedCount: 1 },
    materializationBudget: {
      kind: 'materialization.budget',
      budgetId: 'materialization:test-ui:runtime-diagnostics',
      consumerFloor: {
        kind: 'consumer.floor',
        floorId: 'floor:materialization:test-ui:runtime-diagnostics',
      },
    },
    consumerFloor: {
      kind: 'consumer.floor',
      floorId: 'floor:materialization:test-ui:runtime-diagnostics',
    },
    events: [{
      recordKind: 'runtime.diagnostic.event',
      eventId: 'event-replay',
      kind: 'adapter.edge.error',
      level: 'error',
      observedAt: Date.now(),
      buildId: 'runtime-2.56',
      runtimeSessionId: 'runtime-session-test',
      safeFacts: { state: 'closed' },
    }],
  });
  assert.equal(win.__constituteRuntimeDiagnostics.length, 1);
  assert.equal(lines.length, 0);
  assert.equal(agent.materializationBudget.budgetId, 'materialization:test-ui:runtime-diagnostics');
  assert.equal(agent.consumerFloor.floorId, 'floor:materialization:test-ui:runtime-diagnostics');
  assert.equal(agent.localMaterializationBudget.kind, 'materialization.budget');
  assert.equal(agent.localMaterializationBudget.copyRole, 'debug');
  assert.equal(agent.localMaterializationBudget.consumerFloor.kind, 'consumer.floor');

  agent.handleMessage({
    type: 'runtime.diagnostic.event',
    runtimeSessionId: 'runtime-session-test',
    event: {
      recordKind: 'runtime.diagnostic.event',
      eventId: 'event-live',
      kind: 'adapter.edge.error',
      level: 'error',
      observedAt: Date.now(),
      buildId: 'runtime-2.56',
      runtimeSessionId: 'runtime-session-test',
      safeFacts: { state: 'closed' },
    },
  });
  assert.equal(win.__constituteRuntimeDiagnostics.length, 2);
  assert.equal(lines.length, 1);
  assert.match(lines[0][0], /runtime diagnostic/);
  assert.equal(agent.localMaterializationBudget.consumerFloor.ackFloor, '2');
});

test('runtime diagnostics agent sends subscription posture for event-plane filtering', () => {
  const win = makeWindow();
  const port = makePort();
  attachRuntimeDiagnostics({
    window: win,
    port,
    surface: 'test-ui',
    clientId: 'test-ui',
    planes: RUNTIME_DIAGNOSTIC_OPERATOR_PLANES,
    minLevelByPlane: { diagnostic: 'warn' },
    denyKinds: ['projection.applied'],
    limit: 12,
    console: { debug() {}, warn() {}, error() {} },
  });
  assert.deepEqual(port.messages[0].subscription.planes, RUNTIME_DIAGNOSTIC_OPERATOR_PLANES);
  assert.equal(port.messages[0].subscription.cost.minLevelByPlane.diagnostic, 'warn');
  assert.deepEqual(port.messages[0].subscription.cost.denyKinds, ['projection.applied']);
  assert.equal(port.messages[0].limit, 12);
});

test('runtime diagnostics agent exposes only allowlisted local and forwarded commands', async () => {
  const win = makeWindow();
  const port = makePort();
  const agent = attachRuntimeDiagnostics({
    window: win,
    port,
    surface: 'test-ui',
    clientId: 'test-ui',
    console: { debug() {}, warn() {}, error() {} },
  });
  win.localStorage.setItem('device', '{"pk":"keep"}');
  win.sessionStorage.setItem('runtime-session', 'keep');
  const refresh = await agent.command('hardRefresh');
  assert.equal(refresh.ok, true);
  assert.equal(refresh.cacheBust, true);
  assert.deepEqual(refresh.cacheKeysDeleted, ['constitute-shell-cache', 'constitute-runtime-cache']);
  assert.equal(refresh.serviceWorkersUnregistered, 0);
  assert.equal(win.navigator.serviceWorker.registrations[0].unregistered, false);
  assert.equal(win.location.reloadCount, 0);
  assert.match(win.location.replacedWith, /__constituteHardRefresh=/);
  assert.equal(win.localStorage.getItem('device'), '{"pk":"keep"}');
  assert.equal(win.sessionStorage.getItem('runtime-session'), 'keep');
  assert.equal(win.indexedDbTouched(), false);
  await assert.rejects(() => agent.command('eval', {}), /not allowlisted/);
  const pending = agent.command('flushDiagnosticsToLogging', { limit: 1 });
  const commandMessage = port.messages.find((message) => message.type === RUNTIME_DIAGNOSTICS_COMMAND);
  assert.equal(commandMessage.command, 'flushDiagnosticsToLogging');
  assert.ok(commandMessage.nonce);
  agent.handleMessage({
    type: 'runtime.response',
    kind: RUNTIME_DIAGNOSTICS_COMMAND,
    requestId: commandMessage.requestId,
    ok: true,
    result: { ok: true },
  });
  assert.deepEqual(await pending, { ok: true });

  const samplePending = agent.command('resourceSample');
  const sampleMessage = port.messages.filter((message) => message.type === RUNTIME_DIAGNOSTICS_COMMAND).at(-1);
  assert.equal(sampleMessage.command, 'resourceSample');
  assert.ok(sampleMessage.nonce);
  agent.handleMessage({
    type: 'runtime.response',
    kind: RUNTIME_DIAGNOSTICS_COMMAND,
    requestId: sampleMessage.requestId,
    ok: true,
    result: { ok: true, posture: { state: 'withinBudget' } },
  });
  assert.deepEqual(await samplePending, { ok: true, posture: { state: 'withinBudget' } });
});
