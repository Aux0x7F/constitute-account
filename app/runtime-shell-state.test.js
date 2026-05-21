import assert from "node:assert/strict";
import test from "node:test";
import {
  browserStorageShellContext,
  deriveRuntimeMaterializationPosture,
  deriveRuntimeShellState,
  runtimeShellConnectionToneClass,
} from "../runtime-shell-state.js";

test("shell state does not render unlinked when identity evidence exists", () => {
  const state = deriveRuntimeShellState({
    buildId: "runtime-test",
    shell: {
      identity: {
        linked: false,
        identityId: "identity-001",
        devicePk: "device-001",
      },
    },
  });

  assert.equal(state.identity.linked, true);
  assert.equal(state.identity.authorityState, "missingAuthority");
  assert.equal(state.identity.handle, "@linked");
  assert.equal(state.connection.code, "connected-limited");
});

test("shell state derives online service posture from edge and retained projections", () => {
  const state = deriveRuntimeShellState({
    buildId: "runtime-test",
    broker: { available: true },
    edge: { connected: true },
    resource: {
      state: "withinBudget",
      profileId: "balanced",
      cleanupAllowed: false,
      cleanupReason: "retention posture must allow release before sweeping",
    },
    materialization: {
      state: "withinBudget",
      budgets: [
        { budgetId: "runtime.projections.retained" },
        { budgetId: "runtime.events.ring" },
      ],
      fanout: 1,
      projectionCount: 1,
      runtimeEventCount: 4,
    },
    retention: {
      state: "releaseRequired",
      reason: "local release requires explicit retention release posture",
      releaseRequired: true,
      destructiveAction: false,
    },
    shell: {
      identity: { linked: true, identityId: "identity-001", label: "operator" },
    },
    serviceCatalog: {
      services: [{ service: "nvr", servicePk: "nvr-service-001" }],
    },
    projections: {
      "nvr.streams": { payload: { sources: ["front"] } },
    },
  });

  assert.equal(state.identity.handle, "@operator");
  assert.equal(state.connection.code, "connected-limited");
  assert.equal(state.connection.toneClass, "connStateText-limited");
  assert.equal(state.gateway.state, "connected");
  assert.equal(state.services.state, "available");
  assert.equal(state.projections.materialized, true);
  assert.equal(state.resource.state, "withinBudget");
  assert.equal(state.resource.cleanupAllowed, false);
  assert.equal(state.materialization.state, "withinBudget");
  assert.equal(state.materialization.budgetCount, 2);
  assert.equal(state.materialization.runtimeEventCount, 4);
  assert.equal(state.retention.state, "releaseRequired");
  assert.equal(state.retention.releaseRequired, true);
});

test("shell state keeps route delivery distinct from adapter live", () => {
  const routed = deriveRuntimeShellState({}, { routeDelivered: true });
  const live = deriveRuntimeShellState({}, { routeDelivered: true, adapterLive: true });

  assert.equal(routed.connection.code, "routed");
  assert.equal(routed.runlevel, "routed");
  assert.equal(routed.interaction.routeDelivered, true);
  assert.equal(routed.interaction.adapterLive, false);
  assert.equal(live.connection.code, "live");
  assert.equal(live.runlevel, "live");
  assert.equal(live.connection.label, "Live");
  assert.equal(live.interaction.adapterLive, true);
  assert.equal(runtimeShellConnectionToneClass("live"), "connStateText-connected");
});

test("shell state exposes runtime materialization budget posture", () => {
  const posture = deriveRuntimeMaterializationPosture({
    runtimeEvents: [{ eventId: "runtime-event-1" }],
    materializationBudget: {
      budgetId: "budget:runtime.snapshot.account",
      state: "withinBudget",
      copyRole: "snapshot-summary",
      payloadClass: "safeFacts",
      privacyTier: "safe",
      limits: {
        estimatedSnapshotBytes: 2048,
        fanout: 1,
        replayLimit: 12,
      },
    },
    materialization: {
      consumerFloor: {
        floorId: "floor:account-ui",
        lagState: "current",
      },
    },
  });

  assert.equal(posture.kind, "runtime.materialization.posture");
  assert.equal(posture.state, "withinBudget");
  assert.equal(posture.budgetId, "budget:runtime.snapshot.account");
  assert.equal(posture.consumerFloorId, "floor:account-ui");
  assert.equal(posture.runtimeEventCount, 1);
  assert.equal(posture.payloadClass, "safeFacts");
  assert.equal(posture.privacyTier, "safe");
});

test("shell state can use retained browser cache as display evidence without raw storage authority", () => {
  const storage = new Map([
    ["swarm.identityCache", JSON.stringify({
      records: [{ identityId: "id-001", label: "Aux" }],
    })],
    ["swarm.deviceCache", JSON.stringify({
      records: [
        { devicePk: "browser-001", identityId: "id-001", role: "browser" },
        {
          devicePk: "gateway-001",
          role: "gateway",
          service: "gateway",
          hostedServices: [
            { devicePk: "nvr-001", service: "nvr", cameraCount: 2 },
          ],
        },
      ],
    })],
  ]);
  const context = browserStorageShellContext({ getItem: (key) => storage.get(key) || null });
  const state = deriveRuntimeShellState({ buildId: "runtime-test" }, { context });

  assert.equal(state.identity.handle, "@Aux");
  assert.equal(state.identity.resolution, "named");
  assert.equal(state.identity.resolvedType, "friendly");
  assert.equal(state.identity.evidenceSource, "browserStorageCache");
  assert.equal(state.identity.authorityPosture, "present");
  assert.equal(state.gateway.state, "known");
  assert.equal(state.services.count, 1);
  assert.equal(state.services.state, "available");
});
