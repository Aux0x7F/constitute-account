import assert from "node:assert/strict";
import test from "node:test";
import {
  accountRuntimeClientModule,
  accountServiceManagerOperationPosture,
  accountServiceManagerProofDigest,
  accountServiceManagerSecretBoundary,
  accountSurfaceApp,
  accountSurfaceAttachContext,
  accountSurfaceBootstrapContract,
  accountSurfaceBootstrapPosture,
  accountSurfaceModuleRegistry,
  accountSurfaceModules,
  accountSurfaceRunnerPlan,
} from "../surface-app-contract.js";

test("account ui declares its surface app modules before runtime attach", () => {
  assert.equal(accountSurfaceApp.posture.state, "ready");
  assert.equal(accountSurfaceApp.hasRole("runtimeClient"), true);
  assert.equal(accountSurfaceApp.hasRole("projectionModel"), true);
  assert.equal(accountSurfaceApp.hasRole("platformAdapter"), true);
  assert.equal(accountSurfaceApp.hasRole("productView"), true);
  assert.equal(accountSurfaceModuleRegistry.kind, "surface.module.registry");
  assert.equal(accountSurfaceModules.state, "ready");
  assert.equal(typeof accountRuntimeClientModule.createRuntimeSurfaceClient, "function");
  assert.equal(accountSurfaceAttachContext.kind, "surface.app.attachContext");
  assert.equal(accountSurfaceAttachContext.appId, "constitute-account");
  assert.equal(accountSurfaceAttachContext.moduleRefs.length, 4);
  assert.equal(accountSurfaceBootstrapPosture.state, "ready");
  assert.equal(accountSurfaceRunnerPlan.kind, "surface.app.runner.plan");
  assert.equal(accountSurfaceRunnerPlan.state, "ready");
  assert.equal(accountSurfaceBootstrapContract.kind, "surface.app.bootstrap.contract");
  assert.equal(accountSurfaceBootstrapContract.state, "ready");
  assert.equal(accountServiceManagerSecretBoundary.kind, "service.manager.secretBoundary");
  assert.equal(accountServiceManagerSecretBoundary.state, "notRequired");
  assert.equal(accountServiceManagerOperationPosture.kind, "service.manager.operation.posture");
  assert.equal(accountServiceManagerOperationPosture.state, "requested");
  assert.equal(accountServiceManagerProofDigest.kind, "service.manager.proof.digest");
  assert.equal(accountSurfaceAttachContext.runnerPlan, accountSurfaceRunnerPlan);
  assert.equal(accountSurfaceAttachContext.bootstrapContract, accountSurfaceBootstrapContract);
  assert.equal(accountSurfaceAttachContext.serviceManagerOperationPosture, accountServiceManagerOperationPosture);
});
