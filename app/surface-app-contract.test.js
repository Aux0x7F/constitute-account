import assert from "node:assert/strict";
import test from "node:test";
import {
  accountSurfaceApp,
  accountSurfaceAttachContext,
} from "../surface-app-contract.js";

test("account ui declares its surface app modules before runtime attach", () => {
  assert.equal(accountSurfaceApp.posture.state, "ready");
  assert.equal(accountSurfaceApp.hasRole("runtimeClient"), true);
  assert.equal(accountSurfaceApp.hasRole("projectionModel"), true);
  assert.equal(accountSurfaceApp.hasRole("platformAdapter"), true);
  assert.equal(accountSurfaceApp.hasRole("productView"), true);
  assert.equal(accountSurfaceAttachContext.kind, "surface.app.attachContext");
  assert.equal(accountSurfaceAttachContext.appId, "constitute-account");
  assert.equal(accountSurfaceAttachContext.moduleRefs.length, 4);
});
