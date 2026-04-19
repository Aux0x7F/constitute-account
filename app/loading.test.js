import test from "node:test";
import assert from "node:assert/strict";

import {
  SHELL_BOOT_FALLBACK_TIMEOUT_MS,
  SHELL_BOOT_RUNTIME_ATTACH_TIMEOUT_MS,
  SW_CONTROLLER_GRACE_MS,
  SW_READY_GRACE_MS,
  bootControllerMode,
  describeShellResourceNameState,
  shellBootCanDismiss,
} from "./loading.js";

test("shell boot dismisses immediately once runtime snapshot is attached", () => {
  assert.equal(shellBootCanDismiss({ runtimeAttached: true, fallbackExpired: false }), true);
});

test("shell boot dismisses after fallback timeout even without runtime snapshot", () => {
  assert.equal(shellBootCanDismiss({ runtimeAttached: false, fallbackExpired: true }), true);
});

test("resource names render from shared-runtime snapshot immediately", () => {
  assert.deepEqual(
    describeShellResourceNameState({
      pk: "pk-1",
      fallback: "pk-1",
      resolvedLabel: "Front Door",
      runtimeAttached: false,
    }),
    { text: "Front Door", loading: false, raw: false },
  );
});

test("resource names stay in loading state when runtime is attached but label is unresolved", () => {
  assert.deepEqual(
    describeShellResourceNameState({
      pk: "pk-1",
      fallback: "pk-1",
      resolvedLabel: "",
      runtimeAttached: true,
    }),
    { text: "Loading name…", loading: true, raw: false },
  );
});

test("resource names fall back to raw identifier before runtime snapshot arrives", () => {
  assert.deepEqual(
    describeShellResourceNameState({
      pk: "pk-1",
      fallback: "pk-1",
      resolvedLabel: "",
      runtimeAttached: false,
    }),
    { text: "pk-1", loading: false, raw: true },
  );
});

test("no-controller boot path always prefers direct fallback over reload", () => {
  assert.equal(bootControllerMode({ controllerPresent: false }), "direct-fallback");
});

test("startup grace windows stay short", () => {
  assert.ok(SW_READY_GRACE_MS < 1_000);
  assert.ok(SW_CONTROLLER_GRACE_MS < 2_000);
  assert.ok(SHELL_BOOT_RUNTIME_ATTACH_TIMEOUT_MS < SHELL_BOOT_FALLBACK_TIMEOUT_MS);
});
