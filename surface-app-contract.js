import { SURFACE_APP, assertSurfaceAppContract } from "../constitute-protocol/src/index.js";
import { defineSurfaceAppContract } from "../constitute-ui/src/surface-app-contract.js";

const ISSUED_AT = 1700000000;

export const accountSurfaceAppContract = assertSurfaceAppContract({
  contractId: "surface-app:constitute-account",
  schemaVersion: SURFACE_APP.SCHEMA_VERSION,
  appId: "constitute-account",
  appRef: "app:account-ui",
  version: "0.1.0",
  displayName: "Account",
  surfaceRef: "surface:account-ui",
  requiredPrimitives: [
    "runtime.attach",
    "runtime.shell.posture",
    "projection.materialization",
  ],
  requiredModuleRoles: [
    SURFACE_APP.MODULE_ROLE.RUNTIME_CLIENT,
    SURFACE_APP.MODULE_ROLE.PROJECTION_MODEL,
    SURFACE_APP.MODULE_ROLE.PLATFORM_ADAPTER,
    SURFACE_APP.MODULE_ROLE.PRODUCT_VIEW,
  ],
  modules: [
    {
      moduleRef: "constitute-ui/runtime-surface-client@0.1.0",
      role: SURFACE_APP.MODULE_ROLE.RUNTIME_CLIENT,
      participantSide: SURFACE_APP.PARTICIPANT_SIDE.WINDOW,
      fulfillmentMode: SURFACE_APP.FULFILLMENT_MODE.BUNDLED,
      version: "0.1.0",
      primitiveRefs: ["runtime.attach", "runtime.intent"],
      inputs: ["runtime.snapshot"],
      outputs: ["runtime.intent", "runtime.evidence"],
      issuedAt: ISSUED_AT,
    },
    {
      moduleRef: "constitute-account/runtime-ui-model@0.1.0",
      role: SURFACE_APP.MODULE_ROLE.PROJECTION_MODEL,
      participantSide: SURFACE_APP.PARTICIPANT_SIDE.WINDOW,
      fulfillmentMode: SURFACE_APP.FULFILLMENT_MODE.BUNDLED,
      version: "0.1.0",
      primitiveRefs: ["runtime.shell.posture", "projection.materialization"],
      inputs: ["runtime.snapshot"],
      outputs: ["account.read-model"],
      issuedAt: ISSUED_AT,
    },
    {
      moduleRef: "constitute-account/browser-storage-shell@0.1.0",
      role: SURFACE_APP.MODULE_ROLE.PLATFORM_ADAPTER,
      participantSide: SURFACE_APP.PARTICIPANT_SIDE.WINDOW,
      fulfillmentMode: SURFACE_APP.FULFILLMENT_MODE.BUNDLED,
      version: "0.1.0",
      primitiveRefs: ["runtime.shell.context"],
      inputs: ["browser.storage"],
      outputs: ["shell.context.evidence"],
      issuedAt: ISSUED_AT,
    },
    {
      moduleRef: "constitute-account/account-view@0.1.0",
      role: SURFACE_APP.MODULE_ROLE.PRODUCT_VIEW,
      participantSide: SURFACE_APP.PARTICIPANT_SIDE.WINDOW,
      fulfillmentMode: SURFACE_APP.FULFILLMENT_MODE.BUNDLED,
      version: "0.1.0",
      primitiveRefs: ["runtime.posture.render"],
      inputs: ["account.read-model", "runtime.shell.posture"],
      outputs: ["user.intent"],
      issuedAt: ISSUED_AT,
    },
  ],
  projectionSubscriptions: [
    { projectionId: "runtime.shell", channelId: "runtime.shell" },
    { projectionId: "swarm.directory", channelId: "swarm.directory" },
  ],
  updatePosture: {
    state: SURFACE_APP.UPDATE_POSTURE.STATIC,
    checkedAt: ISSUED_AT,
  },
  issuedAt: ISSUED_AT,
});

export const accountSurfaceApp = defineSurfaceAppContract(accountSurfaceAppContract, {
  validate: assertSurfaceAppContract,
});

export const accountSurfaceAttachContext = accountSurfaceApp.attachContext({
  productSurface: "constitute-account",
});
