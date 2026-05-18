import {
  SURFACE_APP,
  assertServiceManagerSecretBoundary,
  assertSurfaceAppBootstrapContract,
  assertSurfaceAppManifest,
  assertSurfaceAppContract,
} from "../constitute-protocol/src/index.js";
import {
  defineSurfaceAppContract,
  surfaceAppRunnerPlan,
  surfaceAppBootstrapPosture,
  surfaceAppRuntimeSelectionPosture,
  surfaceServiceManagerOperationPosture,
  surfaceServiceManagerProofDigest,
} from "../constitute-ui/src/surface-app-contract.js";
import { createRuntimeSurfaceClient } from "../constitute-ui/src/runtime-surface-client.js";
import {
  createSurfaceModuleRegistry,
  surfaceAppModuleImplementations,
} from "../constitute-ui/src/surface-module-registry.js";
import {
  browserStorageShellContext,
  deriveRuntimeShellState,
  runtimeShellConnectionToneClass,
} from "./runtime-shell-state.js";

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
  serviceManagerPosture: {
    managerId: "manager:manual:account-ui",
    subjectRef: "app:account-ui",
    managerRef: "manager:manual:account-ui",
    state: SURFACE_APP.SERVICE_MANAGER_POSTURE.MANUAL,
    serviceRefs: ["app:account-ui"],
    capabilityRefs: ["service.manage"],
    evidenceRefs: ["build:account-ui:local"],
    issuedAt: ISSUED_AT,
  },
  secretBoundary: {
    state: SURFACE_APP.SECRET_BOUNDARY.NOT_REQUIRED,
  },
  releasePosture: {
    state: SURFACE_APP.RELEASE_POSTURE.STATIC,
    evidenceRefs: ["build:account-ui:local"],
  },
  issuedAt: ISSUED_AT,
});

export const accountSurfaceApp = defineSurfaceAppContract(accountSurfaceAppContract, {
  validate: assertSurfaceAppContract,
});

export const accountSurfaceAppManifest = assertSurfaceAppManifest({
  kind: "surface.app.manifest",
  manifestId: "manifest:account-ui",
  appId: "constitute-account",
  state: SURFACE_APP.MANIFEST_VERSION_STATE.CURRENT,
  currentAppContractRef: "app:account-ui",
  currentVersion: "0.1.0",
  defaultSourceMode: SURFACE_APP.FULFILLMENT_MODE.BUNDLED,
  requiredModuleRoles: [
    SURFACE_APP.MODULE_ROLE.RUNTIME_CLIENT,
    SURFACE_APP.MODULE_ROLE.PROJECTION_MODEL,
    SURFACE_APP.MODULE_ROLE.PLATFORM_ADAPTER,
    SURFACE_APP.MODULE_ROLE.PRODUCT_VIEW,
  ],
  bundledSourceRefs: ["bundle:account-ui@0.1.0"],
  compatibilityWindow: {
    minVersion: "0.1.0",
    maxVersion: "0.1.x",
    protocolRef: "protocol:surface-app:v1",
  },
  versions: [
    {
      appContractRef: "app:account-ui",
      version: "0.1.0",
      state: SURFACE_APP.MANIFEST_VERSION_STATE.CURRENT,
      sourceMode: SURFACE_APP.FULFILLMENT_MODE.BUNDLED,
      requiredModuleRoles: [
        SURFACE_APP.MODULE_ROLE.RUNTIME_CLIENT,
        SURFACE_APP.MODULE_ROLE.PROJECTION_MODEL,
        SURFACE_APP.MODULE_ROLE.PLATFORM_ADAPTER,
        SURFACE_APP.MODULE_ROLE.PRODUCT_VIEW,
      ],
      compatibilityWindow: {
        minVersion: "0.1.0",
        maxVersion: "0.1.x",
        protocolRef: "protocol:surface-app:v1",
      },
      bundledSourceRefs: ["bundle:account-ui@0.1.0"],
      grantRefs: ["grant:app:account-ui:run"],
      runnerRequirementRefs: ["runner:req:account-ui"],
      serviceManagerRequirementRefs: ["service-manager:req:account-ui"],
      compatibilityRefs: ["protocol:surface-app:v1"],
      bootstrapContractRef: "bootstrap-contract:app:account-ui",
      releaseContractRef: "release:account-ui:local",
      issuedAt: ISSUED_AT,
    },
  ],
  appContractRefs: ["app:account-ui"],
  grantRefs: ["grant:app:account-ui:run"],
  runnerRequirementRefs: ["runner:req:account-ui"],
  serviceManagerRequirementRefs: ["service-manager:req:account-ui"],
  compatibilityRefs: ["protocol:surface-app:v1"],
  bootstrapContractRefs: ["bootstrap-contract:app:account-ui"],
  releaseContractRefs: ["release:account-ui:local"],
  authorityRefs: ["authority:account-ui:local"],
  evidenceRefs: ["build:account-ui:local"],
  issuedAt: ISSUED_AT,
});

export const accountSurfaceRuntimeSelectionPosture = surfaceAppRuntimeSelectionPosture(
  accountSurfaceAppManifest,
  [accountSurfaceApp],
  {
    runtimeVersion: "0.1.0",
    issuedAt: ISSUED_AT,
  },
);

export const accountSurfaceModuleRegistry = createSurfaceModuleRegistry([
  {
    moduleRef: "constitute-ui/runtime-surface-client@0.1.0",
    role: SURFACE_APP.MODULE_ROLE.RUNTIME_CLIENT,
    version: "0.1.0",
    primitiveRefs: ["runtime.attach", "runtime.intent"],
    implementation: Object.freeze({ createRuntimeSurfaceClient }),
  },
  {
    moduleRef: "constitute-account/runtime-ui-model@0.1.0",
    role: SURFACE_APP.MODULE_ROLE.PROJECTION_MODEL,
    version: "0.1.0",
    primitiveRefs: ["runtime.shell.posture", "projection.materialization"],
    implementation: Object.freeze({
      deriveRuntimeShellState,
      runtimeShellConnectionToneClass,
    }),
  },
  {
    moduleRef: "constitute-account/browser-storage-shell@0.1.0",
    role: SURFACE_APP.MODULE_ROLE.PLATFORM_ADAPTER,
    version: "0.1.0",
    primitiveRefs: ["runtime.shell.context"],
    implementation: Object.freeze({ browserStorageShellContext }),
  },
  {
    moduleRef: "constitute-account/account-view@0.1.0",
    role: SURFACE_APP.MODULE_ROLE.PRODUCT_VIEW,
    version: "0.1.0",
    primitiveRefs: ["runtime.posture.render"],
    implementation: Object.freeze({ surfaceRef: "constitute-account" }),
  },
]);

export const accountSurfaceModules = surfaceAppModuleImplementations(
  accountSurfaceModuleRegistry,
  accountSurfaceApp,
);

export const accountSurfaceRunnerPlan = surfaceAppRunnerPlan(accountSurfaceApp, {
  issuedAt: ISSUED_AT,
});

export const accountServiceManagerSecretBoundary = assertServiceManagerSecretBoundary(
  accountSurfaceRunnerPlan.secretBoundary,
);

export const accountSurfaceBootstrapContract = assertSurfaceAppBootstrapContract(
  accountSurfaceRunnerPlan.bootstrapContract,
);

export const accountSurfaceBootstrapPosture = surfaceAppBootstrapPosture(accountSurfaceApp, {
  issuedAt: ISSUED_AT,
});

export const accountServiceManagerOperationPosture = surfaceServiceManagerOperationPosture(accountSurfaceApp, {
  operation: SURFACE_APP.SERVICE_MANAGER_OPERATION.HEALTH_CHECK,
  operationId: "operation:account-ui:bootstrap-health",
  requestedAt: ISSUED_AT,
});

export const accountServiceManagerProofDigest = surfaceServiceManagerProofDigest(accountSurfaceApp, {
  operationPosture: accountServiceManagerOperationPosture,
  digestId: "proof-digest:account-ui:bootstrap",
  observedAt: ISSUED_AT,
});

export const accountRuntimeClientModule = accountSurfaceModuleRegistry.require(
  accountSurfaceApp,
  SURFACE_APP.MODULE_ROLE.RUNTIME_CLIENT,
).implementation;

export const accountProjectionModelModule = accountSurfaceModuleRegistry.require(
  accountSurfaceApp,
  SURFACE_APP.MODULE_ROLE.PROJECTION_MODEL,
).implementation;

export const accountPlatformAdapterModule = accountSurfaceModuleRegistry.require(
  accountSurfaceApp,
  SURFACE_APP.MODULE_ROLE.PLATFORM_ADAPTER,
).implementation;

export const accountSurfaceAttachContext = accountSurfaceApp.attachContext({
  productSurface: "constitute-account",
  runtimeSelectionPosture: accountSurfaceRuntimeSelectionPosture,
  runnerPlan: accountSurfaceRunnerPlan,
  bootstrapContract: accountSurfaceBootstrapContract,
  serviceManagerSecretBoundary: accountServiceManagerSecretBoundary,
  bootstrapPosture: accountSurfaceBootstrapPosture,
  serviceManagerOperationPosture: accountServiceManagerOperationPosture,
  serviceManagerProofDigest: accountServiceManagerProofDigest,
});
