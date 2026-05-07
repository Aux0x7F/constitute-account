# Constitute Account - Architecture Overview

`constitute-account` is the browser-native account authority for the Constitution ecosystem.
It owns identity, device, pairing, grant, notification, and identity-zone UX.

It does not permanently embed every first-party application inside the account surface. Shared first-party chrome comes from `constitute-ui`, and gateway-specific management lives in `constitute-gateway-ui`.

## Core Concepts

### Identity
An identity is the trust domain / owner grouping:
- identity label
- identity id
- approved devices
- approved service-backed devices

### Device
A paired endpoint acting for an identity.

### Service-Backed Device
A native runtime with its own keypair that still publishes as a device record.

Examples:
- owned gateway
- hosted NVR service

### Gateway
A special service-backed device that:
- brokers browser service access/signaling
- inventories hosted services
- enforces managed-service capability checks

## Account Responsibilities
`constitute-account` owns:
- onboarding
- device creation and pairing approval
- identity lifecycle
- zone management
- grants/access
- notifications
- shared runtime authority

`constitute-account` does not own:
- gateway inventory and freshness UI
- hosted service inventory UI
- network/cybersecurity posture UI
- long-running native transport
- camera ingest or retention
- direct service-specific media pipelines

## UI Structure

### Account Surface
The account app remains at `tld/constitute-account/` and is responsible for:
- account overview
- identity/profile
- devices
- grants/access
- notifications
- identity-owned zones

Current shell rules:
- onboarding is automatic when no linked session/device exists; it is not a persistent navigation option
- notifications should stay in a loading state until the first notification query resolves
- service-worker controller warmup may fall back to direct RPC transport, but that infrastructure detail should not by itself degrade the visible connection summary

### Managed App Surfaces
First-party apps publish separately, for example:
- `tld/constitute-gateway-ui/`
- `tld/constitute-nvr-ui/`
- future `tld/constitute-physec/`

Direct app entry is canonical. Managed app surfaces still redeem short-lived service access context instead of long-lived secrets in the URL.
Users should not need to visit `constitute-account` manually before another first-party app can become usable. When an app needs account/session/grant repair, it should attach to the shared runtime and drive that recovery through the app flow.

## Runtime Structure

Browser UI
- `app.js`
  - account routes, pairing/device/grant presentation, and runtime/broker integration
- `identity/client.js`
  - window-to-Service Worker RPC bridge
- `runtime.worker.js`
  - same-origin shared runtime for managed service access context, retained projections, cross-surface status, and gateway request brokering
- `relay.worker.js`
  - persistent WebSocket relay bridge for relay pool transport only

Service Worker
- `identity/sw/daemon.js`
  - identity, device, pairing, and relay state authority
- `identity/sw/rpc.js`
  - RPC surface for shell actions
- `identity/sw/relayIn.js`
  - relay ingest and event dispatch
- `identity/sw/*`
  - storage, crypto, Nostr, zone, and directory helpers

## Transport Direction

### Current Browser Authority
- Service Worker is the cryptographic/state authority.
- SharedWorker runtime owns same-origin service access/session/status coordination across first-party app surfaces.
- `runtime.worker.js` is an ES module SharedWorker. First-party apps must attach with the versioned URL and worker name `constitute-account-runtime-${runtimeBuildId}` so direct-entry apps and the account bridge share one runtime instance.
- Relay transport runs in a dedicated/shared worker bridge, separate from page rendering.
- Windows never own long-lived secret authority directly.

### Managed-Service Direction
- browser app surfaces should attach to owned services through owned gateways
- gateway remains the control/auth boundary
- WebRTC is the preferred browser-safe transport direction for managed live media and direct paths
- shell service access/bootstrap must stay separate from media transport
- retained runtime projection is first truth for display/session hints until contradicted
- retained runtime projections are first truth for generic consumable service data, including logging event and health projections
- privileged service admission still requires valid cryptographic authorization
- service access uses `constitute-protocol` CAAC primitives for explicit capability state bound to identity, device, gateway, service, scope, expiry, and replay protection

## Startup Model

### First Paint Authority
- shared runtime snapshot is the first-paint authority for the shell and same-origin first-party app surfaces
- persisted runtime state may be stale, but it is preferred over blocking on live hydration
- live refresh reconciles the shell and managed surfaces after first paint

### Critical Boot Vs Background Hydration
Critical boot is limited to:
- shell/runtime attach
- first available runtime snapshot
- minimum surface structure for the shell or managed app

Background hydration owns:
- service worker/controller readiness
- relay startup
- identity/device refresh
- directory, zone, swarm, and notification refresh
- app catalog hydration
- managed app signaling/media negotiation

### Surface State Rules
- service worker/controller readiness is infrastructure warming, not a page gate
- the shell should not remain behind a full-page splash once runtime snapshot or degraded empty-shell state is available
- managed app surfaces should dismiss full-page splash once service access context and initial surface structure are ready
- live media connection state belongs to tiles and section-level status after first paint
- onboarding progress should remain stable while background hydration continues; refresh loops must not snap the user back to step one unless the device truly lost its local authority

## Runtime Projection Contract

The shared runtime owns retained projection storage for browser-safe, capability-scoped data that can be consumed by multiple first-party app surfaces.

Current projection rules:
- projection channels are named and typed by `constitute-protocol`
- projection payloads stay domain-generic where the domain supports it; logging uses generic log event envelopes instead of per-service UI schemas
- browser apps render retained/materialized projections first and observe runtime updates
- account/runtime syncs missing, stale, or incomplete projections through service-owned CAAC exchange, with Gateway only routing and attesting
- broker/control exists for service access and projection sync/repair; it is not the primary product data interface
- runtime stores materialized projections generically by service identity, channel, and policy id; it does not own Logging, NVR, or Storage domain semantics
- runtime owns browser-side IndexedDB materialization, coverage tracking, and observer updates
- service-owned projection responses are authoritative for their policy; runtime replaces that materialized event set rather than unioning stale rows forever
- semantically unchanged projections must not trigger persistence, snapshots, or observer updates
- UI surfaces do not coordinate chunks, cursors, scroll paging, or service query windows

Initial projection channels:
- `logging.events`
- `logging.health`

## Discovery and Directory
Zones remain the discovery scope:
- zone keys are operator/user-shared
- directory stores discovered devices and service-backed devices
- service-backed devices must render distinctly from user devices
- appliance inventory should show host relationship, freshness, and service access availability

## State Storage

### IndexedDB
Authoritative local state store:
- device metadata
- identity metadata
- pairing requests
- notifications
- discovered devices
- discovered service-backed devices
- zone memberships

### Relay and App-Channel State
Used for:
- pairing requests/approvals
- device and identity label updates
- gateway/service status events
- discovery and zone presence
- service access/signaling coordination

Pairing UX remains action-oriented:
- pairing codes can be pasted from operator tools and are normalized before claim
- pairing claim/request expiry is owned by Service Worker state and projected to UI
- stale pair requests stop rendering after their advertised TTL

## Security Model
- device-level signing remains mandatory
- Service Worker remains the cryptographic authority for shell state
- long-lived identity secrets must not be passed in app service access URLs
- managed app surfaces should redeem short-lived service access context through shared runtime first, with explicit local fallback only where needed
- relay transport remains untrusted and validation-bound
- signed data is not confidential by default
- touched service-access paths must use CAAC envelopes when sensitive metadata crosses broker or relay-facing boundaries; signed integrity alone is not treated as confidentiality

## Active Convergence Slice
Current convergence work is focused on:
- account-centered identity/session/grant authority in `constitute-account`
- shared first-party chrome/primitives in `constitute-ui`
- gateway-management extraction into `constitute-gateway-ui`
- managed NVR service access into `constitute-nvr-ui`
- shared non-UI protocol/security/control primitives in `constitute-protocol`
- runtime-propagated logging projections for `constitute-logging-ui`
- WebRTC live preview as the managed browser media direction

## Design Principles
- management shell and app surfaces stay separate
- gateway remains the canonical browser control boundary
- transport choice must not redefine trust
- service-backed devices participate in the same identity model
- service access context is short-lived and explicit

## Current Product Boundary
- `constitute-account` is the account-centered browser authority now
- direct app entry is canonical
- gateway-specific management belongs in `constitute-gateway-ui`
- the footer account/state rail is the stable first-party account entrypoint across apps
