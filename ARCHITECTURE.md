# Constitute - Architecture Overview

`constitute` is the browser-native management shell for the Constitution ecosystem.
It owns identity, device, pairing, gateway, zone, and managed-service control UX.

It does not permanently embed every first-party application inside the shell. Instead, it launches managed app surfaces that publish separately under the same site domain.

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
- brokers browser launch/signaling
- inventories hosted services
- enforces managed-service capability checks

## Shell Responsibilities
`constitute` owns:
- onboarding
- device creation and pairing approval
- identity lifecycle
- zone management
- gateway inventory and freshness
- hosted service inventory
- managed app launch

`constitute` does not own:
- long-running native transport
- camera ingest or retention
- direct service-specific media pipelines

## UI Structure

### Management Shell
The shell remains at `tld/constitute/` and is responsible for:
- Home
- Settings
- pairing and identity management
- devices and service-backed devices
- appliances/gateway management
- launch entry points for managed apps

### Managed App Surfaces
First-party apps publish separately, for example:
- `tld/constitute-nvr-ui/`

The shell launches these in a new tab or window and provides a short-lived launch context instead of long-lived secrets in the URL.

## Runtime Structure

Browser UI
- `app.js`
  - shell routes and appliances, launcher flow, and peer/service presentation
- `identity/client.js`
  - window-to-Service Worker RPC bridge
- `runtime.worker.js`
  - same-origin shared runtime for managed launch context, cross-surface status, and gateway request brokering
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
- SharedWorker runtime owns same-origin launch/session/status coordination across first-party app surfaces.
- Relay transport runs in a dedicated/shared worker bridge, separate from page rendering.
- Windows never own long-lived secret authority directly.

### Managed-Service Direction
- browser app surfaces should attach to owned services through owned gateways
- gateway remains the control/auth boundary
- WebRTC is the preferred browser-safe transport direction for managed live media and direct paths
- shell launch/bootstrap must stay separate from media transport

## Discovery and Directory
Zones remain the discovery scope:
- zone keys are operator/user-shared
- directory stores discovered devices and service-backed devices
- service-backed devices must render distinctly from user devices
- appliance inventory should show host relationship, freshness, and launch availability

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
- managed launch/signaling coordination

## Security Model
- device-level signing remains mandatory
- Service Worker remains the cryptographic authority for shell state
- long-lived identity secrets must not be passed in app launch URLs
- managed app surfaces should redeem short-lived launch context through shared runtime first, with explicit local fallback only where needed
- relay transport remains untrusted and validation-bound

## Active Convergence Slice
`constitute` is converging toward:
- service-backed device rendering (`deviceKind = user|service`)
- gateway-managed launch authorization
- Pages-native app surfaces (`tld/<repo-name>/`)
- managed NVR launch into `constitute-nvr-ui`
- WebRTC live preview as the managed browser media direction

## Design Principles
- management shell and app surfaces stay separate
- gateway remains the canonical browser control boundary
- transport choice must not redefine trust
- service-backed devices participate in the same identity model
- launch context is short-lived and explicit
