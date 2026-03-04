# Constitute

Browser-native identity and discovery client for the Constitution ecosystem.
`constitute` is the web-side runtime that converges against `constitute-gateway` contracts.

## Status
- Prototype: active development
- Discovery bootstrap: implemented (zones + directory)
- Gateway contract convergence: in progress (app-channel parity + ingest hardening)
- Browser swarm transport: staged behind gateway-first convergence

## Key Concepts
- Identity: cryptographic grouping of devices
- Device: cryptographic endpoint (software or WebAuthn-backed)
- Pairing: approval-based device association
- Zone: discovery scope joined via shared key
- Directory: local cache of discovered peers

## Current Features
- Device identity lifecycle (software + optional WebAuthn)
- Identity create/join + pairing approval flow
- Notifications and pending-request management
- Relay transport via SharedWorker + Service Worker authority
- Zone presence/list propagation and directory updates
- Swarm record cache (identity/device) with signed record validation
- Canonical app-channel plumbing for `swarm_record_request` and `swarm_dht_get/put`
- Manifest-driven app auto-enable from service records (`uiRepo` / `uiRef` / `uiManifestUrl`)
- Home app launcher cards for enabled capabilities
- Appliances tab for gateway/NVR device inventory and control-panel launch wiring

## Project Layout
- `app.js`: UI state, routing, and peer presentation
- `identity/client.js`: window-to-Service Worker RPC bridge
- `relay.worker.js`: shared relay transport worker
- `identity/sw/*`: Service Worker daemon and protocol handlers
- `ARCHITECTURE.md`: architecture and roadmap

## Architecture
See `ARCHITECTURE.md` for system design and convergence direction.

## Running Locally
1. Serve this repo on `http://localhost:8000` (or equivalent static host)
2. Open in a modern browser with Service Worker support
3. Use HTTPS or localhost for WebAuthn paths

## Usage
- Create or join an identity
- Pair additional devices from notifications / pairing flow
- Manage zones and discovered peers in `Settings > Peers`
- Use `Settings > Appliances` to install/pair gateways and open NVR control panel surfaces
- Manage optional app repos in `Settings > Apps` and launch enabled apps from Home
- If identity/device prerequisites are missing, UI routes to onboarding

## Roadmap Snapshot
- P0: converge web behavior to frozen gateway protocol contracts
- P1: validate browser <-> gateway integration paths end-to-end
- P2: browser swarm transport refinement (TURN as fallback boundary)
- P3: codebase refactor and modularization

## Security Notes
- UI does not hold long-term secret material
- Service Worker is the local cryptographic authority
- Relay transport is treated as untrusted; envelopes must validate

## License
TBD

