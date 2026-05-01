# Constitute Account

Browser-native account authority for the Constitution ecosystem.
`constitute-account` owns identity, pairing, grants, notifications, identity-owned zones, and the shared browser runtime used by first-party app surfaces.

## Status
- Prototype: active development
- Discovery bootstrap: implemented (zones + directory)
- Shared runtime + managed launch convergence: active
- Direct first-party app entry: active

## Key Concepts
- Identity: cryptographic grouping of devices
- Device: cryptographic endpoint (software or WebAuthn-backed)
- Service-backed device: native runtime with its own keypair, still published as a device record
- Pairing: approval-based device association
- Zone: discovery scope joined via shared key
- Gateway: owned browser control boundary and hosted-service inventory owner
- Directory: local cache of discovered peers and services

## Current Features
- Device identity lifecycle (software + optional WebAuthn)
- Identity create/join + pairing approval flow
- Grants/access review
- Notifications and pending-request management
- Relay transport via worker bridge + Service Worker authority
- Shared runtime worker for managed launch context and cross-surface service status
- Zone presence/list propagation and directory updates
- Swarm record cache (identity/device) with signed record validation
- Canonical app-channel plumbing for `swarm_record_request` and `swarm_dht_get/put`
- Managed app launch/runtime authority for first-party Pages surfaces such as `constitute-gateway-ui` and `constitute-nvr-ui`

## Project Layout
- `app.js`: account routes, pairing/device/grant presentation, and runtime/broker integration
- `identity/client.js`: window-to-Service Worker RPC bridge
- `relay.worker.js`: shared relay transport worker
- `runtime.worker.js`: shared runtime for managed app surfaces
- `identity/sw/*`: Service Worker daemon and protocol handlers
- `ARCHITECTURE.md`: architecture and roadmap

## Architecture
See `ARCHITECTURE.md` for system design and convergence direction.

## Running Locally
1. Run `npm install`
2. Run `npm run build`
3. Serve the built static output at `/constitute-account/`
4. Use HTTPS or localhost for WebAuthn paths

## Usage
- Create or join an identity
- Pair additional devices from notifications / pairing flow
- Review linked devices, grants, and zones from the account surface navigation
- For CLI/source install flows, follow `constitute-gateway` operator docs: `https://github.com/Aux0x7F/constitute-gateway/blob/main/docs/OPERATOR.md`
- Use `constitute-gateway-ui` for gateway inventory, hosted-service management, gateway zone sync, and network/security posture
- Managed first-party apps publish separately under the site domain, for example `tld/constitute-gateway-ui/` and `tld/constitute-nvr-ui/`
- If identity/device prerequisites are missing, UI routes to onboarding

## Update Durability
- Browser app updates do not reset identity/device state by design.
- Service Worker state remains in IndexedDB (`constitute_db`) and now mirrors to a CacheStorage backup lane for recovery.
- On read, if IDB misses/fails, backup cache is used and state is rehydrated into IDB.
- User state can still be lost by explicit browser site-data clearing; that is outside app control.

## Roadmap Snapshot
- P0: account-centered first-party chrome + runtime convergence
- P1: validate direct app entry and shared runtime recovery paths end to end
- P2: browser swarm transport refinement and TURN hard-NAT fallback
- P3: broader service/app surface expansion

## Security Notes
- UI does not hold long-term secret material
- Service Worker is the local cryptographic authority
- Relay transport is treated as untrusted; envelopes must validate

## License
TBD
