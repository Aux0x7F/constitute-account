# Constitute

Browser-native management shell for the Constitution ecosystem.
`constitute` owns identity, pairing, zones, gateway inventory, hosted-service management, and managed app launch.

## Status
- Prototype: active development
- Discovery bootstrap: implemented (zones + directory)
- Gateway-managed launch and service-backed device convergence: active
- Managed NVR launch to separate app surface: active

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
- Notifications and pending-request management
- Relay transport via SharedWorker + Service Worker authority
- Zone presence/list propagation and directory updates
- Swarm record cache (identity/device) with signed record validation
- Canonical app-channel plumbing for `swarm_record_request` and `swarm_dht_get/put`
- Appliances tab for gateway/service inventory, freshness, host relationship, installer utility download, and remote service install requests
- Managed app launch direction for Pages-hosted first-party apps (for example `constitute-nvr-ui`)

## Project Layout
- `app.js`: shell routes, appliances, launcher flow, and peer/service presentation
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
- Use `Settings > Appliances` to download the installer utility for your operator platform
- For CLI/source install flows, follow `constitute-gateway` operator docs: `https://github.com/Aux0x7F/constitute-gateway/blob/main/docs/OPERATOR.md`
- In the installer utility, run release install/update for the current operator platform (service install only); first install prints a generated pairing code when pairing is pending
- For paired gateways, use `Configure Zones` in appliance actions to sync identity zones plus gateway-specific extra zones
- For paired Linux gateways, use `Install NVR Service` in appliance actions to trigger host-side NVR install remotely
- Use `Settings > Appliances` to pair existing gateways and launch owned managed app surfaces
- Managed first-party apps publish separately under the site domain, for example `tld/constitute-nvr-ui/`
- If identity/device prerequisites are missing, UI routes to onboarding

## Update Durability
- Browser app updates do not reset identity/device state by design.
- Service Worker state remains in IndexedDB (`constitute_db`) and now mirrors to a CacheStorage backup lane for recovery.
- On read, if IDB misses/fails, backup cache is used and state is rehydrated into IDB.
- User state can still be lost by explicit browser site-data clearing; that is outside app control.

## Roadmap Snapshot
- P0: service-backed device and managed app launch convergence
- P1: validate gateway-mediated NVR launch and WebRTC live preview end to end
- P2: browser swarm transport refinement and TURN hard-NAT fallback
- P3: broader service/app surface expansion

## Security Notes
- UI does not hold long-term secret material
- Service Worker is the local cryptographic authority
- Relay transport is treated as untrusted; envelopes must validate

## License
TBD
