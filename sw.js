import { startDaemon } from './identity/sw/daemon.js';

const SERVICE_WORKER_BUILD_ID = '2026-05-03-servicepk-projection';

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

try {
  self.__CONSTITUTE_SW_BUILD_ID__ = SERVICE_WORKER_BUILD_ID;
  startDaemon(self);
} catch (e) {
  console.error('[SW] startDaemon failed', e);
}

