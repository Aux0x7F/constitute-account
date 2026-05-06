// identity/client.js
// Client-side RPC wrapper for the identity Service Worker.
//
// IMPORTANT: We serialize RPC calls.
// The Service Worker is single-threaded and uses IndexedDB; sending many
// concurrent RPCs can cause contention and apparent hangs/timeouts.

import {
  SW_CONTROLLER_GRACE_MS,
  SW_READY_GRACE_MS,
  bootControllerMode,
} from "../app/loading.js";

const SERVICE_WORKER_BUILD_ID = "2026-05-03-servicepk-projection";

function currentServiceWorkerUrl() {
  const target = new URL("./sw.js", window.location.href);
  target.searchParams.set("v", SERVICE_WORKER_BUILD_ID);
  return target.toString();
}

function isLocalDevHost() {
  const host = String(window.location.hostname || "").toLowerCase();
  return host === "localhost" || host === "127.0.0.1";
}

export class IdentityClient {
  constructor({ onEvent, debug = false } = {}) {
    this.onEvent = onEvent || (() => {});
    this.debug = debug === true;
    this._reqId = 1;
    this._pending = new Map();
    this._readyPromise = null;
    this._reg = null;

    // Serialize calls to prevent SW/IDB contention.
    this._queue = Promise.resolve();

    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.addEventListener("message", (e) => this._onMessage(e));
      navigator.serviceWorker.addEventListener("controllerchange", () => {
        const controller = navigator.serviceWorker.controller;
        const scriptUrl = String(controller?.scriptURL || this._reg?.active?.scriptURL || '').trim();
        this.onEvent({ type: "log", message: `service worker controllerchange ${scriptUrl || '(unknown script)'}` });
      });
    }
  }

  async ready() {
    if (this._readyPromise) return this._readyPromise;

    this._readyPromise = (async () => {
      if (!("serviceWorker" in navigator)) {
        throw new Error("Service Worker not supported in this browser");
      }

      this._debug("[client] ready: checking SW registration");
      const desiredScriptUrl = currentServiceWorkerUrl();

      if (isLocalDevHost() && "serviceWorker" in navigator) {
        const regs = await navigator.serviceWorker.getRegistrations().catch(() => []);
        for (const reg of regs) {
          const activeUrl = String(reg?.active?.scriptURL || reg?.waiting?.scriptURL || reg?.installing?.scriptURL || "").trim();
          if (!activeUrl) continue;
          if (!activeUrl.includes("/constitute-account/sw.js")) continue;
          if (activeUrl === desiredScriptUrl) continue;
          try {
            this._debug("[client] unregister stale SW", activeUrl);
            await reg.unregister();
          } catch {}
        }
      }

      // Ensure SW is registered. sw.js is an ES module.
      let reg = await navigator.serviceWorker.getRegistration("./");
      if (!reg) reg = await navigator.serviceWorker.getRegistration();

      const activeScriptUrl = String(reg?.active?.scriptURL || reg?.waiting?.scriptURL || reg?.installing?.scriptURL || '').trim();
      const needsRegister = !reg || !activeScriptUrl || activeScriptUrl !== desiredScriptUrl;
      if (needsRegister) {
        this.onEvent({ type: "log", message: "registering service worker ./sw.js (module)" });
        this._debug("[client] registering SW", desiredScriptUrl);
        reg = await navigator.serviceWorker.register(desiredScriptUrl, {
          scope: "./",
          type: "module",
        });
      }

      this._reg = reg;
      const registeredScriptUrl = String(reg?.active?.scriptURL || reg?.waiting?.scriptURL || reg?.installing?.scriptURL || '').trim();
      if (registeredScriptUrl) {
        this._debug("[client] service worker registration", registeredScriptUrl);
      }
      this._debug("[client] waiting for SW ready");
      await Promise.race([
        navigator.serviceWorker.ready,
        new Promise((resolve) => setTimeout(resolve, SW_READY_GRACE_MS)),
      ]);

      let controllerOk = false;
      const activeWorker = reg?.active || null;
      const controllerPresent = Boolean(navigator.serviceWorker.controller);
      const shouldWaitForController = !controllerPresent && (!activeWorker || needsRegister);
      if (!shouldWaitForController) {
        if (controllerPresent) {
          controllerOk = true;
        } else {
          this._debug("[client] controller unavailable; using direct port fallback");
        }
      } else {
        // Give the controller a short grace window only when the page is actually
        // waiting on a newly registered or not-yet-active worker.
        this._debug("[client] waiting for controller");
        try {
          await this._waitForController(SW_CONTROLLER_GRACE_MS);
          controllerOk = true;
        } catch (e) {
          const mode = bootControllerMode({
            controllerPresent: Boolean(navigator.serviceWorker.controller),
          });
          if (mode === "controller") {
            controllerOk = true;
          } else {
            this._debug("[client] controller unavailable; using direct port fallback");
          }
        }
      }
      if (controllerOk) this._debug("[client] controller ready");
      return reg;
    })();

    return this._readyPromise;
  }

  isServiceWorkerAvailable() {
    return !!(navigator.serviceWorker.controller || this._reg?.active || this._reg?.waiting || this._reg?.installing);
  }

  /**
   * Enqueue an RPC call (serialized).
   */
  call(method, params = {}, { timeoutMs, priority } = {}) {
    if (priority === 'immediate') {
      return this._callOnce(method, params, { timeoutMs });
    }

    const run = () => this._callOnce(method, params, { timeoutMs });

    const p = this._queue.then(run);

    // Keep the queue alive even if a call fails.
    this._queue = p.catch(() => {});
    return p;
  }

  async _callOnce(method, params = {}, { timeoutMs } = {}) {
    // Default timeout: be generous for IDB/crypto + first-load conditions.
    const t = timeoutMs ?? 15000;

    await this.ready();

    const controller = navigator.serviceWorker.controller;

    const id = this._reqId++;
    const payload = { type: "req", id, method, params };

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new Error(`timeout calling ${method}`));
      }, t);

      this._pending.set(id, { resolve, reject, timer });

      try {
        if (controller) {
          controller.postMessage(payload);
        } else if (this._reg?.active) {
          const ch = new MessageChannel();
          ch.port1.onmessage = (e) => {
            const msg = e.data || {};
            if (msg.type !== "res" || msg.id !== id) return;
            const p = this._pending.get(msg.id);
            if (!p) return;
            clearTimeout(p.timer);
            this._pending.delete(msg.id);
            if (msg.ok) p.resolve(msg.result);
            else p.reject(new Error(msg.error || "unknown error"));
          };
          this._reg.active.postMessage(payload, [ch.port2]);
        } else {
          const any = this._reg?.waiting || this._reg?.installing;
          if (any) {
            const ch = new MessageChannel();
            ch.port1.onmessage = (e) => {
              const msg = e.data || {};
              if (msg.type !== "res" || msg.id !== id) return;
              const p = this._pending.get(msg.id);
              if (!p) return;
              clearTimeout(p.timer);
              this._pending.delete(msg.id);
              if (msg.ok) p.resolve(msg.result);
              else p.reject(new Error(msg.error || "unknown error"));
            };
            any.postMessage(payload, [ch.port2]);
          } else {
            throw new Error("service worker controller not available");
          }
        }
      } catch (e) {
        clearTimeout(timer);
        this._pending.delete(id);
        reject(e);
      }
    });
  }

  _onMessage(e) {
    const msg = e.data || {};

    // daemon events
    if (msg.type === "evt" && msg.evt) {
      this.onEvent(msg.evt);
      return;
    }

    // rpc responses
    if (msg.type === "res") {
      const p = this._pending.get(msg.id);
      if (!p) return;

      clearTimeout(p.timer);
      this._pending.delete(msg.id);

      if (msg.ok) p.resolve(msg.result);
      else p.reject(new Error(msg.error || "unknown error"));
    }
  }

  _debug(...args) {
    if (!this.debug) return;
    console.debug(...args);
  }

  _waitForController(timeoutMs = 8000) {
    if (navigator.serviceWorker.controller) return Promise.resolve();

    return new Promise((resolve, reject) => {
      const start = Date.now();

      const tick = () => {
        if (navigator.serviceWorker.controller) {
          cleanup();
          this._debug("[client] controllerchange: controller set");
          resolve();
          return;
        }
        if (Date.now() - start > timeoutMs) {
          cleanup();
          this._debug("[client] controller wait timeout");
          reject(new Error("service worker controller not available"));
        }
      };

      const onChange = () => tick();
      const cleanup = () => {
        clearInterval(iv);
        navigator.serviceWorker.removeEventListener("controllerchange", onChange);
      };

      navigator.serviceWorker.addEventListener("controllerchange", onChange);
      const iv = setInterval(tick, 100);
      tick();
    });
  }
}

