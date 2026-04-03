let ws = null;
let wsUrl = null;
let state = 'idle';
const ports = new Set();
let reconnectTimer = null;
let reconnectAttempt = 0;
let manualClose = false;

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function broadcast(msg) {
  for (const p of ports) {
    try { p.postMessage(msg); } catch {}
  }
}

function setState(next, extra = {}) {
  state = next;
  broadcast({ type: 'relay.status', state, url: wsUrl || '', ...extra });
}

function scheduleReconnect() {
  clearReconnectTimer();
  if (manualClose || !wsUrl) return;
  reconnectAttempt += 1;
  const delayMs = Math.min(15_000, 1_000 * Math.max(1, reconnectAttempt));
  setState('connecting', { reason: `reconnect in ${delayMs}ms`, attempt: reconnectAttempt });
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect(wsUrl, { isRetry: true });
  }, delayMs);
}

function connect(url, { isRetry = false } = {}) {
  const target = String(url || '').trim();
  if (!target) throw new Error('missing url');
  manualClose = false;
  clearReconnectTimer();

  if (ws && wsUrl === target && ws.readyState === WebSocket.OPEN) {
    setState('open');
    return;
  }

  try { if (ws) ws.close(); } catch {}
  ws = null;

  wsUrl = target;
  setState('connecting', isRetry ? { reason: `retry ${reconnectAttempt}` } : {});

  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    reconnectAttempt = 0;
    setState('open');
  };
  ws.onerror = () => setState('error');
  ws.onclose = (e) => {
    setState('closed', { code: e?.code ?? null, reason: e?.reason ?? '' });
    ws = null;
    scheduleReconnect();
  };

  ws.onmessage = (e) => {
    broadcast({ type: 'relay.rx', data: e.data, url: wsUrl });
  };
}

function send(frame) {
  if (!ws || ws.readyState !== WebSocket.OPEN) throw new Error('relay not open');
  ws.send(String(frame));
}

onconnect = (e) => {
  const port = e.ports[0];
  ports.add(port);

  port.onmessage = (ev) => {
    const msg = ev.data || {};
    try {
      if (msg.type === 'relay.connect') {
        connect(msg.url);
        port.postMessage({ type: 'relay.ack', ok: true });
        return;
      }
      if (msg.type === 'relay.send') {
        send(msg.frame);
        port.postMessage({ type: 'relay.ack', ok: true });
        return;
      }
      if (msg.type === 'relay.status') {
        port.postMessage({ type: 'relay.status', state, url: wsUrl || '' });
        return;
      }
      if (msg.type === 'relay.close') {
        manualClose = true;
        clearReconnectTimer();
        try { if (ws) ws.close(); } catch {}
        ws = null;
        setState('closed');
        port.postMessage({ type: 'relay.ack', ok: true });
        return;
      }
    } catch (err) {
      port.postMessage({ type: 'relay.ack', ok: false, error: String(err?.message || err) });
    }
  };

  port.start();
  port.postMessage({ type: 'relay.status', state, url: wsUrl || '' });
};
