const DB_NAME = 'constitute_db';
const DB_VER = 1;
const BACKUP_CACHE = 'constitute_kv_backup_v1';
const BACKUP_PREFIX = '/__constitute_kv__/';

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VER);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains('kv')) db.createObjectStore('kv');
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function withDbRetry(label, work, attempts = 2) {
  let lastError = null;
  for (let index = 0; index < attempts; index += 1) {
    try {
      const db = await openDB();
      return await work(db);
    } catch (error) {
      lastError = error;
      if (index < attempts - 1) {
        await new Promise((resolve) => setTimeout(resolve, 30 * (index + 1)));
      }
    }
  }
  throw new Error(`${label}: ${String(lastError?.message || lastError || 'database failure')}`);
}

function backupRequestForKey(key) {
  return new Request(`${BACKUP_PREFIX}${encodeURIComponent(String(key || ''))}`);
}

async function backupSet(key, value) {
  try {
    const cache = await caches.open(BACKUP_CACHE);
    const body = JSON.stringify({ v: value });
    await cache.put(
      backupRequestForKey(key),
      new Response(body, { headers: { 'content-type': 'application/json' } })
    );
  } catch {
    // backup is best-effort only
  }
}

async function backupGet(key) {
  try {
    const cache = await caches.open(BACKUP_CACHE);
    const hit = await cache.match(backupRequestForKey(key));
    if (!hit) return undefined;
    const raw = await hit.json().catch(() => null);
    if (!raw || !Object.prototype.hasOwnProperty.call(raw, 'v')) return undefined;
    return raw.v;
  } catch {
    return undefined;
  }
}

export async function kvGet(key) {
  try {
    const value = await withDbRetry(`kvGet(${String(key || '')})`, async (db) => await new Promise((resolve, reject) => {
      const tx = db.transaction('kv', 'readonly');
      const st = tx.objectStore('kv');
      const req = st.get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    }));

    if (typeof value !== 'undefined') {
      return value;
    }
  } catch {
    // fall through to backup cache
  }

  const backup = await backupGet(key);
  if (typeof backup !== 'undefined') {
    // heal IDB on-demand when possible
    try {
      await kvSet(key, backup);
    } catch {}
  }
  return backup;
}

export async function kvSet(key, value) {
  await withDbRetry(`kvSet(${String(key || '')})`, async (db) => await new Promise((resolve, reject) => {
    const tx = db.transaction('kv', 'readwrite');
    tx.objectStore('kv').put(value, key);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  }));
  await backupSet(key, value);
}
