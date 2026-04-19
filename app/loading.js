export const SHELL_BOOT_RUNTIME_ATTACH_TIMEOUT_MS = 900;
export const SHELL_BOOT_FALLBACK_TIMEOUT_MS = 1_600;
export const SW_READY_GRACE_MS = 400;
export const SW_CONTROLLER_GRACE_MS = 800;

export function shellBootCanDismiss({ runtimeAttached = false, fallbackExpired = false } = {}) {
  return runtimeAttached || fallbackExpired;
}

export function describeShellResourceNameState({
  pk = "",
  fallback = "",
  resolvedLabel = "",
  runtimeAttached = false,
} = {}) {
  const key = String(pk || "").trim();
  const raw = String(fallback || "").trim() || "—";
  const resolved = String(resolvedLabel || "").trim();
  if (!key) {
    return { text: raw, loading: false, raw: false };
  }
  if (resolved) {
    return { text: resolved, loading: false, raw: false };
  }
  if (runtimeAttached) {
    return { text: "Loading name…", loading: true, raw: false };
  }
  return { text: raw, loading: false, raw: true };
}

export function bootControllerMode({ controllerPresent = false } = {}) {
  return controllerPresent ? "controller" : "direct-fallback";
}
