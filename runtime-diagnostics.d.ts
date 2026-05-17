export declare const RUNTIME_DIAGNOSTICS_STORAGE_KEY: string;
export declare const RUNTIME_DIAGNOSTICS_RING_LIMIT: number;
export declare const RUNTIME_DIAGNOSTICS_SUBSCRIBE: string;
export declare const RUNTIME_DIAGNOSTICS_UNSUBSCRIBE: string;
export declare const RUNTIME_DIAGNOSTICS_COMMAND: string;
export declare const RUNTIME_DIAGNOSTIC_PLANES: Readonly<Record<string, string>>;
export declare const RUNTIME_DIAGNOSTIC_OPERATOR_PLANES: readonly string[];

export type RuntimeDiagnosticsAgent = {
  readonly enabled: boolean;
  readonly surface: string;
  readonly clientId: string;
  readonly runtimeSessionId: string;
  handleMessage(message: unknown): boolean;
  command(name: string, args?: Record<string, unknown>): Promise<unknown>;
};

export declare function runtimeDiagnosticsEnabled(win?: Window): boolean;
export declare function attachRuntimeDiagnostics(options?: {
  window?: Window;
  port?: MessagePort | null;
  surface?: string;
  clientId?: string;
  enabled?: boolean;
  logging?: boolean;
  limit?: number;
  planes?: string[];
  minLevel?: string;
  minLevelByPlane?: Record<string, string>;
  denyKinds?: string[];
  subscription?: Record<string, unknown>;
  console?: Console;
}): RuntimeDiagnosticsAgent;
