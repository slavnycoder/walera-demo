// Walera SSE client wrapper — project-agnostic. Owns the SSE plumbing
// (auto-reconnect, bearer header, JSON parsing) and re-emits walera's
// four event types (`initial_data`, `tx`, `error`, `shutdown`) plus
// connection-lifecycle callbacks (`onOpen`, `onDisconnect`).
//
// The wrapper deliberately does *not* know about your storage layer,
// schema, or row-coercion rules. Apply the parsed events to Dexie /
// Redux / Zustand / your-state-of-choice in `onInitialData` / `onTx`.
//
// Source lives in `frontend/src/walera-client.ts`; Caddy serves the
// compiled `frontend/web/walera-client.js`. Recompile after edits:
//   cd frontend && npx -y -p typescript@5 tsc -p tsconfig.json

import { fetchEventSource } from "https://esm.sh/@microsoft/fetch-event-source@2.0.1";

export type WaleraOp = "insert" | "update" | "delete";

export interface WaleraChange {
  table: string;
  pk: string;
  op: WaleraOp;
  // Present for insert/update. May be a *partial* row on update — pgoutput
  // emits only changed columns, so merge against existing state rather
  // than overwriting. Absent on delete.
  data?: Record<string, string>;
}

export interface WaleraTx {
  tx_id: number;
  commit_lsn?: string;
  changes: WaleraChange[];
}

export interface WaleraClientOptions<TInitialData = unknown> {
  // Walera base, e.g. "http://localhost:8080". No trailing slash needed.
  baseUrl: string;

  // Channel path component after `/sse/v1/`, e.g. "todo_lists/1" or
  // "orders/all". The wrapper does not parse it; pass it through.
  channel: string;

  // Bearer token Walera will forward to the auth backend's
  // POST /auth/sessions on handshake.
  token: string;

  // Forwarded to fetchEventSource. Defaults to true so the stream
  // survives backgrounded tabs (mobile Safari can still freeze it).
  openWhenHidden?: boolean;

  // Reconnect delay (ms) returned to fetchEventSource on disconnect.
  // Default 3000.
  retryDelayMs?: number;

  // First SSE frame after every successful open. Walera re-emits it on
  // every handshake, so this also closes the gap on reconnect.
  onInitialData?: (data: TInitialData) => void | Promise<void>;

  // Every committed transaction matching the subscribed channel.
  onTx?: (tx: WaleraTx) => void | Promise<void>;

  // Server-side error event (e.g. "auth_revoked").
  onError?: (reason: string) => void;

  // Server is shutting down; client should expect a reconnect.
  onShutdown?: () => void;

  // Connection lifecycle.
  onOpen?: () => void;
  onDisconnect?: (err: unknown) => void;

  // Optional one-line trace sink (useful for an in-page event log).
  onLog?: (line: string) => void;
}

export interface WaleraClient {
  // Closes the SSE connection. After close the client does not reconnect.
  close(): void;
}

export function connectWalera<TInitialData = unknown>(
  opts: WaleraClientOptions<TInitialData>,
): WaleraClient {
  const {
    baseUrl,
    channel,
    token,
    openWhenHidden = true,
    retryDelayMs = 3000,
    onInitialData,
    onTx,
    onError,
    onShutdown,
    onOpen,
    onDisconnect,
    onLog,
  } = opts;

  const url = `${baseUrl.replace(/\/$/, "")}/sse/v1/${channel}`;
  const abort = new AbortController();
  const log = (msg: string) => onLog?.(msg);

  void fetchEventSource(url, {
    headers: { Authorization: `Bearer ${token}` },
    openWhenHidden,
    signal: abort.signal,
    async onopen(resp) {
      if (!resp.ok) {
        log(`SSE open failed: ${resp.status}`);
        // Throwing here makes fetchEventSource treat the response as a
        // fatal open error and call onerror.
        throw new Error(`walera SSE open ${resp.status}`);
      }
      log("SSE connected");
      onOpen?.();
    },
    async onmessage(msg) {
      try {
        switch (msg.event) {
          case "initial_data": {
            const data = JSON.parse(msg.data) as TInitialData;
            await onInitialData?.(data);
            log("initial_data delivered");
            return;
          }
          case "tx": {
            const tx = JSON.parse(msg.data) as WaleraTx;
            await onTx?.(tx);
            return;
          }
          case "error": {
            onError?.(msg.data);
            log(`SSE error event: ${msg.data}`);
            return;
          }
          case "shutdown": {
            onShutdown?.();
            log("SSE shutdown frame received");
            return;
          }
          default:
            // Heartbeats and unknown events: ignore. fetchEventSource
            // strips comment lines (`:\n\n`) on its own.
            return;
        }
      } catch (err) {
        log(`onmessage handler error (${msg.event}): ${String(err)}`);
      }
    },
    onerror(err) {
      onDisconnect?.(err);
      log(`SSE disconnect: ${String(err)}`);
      return retryDelayMs;
    },
  });

  return {
    close() {
      abort.abort();
    },
  };
}
