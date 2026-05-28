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
export function connectWalera(opts) {
    const { baseUrl, channel, token, openWhenHidden = true, retryDelayMs = 3000, onInitialData, onTx, onError, onShutdown, onOpen, onDisconnect, onLog, } = opts;
    const url = `${baseUrl.replace(/\/$/, "")}/sse/v1/${channel}`;
    const abort = new AbortController();
    const log = (msg) => onLog?.(msg);
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
                        const data = JSON.parse(msg.data);
                        await onInitialData?.(data);
                        log("initial_data delivered");
                        return;
                    }
                    case "tx": {
                        const tx = JSON.parse(msg.data);
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
            }
            catch (err) {
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
