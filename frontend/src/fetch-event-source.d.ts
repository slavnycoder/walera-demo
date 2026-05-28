// Ambient declaration for the esm.sh URL import. The runtime resolves
// the URL fine; this just lets `tsc --strict` type-check the call site.
// Mirrors the public surface of @microsoft/fetch-event-source@2.x that
// walera-client.ts actually uses.

declare module "https://esm.sh/@microsoft/fetch-event-source@2.0.1" {
  export interface EventSourceMessage {
    id: string;
    event: string;
    data: string;
    retry?: number;
  }

  export interface FetchEventSourceInit extends Omit<RequestInit, "headers"> {
    headers?: Record<string, string>;
    openWhenHidden?: boolean;
    onopen?: (response: Response) => void | Promise<void>;
    onmessage?: (msg: EventSourceMessage) => void | Promise<void>;
    onclose?: () => void;
    onerror?: (err: unknown) => number | void | null | undefined;
    fetch?: typeof fetch;
  }

  export function fetchEventSource(url: string, init: FetchEventSourceInit): Promise<void>;
}
