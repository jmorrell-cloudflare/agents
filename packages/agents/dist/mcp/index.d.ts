import { DurableObject } from 'cloudflare:workers';
import { Connection, WSMessage } from 'partyserver';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

interface CORSOptions {
    origin?: string;
    methods?: string;
    headers?: string;
    maxAge?: number;
}
declare abstract class McpAgent<Env = unknown, State = unknown, Props extends Record<string, unknown> = Record<string, unknown>> extends DurableObject<Env> {
    #private;
    protected constructor(ctx: DurableObjectState, env: Env);
    /**
     * Agents API allowlist
     */
    initialState: State;
    get state(): State;
    sql<T = Record<string, string | number | boolean | null>>(strings: TemplateStringsArray, ...values: (string | number | boolean | null)[]): T[];
    setState(state: State): void;
    onStateUpdate(state: State | undefined, source: Connection | "server"): void;
    onStart(): Promise<void>;
    /**
     * McpAgent API
     */
    abstract server: McpServer;
    props: Props;
    initRun: boolean;
    abstract init(): Promise<void>;
    _init(props: Props): Promise<void>;
    isInitialized(): boolean;
    fetch(request: Request): Promise<Response>;
    getWebSocket(): WebSocket | null;
    getWebSocketForResponseID(id: string): WebSocket | null;
    onMessage(connection: Connection, event: WSMessage): Promise<void>;
    onSSEMcpMessage(sessionId: string, request: Request): Promise<Error | null>;
    webSocketMessage(ws: WebSocket, event: ArrayBuffer | string): Promise<void>;
    webSocketError(ws: WebSocket, error: unknown): Promise<void>;
    webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void>;
    static mount(path: string, { binding, corsOptions, }?: {
        binding?: string;
        corsOptions?: CORSOptions;
    }): {
        fetch: (request: Request, env: Record<string, DurableObjectNamespace<McpAgent>>, ctx: ExecutionContext) => Promise<Response | undefined>;
    };
    static serveSSE(path: string, { binding, corsOptions, }?: {
        binding?: string;
        corsOptions?: CORSOptions;
    }): {
        fetch: (request: Request, env: Record<string, DurableObjectNamespace<McpAgent>>, ctx: ExecutionContext) => Promise<Response | undefined>;
    };
    static serve(path: string, { binding, corsOptions, }?: {
        binding?: string;
        corsOptions?: CORSOptions;
    }): {
        fetch: (request: Request, env: Record<string, DurableObjectNamespace<McpAgent>>, ctx: ExecutionContext) => Promise<Response>;
    };
}

export { McpAgent };
