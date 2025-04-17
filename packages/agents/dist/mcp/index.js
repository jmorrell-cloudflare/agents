import {
  Agent
} from "../chunk-XG52S6YY.js";
import {
  __privateAdd,
  __privateGet,
  __privateMethod,
  __privateSet
} from "../chunk-HMLY7DHA.js";

// src/mcp/index.ts
import { DurableObject } from "cloudflare:workers";
import {
  InitializeRequestSchema,
  JSONRPCErrorSchema,
  JSONRPCMessageSchema,
  JSONRPCNotificationSchema,
  JSONRPCRequestSchema,
  JSONRPCResponseSchema
} from "@modelcontextprotocol/sdk/types.js";
var MAXIMUM_MESSAGE_SIZE_BYTES = 4 * 1024 * 1024;
function handleCORS(request, corsOptions) {
  const origin = request.headers.get("Origin") || "*";
  const corsHeaders = {
    "Access-Control-Allow-Origin": corsOptions?.origin || origin,
    "Access-Control-Allow-Methods": corsOptions?.methods || "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": corsOptions?.headers || "Content-Type",
    "Access-Control-Max-Age": (corsOptions?.maxAge || 86400).toString()
  };
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders });
  }
  return null;
}
function parseMessage(message) {
  const requestResult = JSONRPCRequestSchema.safeParse(message);
  if (requestResult.success) {
    return {
      type: "request",
      message: requestResult.data,
      isInitializationRequest: InitializeRequestSchema.safeParse(message).success
    };
  }
  const notificationResult = JSONRPCNotificationSchema.safeParse(message);
  if (notificationResult.success) {
    return {
      type: "notification",
      message: notificationResult.data
    };
  }
  const responseResult = JSONRPCResponseSchema.safeParse(message);
  if (responseResult.success) {
    return {
      type: "response",
      message: responseResult.data
    };
  }
  const errorResult = JSONRPCErrorSchema.safeParse(message);
  if (errorResult.success) {
    return {
      type: "error",
      message: errorResult.data
    };
  }
  throw new Error("Invalid message");
}
var _getWebSocket, _started;
var McpSSETransport = class {
  constructor(getWebSocket) {
    __privateAdd(this, _getWebSocket);
    __privateAdd(this, _started, false);
    __privateSet(this, _getWebSocket, getWebSocket);
  }
  async start() {
    if (__privateGet(this, _started)) {
      throw new Error("Transport already started");
    }
    __privateSet(this, _started, true);
  }
  async send(message) {
    if (!__privateGet(this, _started)) {
      throw new Error("Transport not started");
    }
    const websocket = __privateGet(this, _getWebSocket).call(this);
    if (!websocket) {
      throw new Error("WebSocket not connected");
    }
    try {
      websocket.send(JSON.stringify(message));
    } catch (error) {
      this.onerror?.(error);
      throw error;
    }
  }
  async close() {
    this.onclose?.();
  }
};
_getWebSocket = new WeakMap();
_started = new WeakMap();
var _getWebSocketForGetRequest, _getWebSocketForMessageID, _notifyResponseIdSent, _started2;
var McpStreamableHttpTransport = class {
  constructor(getWebSocketForMessageID, notifyResponseIdSent) {
    // TODO: If there is an open connection to send server-initiated messages
    // back, we should use that connection
    __privateAdd(this, _getWebSocketForGetRequest);
    // Get the appropriate websocket connection for a given message id
    __privateAdd(this, _getWebSocketForMessageID);
    // Notify the server that a response has been sent for a given message id
    // so that it may clean up it's mapping of message ids to connections
    // once they are no longer needed
    __privateAdd(this, _notifyResponseIdSent);
    __privateAdd(this, _started2, false);
    __privateSet(this, _getWebSocketForMessageID, getWebSocketForMessageID);
    __privateSet(this, _notifyResponseIdSent, notifyResponseIdSent);
    __privateSet(this, _getWebSocketForGetRequest, () => null);
  }
  async start() {
    if (__privateGet(this, _started2)) {
      throw new Error("Transport already started");
    }
    __privateSet(this, _started2, true);
  }
  async send(message) {
    if (!__privateGet(this, _started2)) {
      throw new Error("Transport not started");
    }
    let websocket = null;
    const parsedMessage = parseMessage(message);
    switch (parsedMessage.type) {
      // These types have an id
      case "response":
      case "error":
        websocket = __privateGet(this, _getWebSocketForMessageID).call(this, parsedMessage.message.id.toString());
        if (!websocket) {
          throw new Error(
            `Could not find WebSocket for message id: ${parsedMessage.message.id}`
          );
        }
        break;
      // requests have an ID but are originated by the server so do not correspond to
      // any active connection
      case "request":
        websocket = __privateGet(this, _getWebSocketForGetRequest).call(this);
        break;
      // Notifications do not have an id
      case "notification":
        websocket = __privateGet(this, _getWebSocketForGetRequest).call(this);
        break;
    }
    try {
      websocket?.send(JSON.stringify(message));
      if (parsedMessage.type === "response") {
        __privateGet(this, _notifyResponseIdSent).call(this, parsedMessage.message.id.toString());
      }
    } catch (error) {
      this.onerror?.(error);
      throw error;
    }
  }
  async close() {
    this.onclose?.();
  }
};
_getWebSocketForGetRequest = new WeakMap();
_getWebSocketForMessageID = new WeakMap();
_notifyResponseIdSent = new WeakMap();
_started2 = new WeakMap();
var _status, _transport, _protocol, _requestIdToConnectionId, _agent, _McpAgent_instances, initialize_fn;
var _McpAgent = class _McpAgent extends DurableObject {
  constructor(ctx, env) {
    var _a;
    super(ctx, env);
    __privateAdd(this, _McpAgent_instances);
    __privateAdd(this, _status, "zero");
    __privateAdd(this, _transport);
    __privateAdd(this, _protocol, "unset");
    __privateAdd(this, _requestIdToConnectionId, /* @__PURE__ */ new Map());
    /**
     * Since McpAgent's _aren't_ yet real "Agents", let's only expose a couple of the methods
     * to the outer class: initialState/state/setState/onStateUpdate/sql
     */
    __privateAdd(this, _agent);
    this.initRun = false;
    const self = this;
    __privateSet(this, _agent, new (_a = class extends Agent {
      onStateUpdate(state, source) {
        return self.onStateUpdate(state, source);
      }
      async onMessage(connection, message) {
        return self.onMessage(connection, message);
      }
    }, _a.options = {
      hibernate: true
    }, _a)(ctx, env));
  }
  get state() {
    return __privateGet(this, _agent).state;
  }
  sql(strings, ...values) {
    return __privateGet(this, _agent).sql(strings, ...values);
  }
  setState(state) {
    return __privateGet(this, _agent).setState(state);
  }
  onStateUpdate(state, source) {
  }
  async onStart() {
    var _a;
    const self = this;
    __privateSet(this, _agent, new (_a = class extends Agent {
      constructor() {
        super(...arguments);
        this.initialState = self.initialState;
      }
      onStateUpdate(state, source) {
        return self.onStateUpdate(state, source);
      }
      async onMessage(connection, event) {
        return self.onMessage(connection, event);
      }
    }, _a.options = {
      hibernate: true
    }, _a)(this.ctx, this.env));
    this.props = await this.ctx.storage.get("props");
    __privateSet(this, _protocol, await this.ctx.storage.get("protocol"));
    this.init?.();
    if (__privateGet(this, _protocol) === "sse") {
      __privateSet(this, _transport, new McpSSETransport(() => this.getWebSocket()));
      await this.server.connect(__privateGet(this, _transport));
    } else if (__privateGet(this, _protocol) === "streamable-http") {
      __privateSet(this, _transport, new McpStreamableHttpTransport(
        (id) => this.getWebSocketForResponseID(id),
        (id) => __privateGet(this, _requestIdToConnectionId).delete(id)
      ));
      await this.server.connect(__privateGet(this, _transport));
    }
  }
  async _init(props) {
    await this.ctx.storage.put("props", props);
    await this.ctx.storage.put("protocol", "unset");
    this.props = props;
    if (!this.initRun) {
      this.initRun = true;
      await this.init();
    }
  }
  isInitialized() {
    return this.initRun;
  }
  // Allow the worker to fetch a websocket connection to the agent
  async fetch(request) {
    if (__privateGet(this, _status) !== "started") {
      await __privateMethod(this, _McpAgent_instances, initialize_fn).call(this);
    }
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket Upgrade request", {
        status: 400
      });
    }
    const url = new URL(request.url);
    const path = url.pathname;
    switch (path) {
      // This session is going to communicate via the SSE protocol
      case "/sse": {
        const websockets = this.ctx.getWebSockets();
        if (websockets.length > 0) {
          return new Response("Websocket already connected", { status: 400 });
        }
        await this.ctx.storage.put("protocol", "sse");
        __privateSet(this, _protocol, "sse");
        if (!__privateGet(this, _transport)) {
          __privateSet(this, _transport, new McpSSETransport(() => this.getWebSocket()));
          await this.server.connect(__privateGet(this, _transport));
        }
        return __privateGet(this, _agent).fetch(request);
      }
      case "/streamable-http": {
        if (!__privateGet(this, _transport)) {
          __privateSet(this, _transport, new McpStreamableHttpTransport(
            (id) => this.getWebSocketForResponseID(id),
            (id) => __privateGet(this, _requestIdToConnectionId).delete(id)
          ));
          await this.server.connect(__privateGet(this, _transport));
        }
        await this.ctx.storage.put("protocol", "streamable-http");
        __privateSet(this, _protocol, "streamable-http");
        return __privateGet(this, _agent).fetch(request);
      }
      default:
        return new Response(
          "Internal Server Error: Expected /sse or /streamable-http path",
          {
            status: 500
          }
        );
    }
  }
  getWebSocket() {
    const websockets = this.ctx.getWebSockets();
    if (websockets.length === 0) {
      return null;
    }
    return websockets[0];
  }
  getWebSocketForResponseID(id) {
    const connectionId = __privateGet(this, _requestIdToConnectionId).get(id);
    if (connectionId === void 0) {
      return null;
    }
    return __privateGet(this, _agent).getConnection(connectionId) ?? null;
  }
  // All messages received here. This is currently never called
  async onMessage(connection, event) {
    if (__privateGet(this, _protocol) !== "streamable-http") {
      const err = new Error(
        "Internal Server Error: Expected streamable-http protocol"
      );
      __privateGet(this, _transport)?.onerror?.(err);
      return;
    }
    let message;
    try {
      const data = typeof event === "string" ? event : new TextDecoder().decode(event);
      message = JSONRPCMessageSchema.parse(JSON.parse(data));
    } catch (error) {
      __privateGet(this, _transport)?.onerror?.(error);
      return;
    }
    const parsedMessage = parseMessage(message);
    switch (parsedMessage.type) {
      case "request":
        __privateGet(this, _requestIdToConnectionId).set(
          parsedMessage.message.id.toString(),
          connection.id
        );
        break;
      case "response":
      case "notification":
      case "error":
        break;
    }
    __privateGet(this, _transport)?.onmessage?.(message);
  }
  // All messages received over SSE after the initial connection has been established
  // will be passed here
  async onSSEMcpMessage(sessionId, request) {
    if (__privateGet(this, _status) !== "started") {
      await __privateMethod(this, _McpAgent_instances, initialize_fn).call(this);
    }
    if (__privateGet(this, _protocol) !== "sse") {
      return new Error("Internal Server Error: Expected SSE protocol");
    }
    try {
      const message = await request.json();
      let parsedMessage;
      try {
        parsedMessage = JSONRPCMessageSchema.parse(message);
      } catch (error) {
        __privateGet(this, _transport)?.onerror?.(error);
        throw error;
      }
      __privateGet(this, _transport)?.onmessage?.(parsedMessage);
      return null;
    } catch (error) {
      __privateGet(this, _transport)?.onerror?.(error);
      return error;
    }
  }
  // Delegate all websocket events to the underlying agent
  async webSocketMessage(ws, event) {
    if (__privateGet(this, _status) !== "started") {
      await __privateMethod(this, _McpAgent_instances, initialize_fn).call(this);
    }
    return await __privateGet(this, _agent).webSocketMessage(ws, event);
  }
  // WebSocket event handlers for hibernation support
  async webSocketError(ws, error) {
    if (__privateGet(this, _status) !== "started") {
      await __privateMethod(this, _McpAgent_instances, initialize_fn).call(this);
    }
    return await __privateGet(this, _agent).webSocketError(ws, error);
  }
  async webSocketClose(ws, code, reason, wasClean) {
    if (__privateGet(this, _status) !== "started") {
      await __privateMethod(this, _McpAgent_instances, initialize_fn).call(this);
    }
    return await __privateGet(this, _agent).webSocketClose(ws, code, reason, wasClean);
  }
  static mount(path, {
    binding = "MCP_OBJECT",
    corsOptions
  } = {}) {
    return _McpAgent.serveSSE(path, { binding, corsOptions });
  }
  static serveSSE(path, {
    binding = "MCP_OBJECT",
    corsOptions
  } = {}) {
    let pathname = path;
    if (path === "/") {
      pathname = "/*";
    }
    const basePattern = new URLPattern({ pathname });
    const messagePattern = new URLPattern({ pathname: `${pathname}/message` });
    return {
      fetch: async (request, env, ctx) => {
        const corsResponse = handleCORS(request, corsOptions);
        if (corsResponse) return corsResponse;
        const url = new URL(request.url);
        const namespace = env[binding];
        if (request.method === "GET" && basePattern.test(url)) {
          const sessionId = url.searchParams.get("sessionId") || namespace.newUniqueId().toString();
          const { readable, writable } = new TransformStream();
          const writer = writable.getWriter();
          const encoder = new TextEncoder();
          const endpointMessage = `event: endpoint
data: ${encodeURI(`${pathname}/message`)}?sessionId=${sessionId}

`;
          writer.write(encoder.encode(endpointMessage));
          const id = namespace.idFromName(`sse:${sessionId}`);
          const doStub = namespace.get(id);
          await doStub._init(ctx.props);
          const upgradeUrl = new URL(request.url);
          upgradeUrl.pathname = "/sse";
          const response = await doStub.fetch(
            new Request(upgradeUrl, {
              headers: {
                Upgrade: "websocket",
                // Required by PartyServer
                "x-partykit-room": sessionId
              }
            })
          );
          const ws = response.webSocket;
          if (!ws) {
            console.error("Failed to establish WebSocket connection");
            await writer.close();
            return;
          }
          ws.accept();
          ws.addEventListener("message", async (event) => {
            try {
              const message = JSON.parse(event.data);
              const result = JSONRPCMessageSchema.safeParse(message);
              if (!result.success) {
                return;
              }
              const messageText = `event: message
data: ${JSON.stringify(result.data)}

`;
              await writer.write(encoder.encode(messageText));
            } catch (error) {
              console.error("Error forwarding message to SSE:", error);
            }
          });
          ws.addEventListener("error", async (error) => {
            try {
              await writer.close();
            } catch (e) {
            }
          });
          ws.addEventListener("close", async () => {
            try {
              await writer.close();
            } catch (error) {
              console.error("Error closing SSE connection:", error);
            }
          });
          return new Response(readable, {
            headers: {
              "Content-Type": "text/event-stream",
              "Cache-Control": "no-cache",
              Connection: "keep-alive",
              "Access-Control-Allow-Origin": corsOptions?.origin || "*"
            }
          });
        }
        if (request.method === "POST" && messagePattern.test(url)) {
          const sessionId = url.searchParams.get("sessionId");
          if (!sessionId) {
            return new Response(
              `Missing sessionId. Expected POST to ${pathname} to initiate new one`,
              { status: 400 }
            );
          }
          const contentType = request.headers.get("content-type") || "";
          if (!contentType.includes("application/json")) {
            return new Response(`Unsupported content-type: ${contentType}`, {
              status: 400
            });
          }
          const contentLength = Number.parseInt(
            request.headers.get("content-length") || "0",
            10
          );
          if (contentLength > MAXIMUM_MESSAGE_SIZE_BYTES) {
            return new Response(
              `Request body too large: ${contentLength} bytes`,
              {
                status: 400
              }
            );
          }
          const id = namespace.idFromName(`sse:${sessionId}`);
          const doStub = namespace.get(id);
          const error = await doStub.onSSEMcpMessage(sessionId, request);
          if (error) {
            return new Response(error.message, {
              status: 400,
              headers: {
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                Connection: "keep-alive",
                "Access-Control-Allow-Origin": corsOptions?.origin || "*"
              }
            });
          }
          return new Response("Accepted", {
            status: 202,
            headers: {
              "Content-Type": "text/event-stream",
              "Cache-Control": "no-cache",
              Connection: "keep-alive",
              "Access-Control-Allow-Origin": corsOptions?.origin || "*"
            }
          });
        }
        return new Response("Not Found", { status: 404 });
      }
    };
  }
  static serve(path, {
    binding = "MCP_OBJECT",
    corsOptions
  } = {}) {
    let pathname = path;
    if (path === "/") {
      pathname = "/*";
    }
    const basePattern = new URLPattern({ pathname });
    return {
      fetch: async (request, env, ctx) => {
        const corsResponse = handleCORS(request, corsOptions);
        if (corsResponse) {
          return corsResponse;
        }
        const url = new URL(request.url);
        const namespace = env[binding];
        if (request.method === "POST" && basePattern.test(url)) {
          const acceptHeader = request.headers.get("accept");
          if (!acceptHeader?.includes("application/json") || !acceptHeader.includes("text/event-stream")) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32e3,
                message: "Not Acceptable: Client must accept application/json and text/event-stream"
              },
              id: null
            });
            return new Response(body2, { status: 406 });
          }
          const ct = request.headers.get("content-type");
          if (!ct || !ct.includes("application/json")) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32e3,
                message: "Unsupported Media Type: Content-Type must be application/json"
              },
              id: null
            });
            return new Response(body2, { status: 415 });
          }
          const contentLength = Number.parseInt(
            request.headers.get("content-length") ?? "0",
            10
          );
          if (contentLength > MAXIMUM_MESSAGE_SIZE_BYTES) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32e3,
                message: `Request body too large. Maximum size is ${MAXIMUM_MESSAGE_SIZE_BYTES} bytes`
              },
              id: null
            });
            return new Response(body2, { status: 413 });
          }
          let sessionId = request.headers.get("mcp-session-id");
          const rawMessage = await request.json();
          let messages = [];
          let parsedMessages = [];
          if (Array.isArray(rawMessage)) {
            messages = rawMessage.map((msg) => JSONRPCMessageSchema.parse(msg));
          } else {
            messages = [JSONRPCMessageSchema.parse(rawMessage)];
          }
          parsedMessages = messages.map(parseMessage);
          const isInitializationRequest = parsedMessages.some(
            (msg) => msg.type === "request" && msg.isInitializationRequest
          );
          if (isInitializationRequest && sessionId) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32600,
                message: "Invalid Request: Initialization requests must not include a sessionId"
              },
              id: null
            });
            return new Response(body2, { status: 400 });
          }
          if (isInitializationRequest && messages.length > 1) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32600,
                message: "Invalid Request: Only one initialization request is allowed"
              },
              id: null
            });
            return new Response(body2, { status: 400 });
          }
          if (!isInitializationRequest && !sessionId) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32600,
                message: "Bad Request: Mcp-Session-Id header is required"
              },
              id: null
            });
            return new Response(body2, { status: 400 });
          }
          sessionId = sessionId ?? namespace.newUniqueId().toString();
          const id = namespace.idFromName(`streamable-http:${sessionId}`);
          const doStub = namespace.get(id);
          if (isInitializationRequest) {
            await doStub._init(ctx.props);
          } else if (!doStub.isInitialized()) {
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32001,
                message: "Session not found"
              },
              id: null
            });
            return new Response(body2, { status: 400 });
          }
          const { readable, writable } = new TransformStream();
          const writer = writable.getWriter();
          const encoder = new TextEncoder();
          const upgradeUrl = new URL(request.url);
          upgradeUrl.pathname = "/streamable-http";
          const response = await doStub.fetch(
            new Request(upgradeUrl, {
              headers: {
                Upgrade: "websocket",
                // Required by PartyServer
                "x-partykit-room": sessionId
              }
            })
          );
          const ws = response.webSocket;
          if (!ws) {
            console.error("Failed to establish WebSocket connection");
            await writer.close();
            const body2 = JSON.stringify({
              jsonrpc: "2.0",
              error: {
                code: -32001,
                message: "Failed to establish WebSocket connection"
              },
              id: null
            });
            return new Response(body2, { status: 500 });
          }
          const requestIds = /* @__PURE__ */ new Set();
          ws.accept();
          ws.addEventListener("message", async (event) => {
            try {
              const data = typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data);
              const message = JSON.parse(data);
              const result = JSONRPCMessageSchema.safeParse(message);
              if (!result.success) {
                return;
              }
              const parsedMessage = parseMessage(result.data);
              switch (parsedMessage.type) {
                case "response":
                case "error":
                  requestIds.add(parsedMessage.message.id);
                  break;
                case "notification":
                case "request":
                  break;
              }
              const messageText = `event: message
data: ${JSON.stringify(result.data)}

`;
              await writer.write(encoder.encode(messageText));
              if (requestIds.size === messages.length) {
                ws.close();
              }
            } catch (error) {
              console.error("Error forwarding message to SSE:", error);
            }
          });
          ws.addEventListener("error", async (error) => {
            try {
              await writer.close();
            } catch (e) {
            }
          });
          ws.addEventListener("close", async () => {
            try {
              await writer.close();
            } catch (error) {
              console.error("Error closing SSE connection:", error);
            }
          });
          const hasOnlyNotificationsOrResponses = parsedMessages.every(
            (msg) => msg.type === "notification" || msg.type === "response"
          );
          if (hasOnlyNotificationsOrResponses) {
            for (const message of messages) {
              ws.send(JSON.stringify(message));
            }
            ws.close();
            return new Response(null, { status: 202 });
          }
          for (const message of messages) {
            const parsedMessage = parseMessage(message);
            switch (parsedMessage.type) {
              case "request":
                requestIds.add(parsedMessage.message.id);
                break;
              case "notification":
              case "response":
              case "error":
                break;
            }
            ws.send(JSON.stringify(message));
          }
          return new Response(readable, {
            headers: {
              "Content-Type": "text/event-stream",
              "Cache-Control": "no-cache",
              Connection: "keep-alive",
              "mcp-session-id": sessionId,
              "Access-Control-Allow-Origin": corsOptions?.origin || "*"
            },
            status: 200
          });
        }
        const body = JSON.stringify({
          jsonrpc: "2.0",
          error: {
            code: -32e3,
            message: "Method not allowed"
          },
          id: null
        });
        return new Response(body, { status: 405 });
      }
    };
  }
};
_status = new WeakMap();
_transport = new WeakMap();
_protocol = new WeakMap();
_requestIdToConnectionId = new WeakMap();
_agent = new WeakMap();
_McpAgent_instances = new WeakSet();
initialize_fn = async function() {
  await this.ctx.blockConcurrencyWhile(async () => {
    __privateSet(this, _status, "starting");
    await this.onStart();
    __privateSet(this, _status, "started");
  });
};
var McpAgent = _McpAgent;
export {
  McpAgent
};
//# sourceMappingURL=index.js.map