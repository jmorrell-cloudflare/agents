// src/index.ts
import { env } from "hono/adapter";
import { createMiddleware } from "hono/factory";
import { routeAgentRequest } from "agents";
function agentsMiddleware(ctx) {
  return createMiddleware(async (c, next) => {
    try {
      const handler = isWebSocketUpgrade(c) ? handleWebSocketUpgrade : handleHttpRequest;
      const response = await handler(c, ctx?.options);
      return response === null ? await next() : response;
    } catch (error) {
      if (ctx?.onError) {
        ctx.onError(error);
        return next();
      }
      throw error;
    }
  });
}
function isWebSocketUpgrade(c) {
  return c.req.header("upgrade")?.toLowerCase() === "websocket";
}
function createRequestFromContext(c) {
  return new Request(c.req.url, {
    method: c.req.method,
    headers: c.req.header(),
    body: c.req.raw.body
  });
}
async function handleWebSocketUpgrade(c, options) {
  const req = createRequestFromContext(c);
  const response = await routeAgentRequest(req, env(c), options);
  if (!response?.webSocket) {
    return null;
  }
  return new Response(null, {
    status: 101,
    webSocket: response.webSocket
  });
}
async function handleHttpRequest(c, options) {
  const req = createRequestFromContext(c);
  return routeAgentRequest(req, env(c), options);
}
export {
  agentsMiddleware
};
//# sourceMappingURL=index.js.map