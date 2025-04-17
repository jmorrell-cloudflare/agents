import * as hono from "hono";
import { Env } from "hono";
import { AgentOptions } from "agents";

/**
 * Configuration options for the Cloudflare Agents middleware
 */
type AgentMiddlewareContext<E extends Env> = {
  /** Cloudflare Agents-specific configuration options */
  options?: AgentOptions<E>;
  /** Optional error handler for caught errors */
  onError?: (error: Error) => void;
};
/**
 * Creates a middleware for handling Cloudflare Agents WebSocket and HTTP requests
 * Processes both WebSocket upgrades and standard HTTP requests, delegating them to Cloudflare Agents
 */
declare function agentsMiddleware<E extends Env = Env>(
  ctx?: AgentMiddlewareContext<E>
): hono.MiddlewareHandler<Env, string, {}>;

export { agentsMiddleware };
