import { resolveAgentIdentity } from '../runtime/registration.ts';
import type { Agent, AgentLocationHint } from '../types.ts';

const AGENT_LOCATION_HINTS = [
	'wnam',
	'enam',
	'sam',
	'weur',
	'eeur',
	'apac',
	'apac-ne',
	'apac-se',
	'oc',
	'afr',
	'me',
] as const satisfies readonly AgentLocationHint[];
const agentLocationHints = new Set<string>(AGENT_LOCATION_HINTS);

/**
 * Resolve and validate the location hint carried by an agent. Generated
 * Cloudflare Worker bootstraps call this once at module initialization; other
 * targets deliberately never consult it.
 */
export function resolveAgentLocationHint(agent: Agent): AgentLocationHint | undefined {
	const locationHint = agent.locationHint;
	if (locationHint === undefined) return undefined;
	if (typeof locationHint !== 'string' || !agentLocationHints.has(locationHint)) {
		const identity = resolveAgentIdentity(agent) ?? (agent.name || '(anonymous)');
		throw new Error(
			`[flue] Agent "${identity}" has an invalid locationHint static ("${String(locationHint)}"). ` +
				`Expected one of: ${AGENT_LOCATION_HINTS.join(', ')}.`,
		);
	}
	return locationHint;
}
