import { describe, expect, it } from 'vitest';
import type { Agent } from '../types.ts';
import { resolveAgentLocationHint } from './location-hint.ts';

const supportedHints = [
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
] as const;

describe('resolveAgentLocationHint', () => {
	it('supports plain function-static assignment', () => {
		function SupportAgent() {
			return 'Support the user.';
		}
		SupportAgent.locationHint = 'apac';

		const agent: Agent = SupportAgent;
		expect(resolveAgentLocationHint(agent)).toBe('apac');
	});

	it('accepts every Cloudflare Durable Object location hint', () => {
		for (const locationHint of supportedHints) {
			const agent = (() => undefined) as Agent;
			agent.locationHint = locationHint;
			expect(resolveAgentLocationHint(agent)).toBe(locationHint);
		}
	});

	it('returns undefined when the static is absent', () => {
		expect(resolveAgentLocationHint((() => undefined) as Agent)).toBeUndefined();
	});

	it.each(['moon', '', 42, null])('rejects an invalid static value (%j)', (locationHint) => {
		const agent = (() => undefined) as Agent;
		(agent as unknown as { locationHint: unknown }).locationHint = locationHint;

		expect(() => resolveAgentLocationHint(agent)).toThrow(
			/invalid locationHint static.*Expected one of: wnam, enam, sam, weur, eeur, apac, apac-ne, apac-se, oc, afr, me/,
		);
	});
});
