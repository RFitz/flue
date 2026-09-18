import { describe, expect, it, vi } from 'vitest';
import type { DispatchInput } from '../runtime/dispatch-queue.ts';
import type { AgentLocationHint } from '../types.ts';
import { createCloudflareWorkerConfig } from './worker-config.ts';

const binding = {};
const identity = {
	bindingName: 'FLUE_SUPPORT_AGENT',
	className: 'FlueSupportAgent',
	locationHint: 'apac' as AgentLocationHint,
};

function dispatchInput(): DispatchInput {
	return {
		submissionId: 'submission-1',
		agent: 'Support',
		id: 'customer-1',
		message: { kind: 'user', body: 'Hello' },
		acceptedAt: '2026-09-18T00:00:00.000Z',
	};
}

describe('createCloudflareWorkerConfig location hints', () => {
	it('forwards the hint through route, dispatch, and instance lookup first touches', async () => {
		const fetchAgent = vi.fn(
			async (_binding: unknown, _id: string, request: Request, _hint?: AgentLocationHint) => {
				const pathname = new URL(request.url).pathname;
				if (pathname.endsWith('/dispatch')) {
					return Response.json({
						submissionId: 'submission-1',
						acceptedAt: '2026-09-18T00:00:00.000Z',
						uid: 'instance-uid',
					});
				}
				if (pathname.endsWith('/instance-info')) {
					return Response.json({ exists: true, uid: 'instance-uid' });
				}
				return new Response('ok');
			},
		);
		const config = createCloudflareWorkerConfig({
			env: { FLUE_SUPPORT_AGENT: binding },
			agentIdentities: { Support: identity },
			fetchAgent,
		});

		await config.routeAgentRequest(
			new Request('https://example.test/agents/Support/customer-1'),
			undefined,
			{
				agentName: 'Support',
				instanceId: 'customer-1',
			},
		);
		await config.dispatchQueue.enqueue(dispatchInput());
		await config.instanceInfo('Support', 'customer-1');

		expect(fetchAgent).toHaveBeenCalledTimes(3);
		for (const call of fetchAgent.mock.calls) {
			expect(call[0]).toBe(binding);
			expect(call[1]).toBe('customer-1');
			expect(call[3]).toBe('apac');
		}
	});

	it('preserves undefined for agents without a hint', async () => {
		const fetchAgent = vi.fn(async () => new Response('ok'));
		const config = createCloudflareWorkerConfig({
			env: { FLUE_SUPPORT_AGENT: binding },
			agentIdentities: {
				Support: { bindingName: identity.bindingName, className: identity.className },
			},
			fetchAgent,
		});

		await config.routeAgentRequest(
			new Request('https://example.test/agents/Support/customer-1'),
			undefined,
			{
				agentName: 'Support',
				instanceId: 'customer-1',
			},
		);

		expect(fetchAgent).toHaveBeenCalledWith(binding, 'customer-1', expect.any(Request), undefined);
	});

	it('uses request env bindings without losing the configured hint', async () => {
		const requestBinding = {};
		const fetchAgent = vi.fn(async () => new Response('ok'));
		const config = createCloudflareWorkerConfig({
			env: { FLUE_SUPPORT_AGENT: binding },
			agentIdentities: { Support: identity },
			fetchAgent,
		});

		await config.routeAgentRequest(
			new Request('https://example.test/agents/Support/customer-1'),
			{
				FLUE_SUPPORT_AGENT: requestBinding,
			},
			{
				agentName: 'Support',
				instanceId: 'customer-1',
			},
		);

		expect(fetchAgent).toHaveBeenCalledWith(
			requestBinding,
			'customer-1',
			expect.any(Request),
			'apac',
		);
	});
});
