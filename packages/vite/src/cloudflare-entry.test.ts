import { describe, expect, it } from 'vitest';
import type { AgentScanResult } from './agent-scan.ts';
import { generateCloudflareEntry } from './cloudflare-entry.ts';

const agents: AgentScanResult[] = [
	{
		filePath: '/app/src/agents.ts',
		exportName: 'SupportAgent',
		identity: 'SupportAgent',
		className: 'FlueSupportAgentAgent',
		bindingName: 'FLUE_SUPPORT_AGENT_AGENT',
	},
	{
		filePath: '/app/src/agents.ts',
		exportName: 'SalesAgent',
		identity: 'SalesAgent',
		className: 'FlueSalesAgentAgent',
		bindingName: 'FLUE_SALES_AGENT_AGENT',
	},
];

describe('generateCloudflareEntry location hints', () => {
	it('resolves each agent function static into its own identity record', () => {
		const entry = generateCloudflareEntry({
			appEntry: '/app/src/app.ts',
			cloudflareEntry: undefined,
			agents,
			providers: [],
			tracing: false,
		});

		expect(entry).toContain('resolveAgentLocationHint,');
		expect(entry).toContain(
			'"SupportAgent": { bindingName: "FLUE_SUPPORT_AGENT_AGENT", className: "FlueSupportAgentAgent", locationHint: resolveAgentLocationHint(__flue_agent_module_0__["SupportAgent"]) },',
		);
		expect(entry).toContain(
			'"SalesAgent": { bindingName: "FLUE_SALES_AGENT_AGENT", className: "FlueSalesAgentAgent", locationHint: resolveAgentLocationHint(__flue_agent_module_0__["SalesAgent"]) },',
		);
	});

	it('passes the resolved hint to the real getAgentByName seam', () => {
		const entry = generateCloudflareEntry({
			appEntry: '/app/src/app.ts',
			cloudflareEntry: undefined,
			agents: agents.slice(0, 1),
			providers: [],
			tracing: false,
		});

		expect(entry).toContain(
			'async function fetchAgent(binding, instanceId, request, locationHint)',
		);
		expect(entry).toContain(
			'const options = locationHint === undefined ? undefined : { locationHint };',
		);
		expect(entry).toContain('getAgentByName(binding, instanceId, options)');
	});
});
