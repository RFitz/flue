import {
	type AssistantMessage,
	fauxAssistantMessage,
	fauxProvider,
	fauxText,
} from '@earendil-works/pi-ai';
import { PGlite } from '@electric-sql/pglite';
import { useModel } from '@flue/runtime';
import {
	agentStreamPath,
	createFlueContext,
	createNodeAgentCoordinator,
	registerFlueAgents,
	resetFlueAgentRegistrationForTests,
	resolveModel,
	setProvider,
} from '@flue/runtime/internal';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { type PostgresRunner, postgres } from './index.ts';

// Two Node coordinators over one Postgres database, each with its own store
// objects and owner id: every process-local cache (conversation writers, fold
// hosts, listener registries, abort controllers) is separate, exactly as it is
// between two real processes. PGlite serializes transactions on its single
// connection, so these tests exercise ownership churn, not lock contention.

function Echo() {
	useModel('faux/model');
	return 'Reply to the user.';
}

const faux = fauxProvider({ models: [{ id: 'model' }] });

function pgliteRunner(db: PGlite): PostgresRunner {
	return {
		query: async (text, params) => (await db.query(text, params)).rows as Record<string, unknown>[],
		transaction: (fn) =>
			db.transaction((tx) =>
				fn({
					query: async (text, params) =>
						(await tx.query(text, params)).rows as Record<string, unknown>[],
				}),
			),
		close() {},
	};
}

type NodeProcess = Awaited<ReturnType<typeof startProcess>>;

async function startProcess(db: PGlite) {
	const adapter = postgres(pgliteRunner(db));
	const connected = await adapter.connect();
	const { conversationStreamStore, attachmentStore } = connected;
	if (!conversationStreamStore || !attachmentStore) {
		throw new Error('postgres adapter did not provide conversation stores');
	}
	const stores = { ...connected, conversationStreamStore, attachmentStore };
	const coordinator = createNodeAgentCoordinator({
		submissions: stores.submissionStore,
		agents: [{ name: 'Echo', agent: Echo }],
		createContext: ({ id, agentName, request, submissionId }) =>
			createFlueContext({
				id,
				agentName,
				submissionId,
				env: {},
				req: request,
				agentConfig: { resolveModel },
			}),
		conversationStreamStore: stores.conversationStreamStore,
		attachmentStore: stores.attachmentStore,
		env: {},
		timings: { heartbeatIntervalMs: 100 },
	});
	return { coordinator, stores };
}

let sequence = 0;
async function admit(proc: NodeProcess, instanceId: string, body: string): Promise<string> {
	sequence += 1;
	const submissionId = `sub_mp${Date.now()}x${sequence}`;
	const admission = await proc.coordinator.admitDispatch({
		submissionId,
		agent: 'Echo',
		id: instanceId,
		message: { kind: 'user', body },
		acceptedAt: new Date().toISOString(),
	});
	expect(admission.kind).toBe('submission');
	return submissionId;
}

/**
 * The row's lifecycle status, its attempt count, and the outcome of its
 * `submission_settled` record (the outcome authority; the row only says
 * `settled`).
 */
async function settledStatus(proc: NodeProcess, instanceId: string, submissionId: string) {
	const row = await proc.stores.submissionStore.getSubmission(submissionId);
	let outcome: string | undefined;
	let offset = '-1';
	while (true) {
		const page = await proc.stores.conversationStreamStore.read(
			agentStreamPath('Echo', instanceId),
			{
				offset,
			},
		);
		for (const batch of page.batches) {
			for (const record of batch.records) {
				if (record.type === 'submission_settled' && record.submissionId === submissionId) {
					outcome = record.outcome;
				}
			}
		}
		offset = page.nextOffset;
		if (page.upToDate) break;
	}
	return { status: row?.status, outcome, attemptCount: row?.attemptCount };
}

const completedFirstTry = { status: 'settled', outcome: 'completed', attemptCount: 1 };

async function eventually(check: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await check()) return true;
		await new Promise((resolve) => setTimeout(resolve, 25));
	}
	return check();
}

function reply(text: string): AssistantMessage {
	return fauxAssistantMessage([fauxText(text)], { stopReason: 'stop' });
}

function gate() {
	let open!: () => void;
	const opened = new Promise<void>((resolve) => {
		open = resolve;
	});
	return { open, opened };
}

describe('Node coordinators sharing one Postgres', { timeout: 30_000 }, () => {
	let db: PGlite;
	let a: NodeProcess;
	let b: NodeProcess;

	beforeAll(async () => {
		db = new PGlite();
		await postgres(pgliteRunner(db)).migrate?.();
		setProvider(faux.provider);
		registerFlueAgents([{ identity: 'Echo', agent: Echo }]);
	}, 60_000);

	afterAll(async () => {
		resetFlueAgentRegistrationForTests();
		await db.close();
	});

	beforeEach(async () => {
		a = await startProcess(db);
		b = await startProcess(db);
	});

	afterEach(async () => {
		await Promise.allSettled([a.coordinator.shutdown(1000), b.coordinator.shutdown(1000)]);
	});

	// #705: B's producer acquire bumps the epoch; A's cached writer is stale.
	it('settles a conversation that moves A → B → A on its first attempt', async () => {
		faux.setResponses([reply('one'), reply('two'), reply('three')]);
		const id = 'churn';

		const first = await admit(a, id, 'first');
		await a.coordinator.waitForIdle();
		expect(await settledStatus(a, id, first)).toEqual(completedFirstTry);

		const second = await admit(b, id, 'second');
		await b.coordinator.waitForIdle();
		expect(await settledStatus(b, id, second)).toEqual(completedFirstTry);

		const third = await admit(a, id, 'third');
		await a.coordinator.waitForIdle();
		expect(await settledStatus(a, id, third)).toEqual(completedFirstTry);
	});

	// A second process that merely touches the conversation (here: admitting
	// the next message) acquires the producer and fences out the live attempt.
	it('does not kill a live attempt when another process admits to the same conversation', async () => {
		const held = gate();
		const started = gate();
		faux.setResponses([
			async () => {
				started.open();
				await held.opened;
				return reply('one');
			},
			reply('two'),
		]);
		const id = 'steal';

		const first = await admit(a, id, 'first');
		await started.opened;
		const second = await admit(b, id, 'second');
		held.open();

		expect(
			await eventually(
				async () => (await settledStatus(a, id, first)).outcome === 'completed',
				3000,
			),
		).toBe(true);
		expect(await settledStatus(a, id, first)).toEqual(completedFirstTry);
		expect(
			await eventually(
				async () => (await settledStatus(a, id, second)).outcome === 'completed',
				3000,
			),
		).toBe(true);
	});

	// A stalled owner whose lease another process reclaimed must stop its
	// attempt (ending its model and tool calls) and leave the row alone.
	it('stops a live attempt whose lease another process reclaimed', async () => {
		const started = gate();
		const stopped = gate();
		faux.setResponses([
			async (_context, options) => {
				started.open();
				await new Promise<void>((resolve) => {
					options?.signal?.addEventListener('abort', () => resolve(), { once: true });
				});
				stopped.open();
				return reply('stopped');
			},
		]);
		const id = 'reclaimed';

		const first = await admit(a, id, 'first');
		await started.opened;
		const running = await b.stores.submissionStore.getSubmission(first);
		if (!running?.attemptId) throw new Error('expected a running attempt');
		const reclaimed = await b.stores.submissionStore.replaceSubmissionAttempt(
			{ submissionId: first, attemptId: running.attemptId },
			'att_reclaimed',
			{ ownerId: 'owner_b', leaseExpiresAt: Date.now() + 30_000 },
		);
		expect(reclaimed?.attemptId).toBe('att_reclaimed');

		const outcome = await Promise.race([
			stopped.opened.then(() => 'stopped'),
			new Promise((resolve) => setTimeout(() => resolve('still running'), 2000)),
		]);
		expect(outcome).toBe('stopped');
		await a.coordinator.waitForIdle();
		const row = await b.stores.submissionStore.getSubmission(first);
		expect(row).toMatchObject({ status: 'running', attemptId: 'att_reclaimed' });
	});

	// abortInstance only fires controllers in its own process; the owner
	// notices the durable intent on its next deadline scan (up to 15s).
	it('aborts a live attempt promptly when the abort arrives at another process', async () => {
		const started = gate();
		faux.setResponses([
			async (_context, options) => {
				started.open();
				await new Promise<void>((resolve) => {
					options?.signal?.addEventListener('abort', () => resolve(), { once: true });
				});
				return reply('aborted');
			},
		]);
		const id = 'abort';

		const first = await admit(a, id, 'first');
		await started.opened;
		expect(await b.coordinator.abortInstance('Echo', id)).toBe(true);

		expect(
			await eventually(async () => (await settledStatus(a, id, first)).outcome === 'aborted', 3000),
		).toBe(true);
	});

	// Stream listeners are an in-process registry: B's long-poll/SSE readers
	// only learn about A's appends by polling.
	it('notifies stream subscribers in another process when a conversation appends', async () => {
		faux.setResponses([reply('one')]);
		const id = 'notify';
		const path = agentStreamPath('Echo', id);
		let notified = 0;
		const unsubscribe = b.stores.conversationStreamStore.subscribe(path, () => {
			notified += 1;
		});
		try {
			await admit(a, id, 'first');
			await a.coordinator.waitForIdle();
			expect(await eventually(async () => notified > 0, 1000)).toBe(true);
		} finally {
			unsubscribe();
		}
	});
});
