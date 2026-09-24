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

function pgliteRunner(db: PGlite, options: { listen?: boolean } = {}): PostgresRunner {
	return {
		...(options.listen === false
			? {}
			: {
					listen: (channel: string, onNotify: (payload: string) => void) =>
						db.listen(channel, onNotify),
				}),
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

async function startProcess(db: PGlite, options: { listen?: boolean; idleTtlMs?: number } = {}) {
	const adapter = postgres(pgliteRunner(db, options));
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
		timings: { heartbeatIntervalMs: 100, abortPollIntervalMs: 100, ownerPollIntervalMs: 100 },
		...(options.idleTtlMs !== undefined ? { ownership: { idleTtlMs: options.idleTtlMs } } : {}),
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

/** The coordinator owner id that ran a submission's (last) attempt. */
async function ranBy(proc: NodeProcess, submissionId: string): Promise<string | undefined> {
	return (await proc.stores.submissionStore.getSubmission(submissionId))?.ownerId;
}

/** The instance-owner lease as stored on the conversation stream row. */
async function instanceOwner(db: PGlite, instanceId: string) {
	const result = await db.query<{ owner_id: string | null; owner_lease_expires_at: string | null }>(
		'SELECT owner_id, owner_lease_expires_at FROM flue_conversation_streams WHERE path = $1',
		[agentStreamPath('Echo', instanceId)],
	);
	const row = result.rows[0];
	return {
		ownerId: row?.owner_id ?? null,
		leaseExpiresAt: row?.owner_lease_expires_at == null ? null : Number(row.owner_lease_expires_at),
	};
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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
	// Each owner goes cold between turns, so the conversation really moves.
	it('settles a conversation that moves A → B → A on its first attempt', async () => {
		const coldA = await startProcess(db, { idleTtlMs: 150 });
		const coldB = await startProcess(db, { idleTtlMs: 150 });
		faux.setResponses([reply('one'), reply('two'), reply('three')]);
		const id = 'churn';
		try {
			const first = await admit(coldA, id, 'first');
			await coldA.coordinator.waitForIdle();
			expect(await settledStatus(coldA, id, first)).toEqual(completedFirstTry);
			await sleep(250);

			const second = await admit(coldB, id, 'second');
			await coldB.coordinator.waitForIdle();
			expect(await settledStatus(coldB, id, second)).toEqual(completedFirstTry);
			await sleep(250);

			const third = await admit(coldA, id, 'third');
			await coldA.coordinator.waitForIdle();
			expect(await settledStatus(coldA, id, third)).toEqual(completedFirstTry);

			const owners = [await ranBy(coldA, first), await ranBy(coldA, second), await ranBy(coldA, third)];
			expect(owners[0]).not.toBe(owners[1]);
			expect(owners[2]).toBe(owners[0]);
		} finally {
			await Promise.allSettled([coldA.coordinator.shutdown(1000), coldB.coordinator.shutdown(1000)]);
		}
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

	it('aborts a live attempt in another process by polling when the driver cannot LISTEN', async () => {
		const owner = await startProcess(db, { listen: false });
		const requester = await startProcess(db, { listen: false });
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
		const id = 'abort-poll';
		try {
			const first = await admit(owner, id, 'first');
			await started.opened;
			expect(await requester.coordinator.abortInstance('Echo', id)).toBe(true);
			expect(
				await eventually(
					async () => (await settledStatus(owner, id, first)).outcome === 'aborted',
					3000,
				),
			).toBe(true);
		} finally {
			await Promise.allSettled([
				owner.coordinator.shutdown(1000),
				requester.coordinator.shutdown(1000),
			]);
		}
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

	// ── Named owner (sticky while hot, cold when idle) ────────────────────

	it('keeps a hot conversation on its owner when another process admits', async () => {
		faux.setResponses([reply('one'), reply('two')]);
		const id = 'hot-owner';

		const first = await admit(a, id, 'first');
		await a.coordinator.waitForIdle();
		const owner = await ranBy(a, first);
		expect((await instanceOwner(db, id)).ownerId).toBe(owner);

		// B admits into A's hot window: B must not claim, A is nudged and does.
		const second = await admit(b, id, 'second');
		expect(
			await eventually(
				async () => (await settledStatus(a, id, second)).outcome === 'completed',
				3000,
			),
		).toBe(true);
		expect(await settledStatus(a, id, second)).toEqual(completedFirstTry);
		expect(await ranBy(a, second)).toBe(owner);
	});

	it('nudges a hot owner by polling when the driver cannot LISTEN', async () => {
		const owner = await startProcess(db, { listen: false });
		const other = await startProcess(db, { listen: false });
		faux.setResponses([reply('one'), reply('two')]);
		const id = 'hot-owner-poll';
		try {
			const first = await admit(owner, id, 'first');
			await owner.coordinator.waitForIdle();
			const second = await admit(other, id, 'second');
			expect(
				await eventually(
					async () => (await settledStatus(owner, id, second)).outcome === 'completed',
					3000,
				),
			).toBe(true);
			expect(await ranBy(owner, second)).toBe(await ranBy(owner, first));
		} finally {
			await Promise.allSettled([owner.coordinator.shutdown(1000), other.coordinator.shutdown(1000)]);
		}
	});

	it('does not let another process steal a hot conversation even when its owner is silent', async () => {
		// The owner never learns of the new work (no LISTEN, and a poll period
		// far past the test): the row must wait in the queue, not move.
		const owner = await startProcess(db, { listen: false });
		const other = await startProcess(db, { listen: false });
		faux.setResponses([reply('one'), reply('two')]);
		const id = 'no-steal';
		try {
			const first = await admit(owner, id, 'first');
			await owner.coordinator.waitForIdle();
			await owner.coordinator.shutdown(1000).catch(() => {});
			// Shutdown released the lease; re-take it as a silent holder.
			const ownerId = await ranBy(owner, first);
			if (!ownerId) throw new Error('expected an owner');
			const now = Date.now();
			await owner.stores.conversationStreamStore.claimInstanceOwner?.(
				agentStreamPath('Echo', id),
				ownerId,
				{ now, leaseExpiresAt: now + 30_000 },
			);

			const second = await admit(other, id, 'second');
			await sleep(500);
			expect((await other.stores.submissionStore.getSubmission(second))?.status).toBe('queued');
			expect((await instanceOwner(db, id)).ownerId).toBe(ownerId);
		} finally {
			// Leave the shared database without a stray hot lease or queued row.
			await db.query(
				'UPDATE flue_conversation_streams SET owner_id = NULL, owner_lease_expires_at = NULL WHERE path = $1',
				[agentStreamPath('Echo', id)],
			);
			await other.coordinator.abortInstance('Echo', id);
			await other.coordinator.waitForIdle();
			await other.coordinator.shutdown(1000);
		}
	});

	it('lets another process claim once the owner has been idle past the TTL', async () => {
		const owner = await startProcess(db, { idleTtlMs: 200 });
		const other = await startProcess(db, { idleTtlMs: 200 });
		faux.setResponses([reply('one'), reply('two')]);
		const id = 'cold-owner';
		try {
			const first = await admit(owner, id, 'first');
			await owner.coordinator.waitForIdle();
			await sleep(300);

			const second = await admit(other, id, 'second');
			await other.coordinator.waitForIdle();
			expect(await settledStatus(other, id, second)).toEqual(completedFirstTry);
			const newOwner = await ranBy(other, second);
			expect(newOwner).not.toBe(await ranBy(owner, first));
			expect((await instanceOwner(db, id)).ownerId).toBe(newOwner);
		} finally {
			await Promise.allSettled([owner.coordinator.shutdown(1000), other.coordinator.shutdown(1000)]);
		}
	});

	it('does not keep an owner hot on reads, history, or stream subscriptions', async () => {
		const owner = await startProcess(db, { idleTtlMs: 300 });
		const other = await startProcess(db, { idleTtlMs: 300 });
		faux.setResponses([reply('one'), reply('two')]);
		const id = 'reads-stay-cold';
		const path = agentStreamPath('Echo', id);
		try {
			const first = await admit(owner, id, 'first');
			await owner.coordinator.waitForIdle();
			const lease = await instanceOwner(db, id);
			expect(lease.ownerId).toBe(await ranBy(owner, first));

			// Observe the conversation from the owner for longer than the TTL.
			const unsubscribe = owner.stores.conversationStreamStore.subscribe(path, () => {});
			const readUntil = Date.now() + 400;
			while (Date.now() < readUntil) {
				await owner.stores.conversationStreamStore.read(path);
				await owner.stores.conversationStreamStore.getMeta(path);
				await settledStatus(owner, id, first);
				await sleep(20);
			}
			unsubscribe();
			expect(await instanceOwner(db, id)).toEqual(lease);

			const second = await admit(other, id, 'second');
			await other.coordinator.waitForIdle();
			expect(await ranBy(other, second)).not.toBe(lease.ownerId);
		} finally {
			await Promise.allSettled([owner.coordinator.shutdown(1000), other.coordinator.shutdown(1000)]);
		}
	});

	it('hands a conversation over at once when its owner shuts down', async () => {
		const owner = await startProcess(db);
		faux.setResponses([reply('one'), reply('two')]);
		const id = 'handover';
		try {
			const first = await admit(owner, id, 'first');
			await owner.coordinator.waitForIdle();
			await owner.coordinator.shutdown(1000);
			expect((await instanceOwner(db, id)).ownerId).toBeNull();

			const second = await admit(b, id, 'second');
			await b.coordinator.waitForIdle();
			expect(await settledStatus(b, id, second)).toEqual(completedFirstTry);
			expect(await ranBy(b, second)).not.toBe(await ranBy(owner, first));
		} finally {
			await owner.coordinator.shutdown(1000);
		}
	});
});
