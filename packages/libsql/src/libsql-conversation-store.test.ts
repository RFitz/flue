import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { defineConversationStreamStoreContractTests } from '@flue/runtime/test-utils/conversation-stream';
import { createClient } from '@libsql/client';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { type LibsqlRunner, libsql } from './index.ts';

function clientRunner(url: string): LibsqlRunner & { exec(sql: string): Promise<void> } {
	const client = createClient({ url });
	const toRows = (rs: { rows: ArrayLike<Record<string, unknown>>; columns: string[] }) =>
		Array.from(rs.rows, (row) =>
			Object.fromEntries(rs.columns.map((column) => [column, row[column]])),
		);
	let tail: Promise<unknown> = Promise.resolve();
	const serialize = <T>(operation: () => Promise<T>): Promise<T> => {
		const result = tail.then(operation, operation);
		tail = result.then(
			() => undefined,
			() => undefined,
		);
		return result;
	};
	return {
		query: (text, params = []) =>
			serialize(async () => toRows(await client.execute({ sql: text, args: params }))),
		transaction: (fn) =>
			serialize(async () => {
				const tx = await client.transaction('write');
				try {
					const result = await fn({
						query: async (text, params = []) =>
							toRows(await tx.execute({ sql: text, args: params })),
					});
					await tx.commit();
					return result;
				} catch (error) {
					await tx.rollback();
					throw error;
				}
			}),
		exec: (sql) =>
			serialize(async () => {
				await client.executeMultiple(sql);
			}),
		close: () => client.close(),
	};
}

let dir: string;
let runner: ReturnType<typeof clientRunner>;

beforeAll(async () => {
	dir = mkdtempSync(join(tmpdir(), 'flue-libsql-'));
	runner = clientRunner(`file:${join(dir, 'contract.db')}`);
	await libsql(runner).migrate?.();
});

afterAll(async () => {
	await runner.close();
	rmSync(dir, { recursive: true, force: true });
});

defineConversationStreamStoreContractTests('libSQL conversation stream store', {
	async create() {
		const connected = await libsql(runner).connect();
		if (!connected.conversationStreamStore) throw new Error('expected a conversation store');
		return {
			stream: connected.conversationStreamStore,
			submissionStore: connected.submissionStore,
		};
	},
	async cleanup() {
		await runner.exec(`
			DELETE FROM flue_conversation_streams;
			DELETE FROM flue_conversation_stream_batches;
			DELETE FROM flue_conversation_fold_checkpoints;
			DELETE FROM flue_agent_submissions;
			DELETE FROM flue_submission_chunks;
		`);
	},
});

describe('libSQL instance-owner lease migration', () => {
	it('adds the owner columns to a pre-existing streams table and stays idempotent', async () => {
		const legacy = clientRunner(`file:${join(dir, 'legacy.db')}`);
		try {
			const adapter = libsql(legacy);
			await adapter.migrate?.();
			// Roll the table back to its shape before the owner lease existed.
			await legacy.exec(`
				ALTER TABLE flue_conversation_streams DROP COLUMN owner_id;
				ALTER TABLE flue_conversation_streams DROP COLUMN owner_lease_expires_at;
			`);
			await legacy.query(
				`INSERT INTO flue_conversation_streams (path, identity_json, incarnation) VALUES (?, ?, ?)`,
				['agents/echo/legacy', '{"agentName":"echo","instanceId":"legacy"}', 'inc_legacy'],
			);
			await adapter.migrate?.();
			await adapter.migrate?.();

			const columns = (await legacy.query(`PRAGMA table_info(flue_conversation_streams)`))
				.map((row) => ({ name: String(row.name), notnull: Number(row.notnull) }))
				.filter((column) => column.name.startsWith('owner'));
			expect(columns).toEqual([
				{ name: 'owner_id', notnull: 0 },
				{ name: 'owner_lease_expires_at', notnull: 0 },
			]);
			const stream = (await adapter.connect()).conversationStreamStore;
			expect(
				await stream?.claimInstanceOwner?.('agents/echo/legacy', 'owner_a', {
					now: 1,
					leaseExpiresAt: 2,
				}),
			).toEqual({ owned: true, leaseExpiresAt: 2 });
		} finally {
			await legacy.close();
		}
	});
});
