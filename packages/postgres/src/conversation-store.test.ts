import { PGlite } from '@electric-sql/pglite';
import { defineConversationStreamStoreContractTests } from '@flue/runtime/test-utils/conversation-stream';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { type PostgresRunner, postgres } from './index.ts';

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

let db: PGlite;

beforeAll(async () => {
	db = new PGlite();
	await postgres(pgliteRunner(db)).migrate?.();
}, 60_000);

afterAll(async () => {
	await db.close();
});

defineConversationStreamStoreContractTests('Postgres conversation stream store', {
	async create() {
		const connected = await postgres(pgliteRunner(db)).connect();
		if (!connected.conversationStreamStore) throw new Error('expected a conversation store');
		return {
			stream: connected.conversationStreamStore,
			submissionStore: connected.submissionStore,
		};
	},
	async cleanup() {
		await db.exec(`
			TRUNCATE flue_conversation_streams, flue_conversation_stream_batches,
				flue_conversation_fold_checkpoints, flue_agent_submissions, flue_submission_chunks
		`);
	},
});

describe('Postgres instance-owner lease migration', () => {
	it('adds the owner columns to a pre-existing streams table and stays idempotent', async () => {
		const legacy = new PGlite();
		try {
			const adapter = postgres(pgliteRunner(legacy));
			await adapter.migrate?.();
			// Roll the table back to its shape before the owner lease existed.
			await legacy.exec(`
				ALTER TABLE flue_conversation_streams DROP COLUMN owner_id;
				ALTER TABLE flue_conversation_streams DROP COLUMN owner_lease_expires_at;
			`);
			await legacy.query(
				`INSERT INTO flue_conversation_streams (path, identity_json, incarnation) VALUES ($1, $2, $3)`,
				['agents/echo/legacy', '{"agentName":"echo","instanceId":"legacy"}', 'inc_legacy'],
			);
			await adapter.migrate?.();
			await adapter.migrate?.();

			const columns = await legacy.query<{ column_name: string; is_nullable: string }>(
				`SELECT column_name, is_nullable FROM information_schema.columns
				 WHERE table_name = 'flue_conversation_streams' AND column_name LIKE 'owner%'
				 ORDER BY column_name`,
			);
			expect(columns.rows).toEqual([
				{ column_name: 'owner_id', is_nullable: 'YES' },
				{ column_name: 'owner_lease_expires_at', is_nullable: 'YES' },
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
