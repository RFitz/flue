import type { ConversationStreamStore } from '@flue/runtime/adapter';
import { defineSqlConversationStreamStore } from '@flue/runtime/adapter';
import { CONVERSATION_APPEND_CHANNEL, fitsNotifyPayload } from './notify.ts';
import type { PostgresParameter, PostgresRunner } from './postgres-adapter.ts';

export function createPgConversationStreamStore(runner: PostgresRunner): ConversationStreamStore {
	const { listen } = runner;
	return defineSqlConversationStreamStore({
		placeholder: (index) => `$${index}`,
		lockClause: 'FOR UPDATE',
		insertIgnorePrefix: 'INSERT',
		insertIgnoreSuffix: 'ON CONFLICT (path) DO NOTHING',
		supportsReturning: true,
		query: (sql, params) => runner.query(sql, params as PostgresParameter[]),
		transaction: (fn) =>
			runner.transaction((tx) =>
				fn({ query: (sql, params) => tx.query(sql, params as PostgresParameter[]) }),
			),
		async notifyAppend(tx, path) {
			if (!fitsNotifyPayload(path)) return;
			await tx.query('SELECT pg_notify($1, $2)', [CONVERSATION_APPEND_CHANNEL, path]);
		},
		...(listen
			? {
					listenAppends: (onAppend: (path: string) => void) =>
						listen.call(runner, CONVERSATION_APPEND_CHANNEL, onAppend),
				}
			: {}),
	});
}
