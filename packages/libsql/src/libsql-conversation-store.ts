import type { ConversationStreamStore } from '@flue/runtime/adapter';
import { defineSqlConversationStreamStore } from '@flue/runtime/adapter';
import type { LibsqlParameter, LibsqlRunner } from './libsql-adapter.ts';

export function createLibsqlConversationStreamStore(runner: LibsqlRunner): ConversationStreamStore {
	return defineSqlConversationStreamStore({
		placeholder: () => '?',
		lockClause: '',
		insertIgnorePrefix: 'INSERT',
		insertIgnoreSuffix: 'ON CONFLICT (path) DO NOTHING',
		supportsReturning: true,
		instanceOwnerLease: true,
		query: (sql, params) => runner.query(sql, params as LibsqlParameter[]),
		transaction: (fn) =>
			runner.transaction((tx) =>
				fn({ query: (sql, params) => tx.query(sql, params as LibsqlParameter[]) }),
			),
	});
}
