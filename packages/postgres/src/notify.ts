/** `pg_notify` channel for committed conversation-stream appends (payload: path). */
export const CONVERSATION_APPEND_CHANNEL = 'flue_conversation_append';

/** `pg_notify` channel for instance-owner claim wakes (payload: owner id). */
export const OWNER_WAKE_CHANNEL = 'flue_owner_wake';

/** `pg_notify` channel for durable abort requests (payload: session key). */
export const SUBMISSION_ABORT_CHANNEL = 'flue_submission_abort';

/**
 * Postgres rejects `pg_notify` payloads of 8000 bytes or more. A longer key
 * skips the signal; its readers and owners fall back to polling.
 */
export function fitsNotifyPayload(payload: string): boolean {
	return new TextEncoder().encode(payload).byteLength < 8000;
}
