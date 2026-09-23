import {
  KNOWLEDGE_PROJECTION_ASYNC,
  KNOWLEDGE_PROJECTION_MODE_SETTING,
} from '@sim/db/knowledge-projection'
import {
  installSearchKeywordTrigger,
  installSearchVectorTrigger,
} from '@sim/db/script-migrations/0016_backfill_search_vectors'
import { installTinKeywordTrigger } from '@sim/db/script-migrations/0019_tin_keyword_projection'
import {
  installProjectionSourceAclSetTriggers,
  replaceProjectionSourceAclSync,
} from '@sim/db/script-migrations/0021_embedding_search_connector'
import type { ScriptMigration } from '@sim/db/script-migrations/types'
import { createLogger } from '@sim/logger'
import postgres, { type Sql, type TransactionSql } from 'postgres'
import { retryOnLockTimeout } from '../scripts/lock-timeout-retry'

const logger = createLogger('KnowledgeProjectionAsync')

/** Each attempt's wait for the table locks the triggers need; the retries find a quiet moment. */
const TRIGGER_LOCK_TIMEOUT = '5s'
const TRIGGER_LOCK_RETRY_BUDGET_MS = 20 * 60_000
const TRIGGER_LOCK_RETRY_BACKOFF = { baseMs: 2_000, maxMs: 30_000 } as const

/**
 * Whether the writing transaction deferred its projection rows. A chunk write marks its document
 * with content to project only then: a synchronous writer has already written the rows, and its
 * mark exists only so a projector pass that read the chunks earlier cannot settle over the write.
 * That pass sees the new generation and runs again; with no pass in flight, the next one only
 * verifies the source and ACL, and never reads the chunks' vectors.
 */
const DEFERRED = `current_setting('${KNOWLEDGE_PROJECTION_MODE_SETTING}', true) IS NOT DISTINCT FROM '${KNOWLEDGE_PROJECTION_ASYNC}'`

/**
 * Marks documents for the knowledge projector: a new mark starts at generation 1, and a mark
 * already waiting gains a generation and keeps whether any change touched chunk content. Documents
 * are marked in id order so writers marking several at once take the rows in one order.
 */
export async function installMarkFunctions(tx: Sql | TransactionSql): Promise<void> {
  await tx.unsafe(`CREATE OR REPLACE FUNCTION mark_knowledge_projection(p_document_ids text[], p_content boolean)
    RETURNS void LANGUAGE sql AS $$
      INSERT INTO knowledge_projection_dirty (document_id, content)
      SELECT marked.id, p_content
      FROM (SELECT DISTINCT unnest(p_document_ids) AS id) AS marked
      WHERE marked.id IS NOT NULL
      ORDER BY marked.id
      ON CONFLICT (document_id) DO UPDATE SET
        generation = knowledge_projection_dirty.generation + 1,
        content = knowledge_projection_dirty.content OR EXCLUDED.content
    $$`)
  await tx.unsafe(`CREATE OR REPLACE FUNCTION mark_inserted_embedding_projection()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      PERFORM mark_knowledge_projection(ARRAY(SELECT document_id FROM inserted_embeddings), ${DEFERRED});
      RETURN NULL;
    END;
    $$`)
  await tx.unsafe(`CREATE OR REPLACE FUNCTION mark_updated_embedding_projection()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      PERFORM mark_knowledge_projection(ARRAY[NEW.document_id, OLD.document_id], ${DEFERRED});
      RETURN NEW;
    END;
    $$`)
}

/**
 * Chunk writes mark their documents. An insert marks once per statement, from the statement's
 * inserted rows, so a processing commit that inserts a document's chunks in batches marks it once
 * per batch rather than once per chunk. An update marks on the columns the projections carry,
 * the same columns the synchronous triggers fire on. A delete marks nothing: the projections'
 * foreign keys remove their rows in the deleting statement.
 */
async function installMarkTriggers(tx: TransactionSql): Promise<void> {
  await tx.unsafe(`CREATE OR REPLACE TRIGGER embedding_projection_mark_insert
    AFTER INSERT ON embedding REFERENCING NEW TABLE AS inserted_embeddings
    FOR EACH STATEMENT EXECUTE FUNCTION mark_inserted_embedding_projection()`)
  await tx.unsafe(`CREATE OR REPLACE TRIGGER embedding_projection_mark_update
    AFTER UPDATE OF knowledge_base_id, document_id, enabled, content,
      embedding, embedding_384, embedding_768, embedding_1024, embedding_3072 ON embedding
    FOR EACH ROW EXECUTE FUNCTION mark_updated_embedding_projection()`)
}

/** Whether the Tin projection's embedding trigger exists here: `0019` installs it only where `tin` does. */
async function tinTriggerInstalled(tx: TransactionSql): Promise<boolean> {
  const [row] = await tx<Array<{ installed: boolean }>>`
    SELECT EXISTS (
      SELECT 1 FROM pg_trigger
      WHERE tgname = 'embedding_keyword_tin_sync' AND tgrelid = 'embedding'::regclass
    ) AS installed`
  return Boolean(row?.installed)
}

/**
 * Lets a writer defer its search projection rows to the knowledge projector, and marks every
 * document whose projection rows a write changes, in either mode.
 *
 * The triggers that write projection rows in the writer's transaction — the embedding triggers of
 * each projection, the projection triggers that copy a chunk's source and ACL, and the document
 * trigger's fan-out — are skipped when the transaction set `sim.projection_mode` to `async`, and
 * run as before otherwise. Every writer deployed before this release sets nothing, so it keeps
 * writing the rows itself; the app sets the mode only while its feature flag is on. The marks are
 * written in both modes, so a projector pass that read a document before a synchronous writer
 * changed it cannot settle over that change.
 *
 * All of it is installed in one transaction, so no write sees the guards without the marks. The
 * trigger DDL takes a short lock on `embedding` and on each projection, in the order writers take
 * them; each attempt waits at most {@link TRIGGER_LOCK_TIMEOUT} and is retried within the budget,
 * so it never queues writers behind it for longer than one attempt. Replacing the functions takes
 * no table lock. Idempotent.
 */
export async function installKnowledgeProjectionAsync(sql: Sql): Promise<void> {
  await retryOnLockTimeout(
    () =>
      sql.begin(async (tx) => {
        await tx.unsafe(`SET LOCAL lock_timeout = '${TRIGGER_LOCK_TIMEOUT}'`)
        await installMarkFunctions(tx)
        await replaceProjectionSourceAclSync(tx)
        await installSearchVectorTrigger(tx)
        await installSearchKeywordTrigger(tx)
        if (await tinTriggerInstalled(tx)) await installTinKeywordTrigger(tx)
        await installMarkTriggers(tx)
        await installProjectionSourceAclSetTriggers(tx)
      }),
    {
      budgetMs: TRIGGER_LOCK_RETRY_BUDGET_MS,
      backoff: TRIGGER_LOCK_RETRY_BACKOFF,
      onRetry: ({ attempt, delayMs }) =>
        logger.warn('Knowledge projection triggers waited out their lock timeout; retrying', {
          attempt,
          retryInMs: Math.round(delayMs),
        }),
    }
  )
}

export const knowledgeProjectionAsyncMigration: ScriptMigration = {
  name: '0024_knowledge_projection_async',
  up: installKnowledgeProjectionAsync,
}

/** Run directly by `db:push`, after the projection migrations it builds on. */
if (import.meta.main) {
  const url = process.env.MIGRATION_DATABASE_URL ?? process.env.DATABASE_URL
  if (!url) throw new Error('DATABASE_URL is required to install the knowledge projection triggers')
  const sql = postgres(url, { max: 1, max_lifetime: null, onnotice: () => undefined })
  try {
    await installKnowledgeProjectionAsync(sql)
  } finally {
    await sql.end()
  }
}
