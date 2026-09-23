import { resolveDbUrl } from '@sim/db'
import {
  type KnowledgeProjectionProgress,
  markUnfilledProjectionDocuments,
  runKnowledgeProjection,
} from '@sim/db/knowledge-projection'
import { createLogger } from '@sim/logger'
import postgres from 'postgres'
import { env, envNumber } from '@/lib/core/config/env'
import { isFeatureEnabled } from '@/lib/core/config/feature-flags'

const logger = createLogger('KnowledgeProjectionPass')

/**
 * Documents one pass projects at once. Measured locally on content-heavy load, vector and keyword
 * pages scale to four (about three times one worker's rows per second) and gain little past it,
 * so four is the default; `KB_CONFIG_PROJECTION_CONCURRENCY` tunes it per deployment.
 */
const PROJECTION_CONCURRENCY = Math.max(1, envNumber(env.KB_CONFIG_PROJECTION_CONCURRENCY, 4))

export interface KnowledgeProjectionPassResult extends KnowledgeProjectionProgress {
  /** Documents the source and ACL fill marked during the pass. */
  filled: number
}

/**
 * One pass of the knowledge projector: converges every marked document, then, while time is
 * left and the fill is on, marks the documents of projection rows the source and ACL fill has not
 * reached and converges those too, a few at a time so fresh writes never queue behind much of it
 * and search keeps probing a small set of marks. The fill is its own flag so an operator can pause
 * its extra index writes.
 *
 * Workers project documents in parallel, each on a connection of its own that holds its
 * per-document advisory locks; a pass this long should not hold the pool's connections. Workers
 * read the same oldest marks and split them at those locks. A round ends when every worker found
 * nothing more it could take; the pass goes on while rounds settle documents, and `remaining`
 * reports marks it left, so the caller can schedule another.
 */
export async function runKnowledgeProjectionPass(options: {
  budgetMs: number
}): Promise<KnowledgeProjectionPassResult> {
  const url = resolveDbUrl('DATABASE_URL', process.env.SIM_DB_ROLE?.trim() || 'web')
  if (!url) throw new Error('DATABASE_URL is required to run the knowledge projector')
  const sessions = Array.from({ length: PROJECTION_CONCURRENCY }, () =>
    postgres(url, { max: 1, max_lifetime: null, onnotice: () => undefined })
  )
  const deadline = Date.now() + options.budgetMs
  const result: KnowledgeProjectionPassResult = {
    settled: 0,
    deferred: 0,
    pages: 0,
    written: 0,
    remaining: false,
    filled: 0,
  }
  try {
    /** `null` ends the fill: it is off, or every unfilled row has been read. */
    let fillCursor: Parameters<typeof markUnfilledProjectionDocuments>[1] | null =
      (await isFeatureEnabled('knowledge-projection-fill')) ? undefined : null
    while (Date.now() < deadline) {
      const round = await Promise.all(
        sessions.map((session) =>
          runKnowledgeProjection(session, { budgetMs: Math.max(0, deadline - Date.now()) })
        )
      )
      const settled = round.reduce((sum, progress) => sum + progress.settled, 0)
      result.settled += settled
      result.pages += round.reduce((sum, progress) => sum + progress.pages, 0)
      result.written += round.reduce((sum, progress) => sum + progress.written, 0)
      result.remaining = round.some((progress) => progress.remaining)
      if (result.remaining) {
        if (settled > 0) continue
        result.deferred = round.reduce((most, progress) => Math.max(most, progress.deferred), 0)
        break
      }
      if (fillCursor === null) break
      const fill = await markUnfilledProjectionDocuments(sessions[0], fillCursor)
      result.filled += fill.marked
      fillCursor = fill.cursor
      if (fill.marked === 0 && fillCursor === null) break
    }
    logger.info('Knowledge projection pass finished', result)
    return result
  } finally {
    await Promise.all(sessions.map((session) => session.end()))
  }
}
