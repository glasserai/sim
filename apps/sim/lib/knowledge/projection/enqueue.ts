import { createLogger } from '@sim/logger'
import { getErrorMessage } from '@sim/utils/errors'
import { isTriggerDevEnabled } from '@/lib/core/config/env-flags'

const logger = createLogger('KnowledgeProjectionEnqueue')

export const KNOWLEDGE_PROJECTION_TASK_ID = 'knowledge-projection'

/**
 * How long one pass starts pages before it hands the rest to the next. The task's duration leaves
 * minutes past it: a page started at the budget runs to its own statement timeout, and closing the
 * pass's connections follows.
 */
export const KNOWLEDGE_PROJECTION_PASS_BUDGET_MS = 8 * 60 * 1000
export const KNOWLEDGE_PROJECTION_MAX_DURATION_SECONDS = 15 * 60

/** The periodic sweep's window: at most one sweep run is enqueued per window. */
const KNOWLEDGE_PROJECTION_SWEEP_INTERVAL_MS = 60 * 1000

/**
 * A prompt request waits this long for more writes before its run starts, and is never pushed
 * back past the maximum, so a steady stream of writes still gets a run each window.
 */
const PROMPT_DEBOUNCE = { key: KNOWLEDGE_PROJECTION_TASK_ID, delay: '5s', maxDelay: '1m' } as const

/** Trigger.dev requests from one process closer together than this collapse into the first. */
const PROMPT_REQUEST_INTERVAL_MS = 5_000

let lastPromptAt = 0

/** The inline pass in flight when no Trigger.dev worker is configured, and whether another is owed. */
let inlinePass: Promise<void> | undefined
let inlinePassOwed = false

/**
 * Starts a pass in this process without waiting for it: at most one runs at a time, a request while
 * one runs is folded into a single pass after it, and a failed pass is logged without dropping one
 * owed after it. The
 * pass module loads on first use. For deployments without a Trigger.dev worker, whose sweep and
 * writes run passes here, as their document processing does.
 */
function runInline(): void {
  if (inlinePass) {
    inlinePassOwed = true
    return
  }
  inlinePass = (async () => {
    do {
      inlinePassOwed = false
      try {
        const { runKnowledgeProjectionPass } = await import('@/lib/knowledge/projection/run')
        await runKnowledgeProjectionPass({ budgetMs: KNOWLEDGE_PROJECTION_PASS_BUDGET_MS })
      } catch (error) {
        logger.error('Inline knowledge projection pass failed', { error: getErrorMessage(error) })
      }
    } while (inlinePassOwed)
  })().finally(() => {
    inlinePass = undefined
  })
}

/**
 * Asks for a projector pass soon after a knowledge write commits, so its marked documents are
 * converged within seconds rather than at the next sweep. Debounced twice: in this process, and
 * across processes by the task's debounce key. Without a Trigger.dev worker the pass runs in this
 * process, one at a time, as document processing does there. Never throws: a request that fails
 * leaves the marks to the sweep, which keeps enqueueing a pass every minute until one runs.
 */
export async function requestKnowledgeProjection(): Promise<void> {
  if (!isTriggerDevEnabled) {
    runInline()
    return
  }
  const now = Date.now()
  if (now - lastPromptAt < PROMPT_REQUEST_INTERVAL_MS) return
  lastPromptAt = now
  try {
    const [{ tasks }, { resolveTriggerRegion }] = await Promise.all([
      import('@trigger.dev/sdk'),
      import('@/lib/core/async-jobs/region'),
    ])
    await tasks.trigger(KNOWLEDGE_PROJECTION_TASK_ID, undefined, {
      debounce: PROMPT_DEBOUNCE,
      maxDuration: KNOWLEDGE_PROJECTION_MAX_DURATION_SECONDS,
      region: await resolveTriggerRegion(),
    })
  } catch (error) {
    logger.warn('Knowledge projection request failed; the sweep will pick the marks up', {
      error: getErrorMessage(error),
    })
  }
}

export interface KnowledgeProjectionSweepResult {
  backend: 'trigger-dev' | 'inline'
  jobId: string | null
}

/**
 * The periodic sweep behind the prompt requests: one pass per window, whatever else ran, so a
 * mark whose request was lost, or a document a pass gave up, is still converged.
 */
export async function enqueueKnowledgeProjectionSweep(): Promise<KnowledgeProjectionSweepResult> {
  if (!isTriggerDevEnabled) {
    runInline()
    return { backend: 'inline', jobId: null }
  }
  const [{ tasks }, { resolveTriggerRegion }] = await Promise.all([
    import('@trigger.dev/sdk'),
    import('@/lib/core/async-jobs/region'),
  ])
  const window = Math.floor(Date.now() / KNOWLEDGE_PROJECTION_SWEEP_INTERVAL_MS)
  const handle = await tasks.trigger(KNOWLEDGE_PROJECTION_TASK_ID, undefined, {
    idempotencyKey: `${KNOWLEDGE_PROJECTION_TASK_ID}:sweep:${window}`,
    idempotencyKeyTTL: '5m',
    maxDuration: KNOWLEDGE_PROJECTION_MAX_DURATION_SECONDS,
    region: await resolveTriggerRegion(),
    ttl: '5m',
  })
  return { backend: 'trigger-dev', jobId: handle.id }
}
