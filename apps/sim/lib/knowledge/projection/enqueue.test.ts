/**
 * @vitest-environment node
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  runPass: vi.fn(),
  resolveRegion: vi.fn(),
  trigger: vi.fn(),
}))

vi.mock('@trigger.dev/sdk', () => ({ tasks: { trigger: mocks.trigger } }))
vi.mock('@/lib/core/async-jobs/region', () => ({ resolveTriggerRegion: mocks.resolveRegion }))
vi.mock('@/lib/core/config/env-flags', () => ({ isTriggerDevEnabled: true }))
vi.mock('@/lib/knowledge/projection/run', () => ({ runKnowledgeProjectionPass: mocks.runPass }))

import {
  enqueueKnowledgeProjectionSweep,
  requestKnowledgeProjection,
} from '@/lib/knowledge/projection/enqueue'

describe('knowledge projection enqueue', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-23T12:34:45.000Z'))
    mocks.resolveRegion.mockResolvedValue('us-east-1')
    mocks.trigger.mockResolvedValue({ id: 'run-1' })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('enqueues one sweep pass per window and never runs the pass inline', async () => {
    await expect(enqueueKnowledgeProjectionSweep()).resolves.toEqual({
      backend: 'trigger-dev',
      jobId: 'run-1',
    })
    expect(mocks.trigger).toHaveBeenCalledWith('knowledge-projection', undefined, {
      idempotencyKey: 'knowledge-projection:sweep:29836114',
      idempotencyKeyTTL: '5m',
      maxDuration: 900,
      region: 'us-east-1',
      ttl: '5m',
    })
    expect(mocks.runPass).not.toHaveBeenCalled()
  })

  it('debounces prompt requests across processes and collapses them within one', async () => {
    await requestKnowledgeProjection()
    await requestKnowledgeProjection()
    expect(mocks.trigger).toHaveBeenCalledTimes(1)
    expect(mocks.trigger).toHaveBeenCalledWith('knowledge-projection', undefined, {
      debounce: { key: 'knowledge-projection', delay: '5s', maxDelay: '1m' },
      maxDuration: 900,
      region: 'us-east-1',
    })
    vi.advanceTimersByTime(5_000)
    await requestKnowledgeProjection()
    expect(mocks.trigger).toHaveBeenCalledTimes(2)
  })

  it('never fails the write that asked when the request is refused', async () => {
    vi.advanceTimersByTime(60_000)
    mocks.trigger.mockRejectedValueOnce(new Error('trigger unavailable'))
    await expect(requestKnowledgeProjection()).resolves.toBeUndefined()
  })
})
