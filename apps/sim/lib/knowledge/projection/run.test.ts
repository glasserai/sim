/**
 * @vitest-environment node
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  runProjection: vi.fn(),
  markUnfilled: vi.fn(),
  isFeatureEnabled: vi.fn(),
  end: vi.fn(),
}))

vi.mock('@sim/db', () => ({ resolveDbUrl: () => 'postgresql://fixture/sim_acl_test' }))
vi.mock('@sim/db/knowledge-projection', () => ({
  runKnowledgeProjection: mocks.runProjection,
  markUnfilledProjectionDocuments: mocks.markUnfilled,
}))
vi.mock('postgres', () => ({ default: () => ({ end: mocks.end }) }))
vi.mock('@/lib/core/config/env', () => ({
  env: { KB_CONFIG_PROJECTION_CONCURRENCY: 2 },
  envNumber: (value: unknown, fallback: number) => (typeof value === 'number' ? value : fallback),
}))
vi.mock('@/lib/core/config/feature-flags', () => ({ isFeatureEnabled: mocks.isFeatureEnabled }))

import { runKnowledgeProjectionPass } from '@/lib/knowledge/projection/run'

const drained = { settled: 1, deferred: 0, pages: 2, written: 3, remaining: false }

describe('runKnowledgeProjectionPass', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.runProjection.mockResolvedValue(drained)
    mocks.isFeatureEnabled.mockResolvedValue(false)
  })

  it('projects with one worker per connection and closes them all', async () => {
    await expect(runKnowledgeProjectionPass({ budgetMs: 60_000 })).resolves.toMatchObject({
      settled: 2,
      written: 6,
      filled: 0,
      remaining: false,
    })
    expect(mocks.runProjection).toHaveBeenCalledTimes(2)
    expect(mocks.isFeatureEnabled).toHaveBeenCalledWith('knowledge-projection-fill')
    expect(mocks.markUnfilled).not.toHaveBeenCalled()
    expect(mocks.end).toHaveBeenCalledTimes(2)
  })

  it('runs another round while workers still settle documents, and stops once none can', async () => {
    mocks.runProjection
      .mockResolvedValueOnce({ ...drained, remaining: true })
      .mockResolvedValueOnce(drained)
      .mockResolvedValueOnce({ ...drained, settled: 0, deferred: 1, remaining: true })
      .mockResolvedValueOnce({ ...drained, settled: 0, remaining: false })
    await expect(runKnowledgeProjectionPass({ budgetMs: 60_000 })).resolves.toMatchObject({
      settled: 2,
      deferred: 1,
      remaining: true,
    })
    expect(mocks.runProjection).toHaveBeenCalledTimes(4)
  })

  it('marks unfilled documents a few at a time and converges them until none are left', async () => {
    mocks.isFeatureEnabled.mockResolvedValue(true)
    mocks.markUnfilled
      .mockResolvedValueOnce({ marked: 2, cursor: { projection: 0, afterId: 'row-2' } })
      .mockResolvedValueOnce({ marked: 0, cursor: null })
    const result = await runKnowledgeProjectionPass({ budgetMs: 60_000 })
    expect(result).toMatchObject({ filled: 2, remaining: false })
    expect(mocks.markUnfilled).toHaveBeenNthCalledWith(1, expect.anything(), undefined)
    expect(mocks.markUnfilled).toHaveBeenNthCalledWith(2, expect.anything(), {
      projection: 0,
      afterId: 'row-2',
    })
  })

  it('does not fill while marks remain, so writers are converged first', async () => {
    mocks.isFeatureEnabled.mockResolvedValue(true)
    mocks.runProjection.mockResolvedValue({ ...drained, settled: 0, remaining: true })
    await expect(runKnowledgeProjectionPass({ budgetMs: 60_000 })).resolves.toMatchObject({
      remaining: true,
      filled: 0,
    })
    expect(mocks.markUnfilled).not.toHaveBeenCalled()
  })

  it('closes its connections when a pass fails', async () => {
    mocks.runProjection.mockRejectedValue(new Error('connection lost'))
    await expect(runKnowledgeProjectionPass({ budgetMs: 60_000 })).rejects.toThrow(
      'connection lost'
    )
    expect(mocks.end).toHaveBeenCalledTimes(2)
  })
})
